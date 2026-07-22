// Package coordinator owns Go-side AnLF cross-domain coordination.
package coordinator

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/backend"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/app"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

var anlfLog = logger.AnlfLog

type NwdafApp interface {
	app.App
	CancelContext() context.Context
}

type BackendRuntimeClient interface {
	ApplySubscriptionRuntime(
		ctx context.Context,
		request contract.ApplySubscriptionRuntimeRequest,
	) (*contract.ApplySubscriptionRuntimeResponse, error)
	ReleaseSubscriptionRuntime(ctx context.Context, subscriptionID string) error
	SyncObservationBindings(
		ctx context.Context,
		subscriptionID string,
		request contract.SyncObservationBindingsRequest,
	) error
}

type ModelProvisionClient interface {
	SyncModelProvisionBinding(context.Context, string, contract.ModelProvisionBinding) error
	ApplyModelProvisionEvent(context.Context, contract.ModelProvisionEvent) (*contract.ModelProvisionEventResponse, error)
}

type ObservationSender interface {
	SendObservations(ctx context.Context, sourceID string, batch contract.ObservationBatch) error
}

type EventsSubscriptionBackend interface {
	CreateEventsSubscription(
		context.Context,
		*models.NnwdafEventsSubscription,
	) (*models.NnwdafEventsSubscription, string, error)
	ReplaceEventsSubscription(
		context.Context,
		string,
		*models.NnwdafEventsSubscription,
	) (*models.NnwdafEventsSubscription, error)
	DeleteEventsSubscription(context.Context, string) error
}

type AvailabilityGate interface {
	Usable() bool
	MarkUnavailable(category string)
}

var (
	ErrBackendUnavailable  = errors.New("AnLF backend is unavailable")
	ErrStaleBackendProcess = errors.New("AnLF backend process instance is stale")
	ErrUnknownSmfResource  = errors.New("SMF resource association references an unknown peer resource")
)

type Coordinator struct {
	nwdaf               NwdafApp
	backend             BackendRuntimeClient
	eventsBackend       EventsSubscriptionBackend
	wg                  *sync.WaitGroup
	observationDelivery *ObservationDelivery
	availability        AvailabilityGate
}

func New(
	nwdaf NwdafApp,
	backend BackendRuntimeClient,
	delivery *ObservationDelivery,
	gates ...AvailabilityGate,
) *Coordinator {
	coordinator := &Coordinator{
		nwdaf:               nwdaf,
		backend:             backend,
		observationDelivery: delivery,
	}
	if eventsBackend, ok := backend.(EventsSubscriptionBackend); ok {
		coordinator.eventsBackend = eventsBackend
	}
	if len(gates) > 0 {
		coordinator.availability = gates[0]
	}
	return coordinator
}

func (a *Coordinator) backendUsable() bool {
	return a != nil && a.backend != nil && (a.availability == nil || a.availability.Usable())
}

func (a *Coordinator) ReplaceSmfResourceAssociations(
	update backend.SmfResourceAssociationUpdate,
) error {
	if a == nil || a.nwdaf == nil || a.availability == nil {
		return ErrBackendUnavailable
	}
	snapshotter, ok := a.availability.(interface{ Snapshot() backend.Snapshot })
	if !ok {
		return ErrBackendUnavailable
	}
	snapshot := snapshotter.Snapshot()
	if snapshot.State != backend.StateUsable {
		return ErrBackendUnavailable
	}
	if update.ProcessInstanceID == "" || update.ProcessInstanceID != snapshot.ProcessInstanceID {
		return ErrStaleBackendProcess
	}

	nwdafContext := a.nwdaf.Context()
	if nwdafContext == nil {
		return ErrBackendUnavailable
	}
	associations := make([]nwdaf_context.SmfPeerResourceAssociation, 0, len(update.SmfResources))
	for _, association := range update.SmfResources {
		associations = append(associations, nwdaf_context.SmfPeerResourceAssociation{
			TargetAPIBaseURI:     association.TargetAPIBaseURI,
			PeerSubscriptionID:   association.PeerSubscriptionID,
			NwdafSubscriptionIDs: append([]string(nil), association.NwdafSubscriptionIDs...),
		})
	}
	if !nwdafContext.ReplaceSmfPeerResourceAssociations(associations) {
		return ErrUnknownSmfResource
	}
	return nil
}

func (a *Coordinator) reportBackendFailure(err error) {
	if a == nil {
		return
	}
	reportAvailabilityFailure(a.availability, err)
}

func reportAvailabilityFailure(availability AvailabilityGate, err error) {
	if err == nil || availability == nil || errors.Is(err, context.Canceled) {
		return
	}
	var statusError interface{ HTTPStatusCode() int }
	if errors.As(err, &statusError) {
		statusCode := statusError.HTTPStatusCode()
		if statusCode >= http.StatusBadRequest && statusCode < http.StatusInternalServerError {
			return
		}
	}
	availability.MarkUnavailable("operation_failure")
}

func (a *Coordinator) config() *factory.Config {
	if a == nil || a.nwdaf == nil {
		return nil
	}
	return a.nwdaf.Config()
}

func (a *Coordinator) SetWaitGroup(wg *sync.WaitGroup) {
	a.wg = wg
}

func (a *Coordinator) launchOwnedTask(fn func()) {
	if fn == nil {
		return
	}
	if a.wg != nil {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			fn()
		}()
		return
	}
	go fn()
}

func (a *Coordinator) StartObservationDelivery() {
	if a.observationDelivery != nil {
		a.observationDelivery.Start()
	}
}

func (a *Coordinator) StopObservationDelivery() {
	if a.observationDelivery != nil {
		a.observationDelivery.Stop()
	}
}

func (a *Coordinator) EnqueueObservations(
	sourceID string,
	observations []contract.SourceObservation,
) bool {
	return a.observationDelivery != nil && a.observationDelivery.Enqueue(sourceID, observations)
}

func (a *Coordinator) BuildProvisionNotificationURI() string {
	cfg := a.config()
	if cfg == nil {
		return ""
	}
	return cfg.GetAnlfServerURI() + "/mlmodel-notify"
}

func (a *Coordinator) BuildAnalyticsReportCallbackURI(subscriptionID string) string {
	cfg := a.config()
	if cfg == nil {
		return ""
	}
	return cfg.GetAnlfServerURI() + "/subscriptions/" + subscriptionID + "/analytics-reports"
}

func (a *Coordinator) BuildRuntimeCompletionCallbackURI(subscriptionID string) string {
	cfg := a.config()
	if cfg == nil {
		return ""
	}
	return cfg.GetAnlfServerURI() + "/subscriptions/" + subscriptionID + "/runtime-completions"
}
