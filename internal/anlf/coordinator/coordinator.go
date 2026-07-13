// Package coordinator owns Go-side AnLF cross-domain coordination.
package coordinator

import (
	"context"
	"sync"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/app"
	"github.com/free5gc/nwdaf/pkg/factory"
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

type Coordinator struct {
	nwdaf               NwdafApp
	backend             BackendRuntimeClient
	wg                  *sync.WaitGroup
	observationDelivery *ObservationDelivery
}

func New(
	nwdaf NwdafApp,
	backend BackendRuntimeClient,
	delivery *ObservationDelivery,
) *Coordinator {
	return &Coordinator{
		nwdaf:               nwdaf,
		backend:             backend,
		observationDelivery: delivery,
	}
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
