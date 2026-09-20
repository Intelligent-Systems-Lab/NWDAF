package processor

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/free5gc/nwdaf/internal/backend"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/app"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

type NwdafApp interface {
	app.App
	CancelContext() context.Context
}

type eventsSubscriptionBackend interface {
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

type mtlfMLModelBackend interface {
	CreateMLModelProvisionSubscription(context.Context, []byte) (*backend.StandardResponse, error)
	ReplaceMLModelProvisionSubscription(context.Context, string, []byte) (*backend.StandardResponse, error)
	DeleteMLModelProvisionSubscription(context.Context, string) (*backend.StandardResponse, error)
	CreateMLModelMonitorRegistration(context.Context, []byte) (*backend.StandardResponse, error)
	DeleteMLModelMonitorRegistration(context.Context, string) (*backend.StandardResponse, error)
	DeliverMLModelMonitorNotification(context.Context, []byte) (*backend.StandardResponse, error)
	DeliverAdrfRetrievalNotification(context.Context, []byte) (*backend.StandardResponse, error)
	CreateMLModelTrainingSubscription(context.Context, []byte, string) (*backend.StandardResponse, error)
	ReplaceMLModelTrainingSubscription(context.Context, string, []byte) (*backend.StandardResponse, error)
	PatchMLModelTrainingSubscription(context.Context, string, []byte) (*backend.StandardResponse, error)
	DeleteMLModelTrainingSubscription(context.Context, string) (*backend.StandardResponse, error)
	DeliverMLModelTrainingNotification(context.Context, []byte) (*backend.StandardResponse, error)
}

type anlfMLModelBackend interface {
	DeliverMLModelProvisionNotification(context.Context, string, []byte) (*backend.StandardResponse, error)
	CreateMLModelMonitorSubscription(context.Context, []byte) (*backend.StandardResponse, error)
	ReplaceMLModelMonitorSubscription(context.Context, string, []byte) (*backend.StandardResponse, error)
	DeleteMLModelMonitorSubscription(context.Context, string) (*backend.StandardResponse, error)
}

type backendAvailability interface {
	Usable() bool
	Acquire() (*backend.GenerationLease, bool)
	MarkUnavailable(string)
	Refresh()
	Snapshot() backend.Snapshot
}

func acquireBackend(
	client any,
	availability backendAvailability,
) (*backend.GenerationLease, bool) {
	if client == nil {
		return nil, false
	}
	if availability == nil {
		return nil, true
	}
	return availability.Acquire()
}

type mlModelPeerConsumer interface {
	CreatePeerMLModelProvision(
		context.Context,
		backend.SelectedTarget,
		[]byte,
	) (*backend.StandardResponse, error)
	ReplacePeerMLModelProvision(context.Context, string, []byte) (*backend.StandardResponse, error)
	DeletePeerMLModelProvision(context.Context, string) (*backend.StandardResponse, error)
	CreatePeerMLModelMonitorRegistration(
		context.Context,
		backend.SelectedTarget,
		[]byte,
	) (*backend.StandardResponse, error)
	DeletePeerMLModelMonitorRegistration(context.Context, string) (*backend.StandardResponse, error)
	CreatePeerMLModelMonitorSubscription(
		context.Context,
		backend.SelectedTarget,
		[]byte,
	) (*backend.StandardResponse, error)
	ReplacePeerMLModelMonitorSubscription(context.Context, string, []byte) (*backend.StandardResponse, error)
	DeletePeerMLModelMonitorSubscription(context.Context, string) (*backend.StandardResponse, error)
	CreatePeerMLModelTraining(
		context.Context,
		backend.SelectedTarget,
		[]byte,
	) (*backend.StandardResponse, error)
	ReplacePeerMLModelTraining(context.Context, string, []byte) (*backend.StandardResponse, error)
	PatchPeerMLModelTraining(context.Context, string, []byte) (*backend.StandardResponse, error)
	DeletePeerMLModelTraining(context.Context, string) (*backend.StandardResponse, error)
}

type Processor struct {
	nwdaf              NwdafApp
	eventsBackend      eventsSubscriptionBackend
	eventsAvailability backendAvailability
	eventsMu           sync.Mutex
	// mlModelMu protects only in-memory ML model route transitions. Peer,
	// backend, and callback I/O must run after releasing it.
	mlModelMu                sync.Mutex
	mlModelOperationRevision uint64
	mtlfMLModelBackend       mtlfMLModelBackend
	anlfMLModelBackend       anlfMLModelBackend
	mtlfAvailability         backendAvailability
	anlfAvailability         backendAvailability
	mlModelHTTPClient        *http.Client
	mlModelPeerConsumer      mlModelPeerConsumer
}

func (p *Processor) config() *factory.Config {
	if p == nil || p.nwdaf == nil {
		return nil
	}
	return p.nwdaf.Config()
}

func NewProcessor(nwdaf NwdafApp) *Processor {
	p := &Processor{
		nwdaf:             nwdaf,
		mlModelHTTPClient: http.DefaultClient,
	}

	logger.ProcLog.Info("Processor initialized")
	return p
}

func (p *Processor) SetMLModelHTTPClient(client *http.Client) {
	if client != nil {
		p.mlModelHTTPClient = client
	}
}

func (p *Processor) SetMLModelPeerConsumer(consumer mlModelPeerConsumer) {
	p.mlModelPeerConsumer = consumer
}

func (p *Processor) SetEventsSubscriptionBackend(
	backend eventsSubscriptionBackend,
	availability backendAvailability,
) {
	p.eventsBackend = backend
	p.eventsAvailability = availability
}

func (p *Processor) SetMLModelBackends(
	mtlfBackend mtlfMLModelBackend,
	anlfBackend anlfMLModelBackend,
	mtlfAvailability backendAvailability,
	anlfAvailability backendAvailability,
) {
	p.mtlfMLModelBackend = mtlfBackend
	p.anlfMLModelBackend = anlfBackend
	p.mtlfAvailability = mtlfAvailability
	p.anlfAvailability = anlfAvailability
}

// HandleAdrfRetrievalNotify forwards a standard ADRF callback to the MTLF backend.
func (p *Processor) HandleAdrfRetrievalNotify(
	ctx context.Context,
	body []byte,
) (*backend.StandardResponse, error) {
	lease, admitted := acquireBackend(p.mtlfMLModelBackend, p.mtlfAvailability)
	if !admitted {
		return nil, errors.New("MTLF backend is unavailable")
	}
	if lease != nil {
		defer lease.Release()
	}
	return p.mtlfMLModelBackend.DeliverAdrfRetrievalNotification(ctx, body)
}
