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
}

type anlfMLModelBackend interface {
	DeliverMLModelProvisionNotification(context.Context, string, []byte) (*backend.StandardResponse, error)
	CreateMLModelMonitorSubscription(context.Context, []byte) (*backend.StandardResponse, error)
	ReplaceMLModelMonitorSubscription(context.Context, string, []byte) (*backend.StandardResponse, error)
	DeleteMLModelMonitorSubscription(context.Context, string) (*backend.StandardResponse, error)
}

type backendAvailability interface {
	Usable() bool
	MarkUnavailable(string)
	Refresh()
}

type Processor struct {
	nwdaf              NwdafApp
	eventsBackend      eventsSubscriptionBackend
	eventsAvailability backendAvailability
	eventsMu           sync.Mutex
	mlModelMu          sync.Mutex
	mtlfMLModelBackend mtlfMLModelBackend
	anlfMLModelBackend anlfMLModelBackend
	mtlfAvailability   backendAvailability
	anlfAvailability   backendAvailability
	mlModelHTTPClient  *http.Client
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
	if p.mtlfMLModelBackend == nil || p.mtlfAvailability == nil || !p.mtlfAvailability.Usable() {
		return nil, errors.New("MTLF backend is unavailable")
	}
	return p.mtlfMLModelBackend.DeliverAdrfRetrievalNotification(ctx, body)
}
