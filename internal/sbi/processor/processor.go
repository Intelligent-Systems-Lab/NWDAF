package processor

import (
	"context"
	"net/http"
	"sync"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/backend"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/mtlf"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/app"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

type NwdafApp interface {
	app.App
	CancelContext() context.Context
	Consumer() consumer.ConsumerAPI
}

type anlfCoordinator interface {
	ApplyInitialSubscriptionRuntime(
		subscription *nwdaf_context.Subscription,
	) (*contract.ApplySubscriptionRuntimeResponse, error)
	ReleaseSubscriptionRuntime(subscriptionID string) error
	SyncCurrentObservationBindings(subscriptionID string) error
	BuildProvisionNotificationURI() string
	SyncModelProvisionBinding(subscriptionID string, binding contract.ModelProvisionBinding) error
	EnqueueObservations(sourceID string, observations []contract.SourceObservation) bool
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
	RefreshBackendSync()
}

type mtlfMLModelBackend interface {
	CreateMLModelProvisionSubscription(context.Context, []byte) (*backend.StandardResponse, error)
	ReplaceMLModelProvisionSubscription(context.Context, string, []byte) (*backend.StandardResponse, error)
	DeleteMLModelProvisionSubscription(context.Context, string) (*backend.StandardResponse, error)
	CreateMLModelMonitorRegistration(context.Context, []byte) (*backend.StandardResponse, error)
	DeleteMLModelMonitorRegistration(context.Context, string) (*backend.StandardResponse, error)
	DeliverMLModelMonitorNotification(context.Context, []byte) (*backend.StandardResponse, error)
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
	wg                 *sync.WaitGroup
	anlf               anlfCoordinator
	eventsBackend      eventsSubscriptionBackend
	eventsMu           sync.Mutex
	mlModelMu          sync.Mutex
	mtlfMLModelBackend mtlfMLModelBackend
	anlfMLModelBackend anlfMLModelBackend
	mtlfAvailability   backendAvailability
	anlfAvailability   backendAvailability
	mlModelHTTPClient  *http.Client
	mtlf               *mtlf.MtlfService
}

func (p *Processor) config() *factory.Config {
	if p == nil || p.nwdaf == nil {
		return nil
	}
	return p.nwdaf.Config()
}

func NewProcessor(nwdaf NwdafApp, coordinator anlfCoordinator, mtlfService *mtlf.MtlfService) *Processor {
	p := &Processor{
		nwdaf:             nwdaf,
		anlf:              coordinator,
		mtlf:              mtlfService,
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

func (p *Processor) SetEventsSubscriptionBackend(backend eventsSubscriptionBackend) {
	p.eventsBackend = backend
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

// SetWaitGroup stores the application WaitGroup for goroutine lifecycle management.
func (p *Processor) SetWaitGroup(wg *sync.WaitGroup) {
	p.wg = wg
	p.mtlf.SetWaitGroup(wg)
}

// StartMtlfTrainingScheduler delegates to MtlfService.
func (p *Processor) StartMtlfTrainingScheduler(wg *sync.WaitGroup) {
	p.mtlf.StartTrainingScheduler(wg)
}

// HandleAdrfRetrievalNotify delegates an ADRF retrieval callback to MtlfService.
func (p *Processor) HandleAdrfRetrievalNotify(notifCorrId string, fetchCorrIds []string, terminationReq bool) {
	p.mtlf.HandleAdrfRetrievalNotify(notifCorrId, fetchCorrIds, terminationReq)
}
