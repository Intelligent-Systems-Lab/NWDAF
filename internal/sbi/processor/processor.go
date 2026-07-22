package processor

import (
	"context"
	"sync"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
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

type Processor struct {
	nwdaf         NwdafApp
	wg            *sync.WaitGroup
	anlf          anlfCoordinator
	eventsBackend eventsSubscriptionBackend
	eventsMu      sync.Mutex
	mtlf          *mtlf.MtlfService
}

func (p *Processor) config() *factory.Config {
	if p == nil || p.nwdaf == nil {
		return nil
	}
	return p.nwdaf.Config()
}

func NewProcessor(nwdaf NwdafApp, coordinator anlfCoordinator, mtlfService *mtlf.MtlfService) *Processor {
	p := &Processor{
		nwdaf: nwdaf,
		anlf:  coordinator,
		mtlf:  mtlfService,
	}

	logger.ProcLog.Info("Processor initialized")
	return p
}

func (p *Processor) SetEventsSubscriptionBackend(backend eventsSubscriptionBackend) {
	p.eventsBackend = backend
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
