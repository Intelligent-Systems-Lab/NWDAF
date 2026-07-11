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

type Processor struct {
	nwdaf      NwdafApp
	wg         *sync.WaitGroup
	anlf       anlfCoordinator
	mtlf       *mtlf.MtlfService
	adrfBuffer *adrfBuffer
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

	// ADRF buffer: forward UPF notifications to ADRF for retrain dataset.
	if c := p.nwdaf.Consumer(); c != nil {
		if adrf := c.AdrfClient(); adrf != nil {
			threshold := 1
			if cfg := p.nwdaf.Config(); cfg != nil && cfg.Configuration != nil {
				threshold = cfg.Configuration.Adrf.StorageThresholdOrDefault()
			}
			p.adrfBuffer = newAdrfBuffer(threshold, p.nwdaf.CancelContext(), adrf)
			logger.ProcLog.Infof("ADRF buffer initialized: threshold=%d", threshold)
		}
	}

	logger.ProcLog.Info("Processor initialized")
	return p
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
