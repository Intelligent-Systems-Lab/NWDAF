package processor

import (
	"context"
	"sync"

	"github.com/free5gc/nwdaf/internal/anlf"
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

type Processor struct {
	nwdaf      NwdafApp
	wg         *sync.WaitGroup
	anlf       *anlf.AnlfService
	mtlf       *mtlf.MtlfService
	adrfBuffer *adrfBuffer
}

func (p *Processor) config() *factory.Config {
	if p == nil || p.nwdaf == nil {
		return nil
	}
	return p.nwdaf.Config()
}

func NewProcessor(nwdaf NwdafApp, anlfService *anlf.AnlfService, mtlfService *mtlf.MtlfService) *Processor {
	p := &Processor{
		nwdaf: nwdaf,
		anlf:  anlfService,
		mtlf:  mtlfService,
	}

	// Wire 1: AnLF reports deviation → MTLF decides whether to retrain.
	// Per TS 23.288 §6.2D→§6.2E: AnLF produces Analytics Accuracy Information;
	// MTLF receives per-scope reports and determines retraining necessity.
	p.anlf.SetOnAccuracyReports(func(
		modelUrl string,
		reports []anlf.AccuracyReport,
		store *nwdaf_context.ModelAccuracyStore,
	) {
		p.mtlf.HandleAccuracyReports(modelUrl, reports, store)
	})

	// Retrain completion is translated into the same provision-style apply path
	// used by external MTLF notifications.
	p.mtlf.SetOnModelProvisionUpdated(func(oldModelReference, newModelReference string) error {
		return p.anlf.ApplyRetrainedModel(oldModelReference, newModelReference)
	})

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
	p.anlf.SetWaitGroup(wg)
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
