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

func NewProcessor(nwdaf NwdafApp) *Processor {
	p := &Processor{
		nwdaf: nwdaf,
		anlf:  anlf.NewAnlfService(nwdaf),
		mtlf:  mtlf.NewMtlfService(nwdaf),
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

	// Wire 2: MTLF requests ML Service operations during hot-swap → AnLF executes them.
	// AnLF loads the new model, unloads the old one, and returns the new model ID.
	p.mtlf.SetOnModelSwapReady(func(newModelUrl, oldModelId string) (string, error) {
		return p.anlf.SwapModel(newModelUrl, oldModelId)
	})

	// Wire 3: MTLF hot-swap completes → AnLF restarts accuracy monitor for new model.
	p.mtlf.SetOnModelSwapped(func(modelUrl string, wg *sync.WaitGroup) {
		p.anlf.StartAccuracyMonitorForModel(modelUrl, wg)
	})

	// ADRF buffer: forward UPF notifications to ADRF for retrain dataset.
	if c := p.nwdaf.Consumer(); c != nil {
		if adrfClient := c.AdrfClient(); adrfClient != nil {
			threshold := 1
			if cfg := p.nwdaf.Config(); cfg != nil && cfg.Configuration != nil {
				threshold = cfg.Configuration.Adrf.StorageThresholdOrDefault()
			}
			p.adrfBuffer = newAdrfBuffer(threshold, adrfClient)
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

// HandleDaisyCallback delegates an async Daisy training callback to MtlfService.
func (p *Processor) HandleDaisyCallback(taskId, modelUrl, status, errMsg string) {
	p.mtlf.HandleTrainingComplete(taskId, modelUrl, status, errMsg)
}

// HandleAdrfRetrievalNotify delegates an ADRF retrieval callback to MtlfService.
func (p *Processor) HandleAdrfRetrievalNotify(notifCorrId string, fetchCorrIds []string, terminationReq bool) {
	p.mtlf.HandleAdrfRetrievalNotify(notifCorrId, fetchCorrIds, terminationReq)
}

// InitializeMlModel delegates to AnlfService and then starts accuracy monitoring.
func (p *Processor) InitializeMlModel(
	nwdafSubId string, mlInfo *nwdaf_context.MlModelInfo, modelUrl string,
) {
	p.anlf.InitializeMlModel(nwdafSubId, mlInfo, modelUrl)
	if p.wg != nil {
		p.anlf.StartAccuracyMonitorForModel(modelUrl, p.wg)
	}
}
