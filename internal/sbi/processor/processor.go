package processor

import (
	"context"
	"sync"

	"github.com/free5gc/nwdaf/internal/anlf"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/mtlf"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
)

type NwdafApp interface {
	CancelContext() context.Context
	Consumer() *consumer.Consumer
}

type Processor struct {
	nwdaf NwdafApp
	wg    *sync.WaitGroup
	anlf  *anlf.AnlfService
	mtlf  *mtlf.MtlfService
}

func NewProcessor(nwdaf NwdafApp) *Processor {
	p := &Processor{
		nwdaf: nwdaf,
		anlf:  anlf.NewAnlfService(nwdaf),
		mtlf:  mtlf.NewMtlfService(nwdaf),
	}

	// Wire 1: AnLF reports deviation → MTLF decides whether to retrain.
	// Per TS 23.288 §6.2D→§6.2E: AnLF produces Analytics Accuracy Information;
	// MTLF receives it and determines retraining necessity.
	p.anlf.SetOnDeviationReport(func(
		modelUrl string,
		deviation float64,
		store *nwdaf_context.ModelAccuracyStore,
	) {
		p.mtlf.HandleDeviationReport(modelUrl, deviation, store)
	})

	// Wire 2: MTLF hot-swap completes → AnLF restarts accuracy monitor for new model.
	p.mtlf.SetOnModelSwapped(func(modelUrl string, wg *sync.WaitGroup) {
		p.anlf.StartAccuracyMonitorForModel(modelUrl, wg)
	})

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

// InitializeMlModel delegates to AnlfService and then starts accuracy monitoring.
func (p *Processor) InitializeMlModel(
	nwdafSubId string, mlInfo *nwdaf_context.MlModelInfo, modelUrl string,
) {
	p.anlf.InitializeMlModel(nwdafSubId, mlInfo, modelUrl)
	if p.wg != nil {
		p.anlf.StartAccuracyMonitorForModel(modelUrl, p.wg)
	}
}
