package processor

import (
	"sync"
	"time"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
)

var mtlfLog = logger.MtlfLog

// StartMtlfTrainingScheduler starts background MTLF training scheduler
// Current: delay-based trigger after startup
// Future: accuracy monitoring will replace/supplement delay-based trigger
func (p *Processor) StartMtlfTrainingScheduler(wg *sync.WaitGroup) {
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil ||
		cfg.Configuration.Mtlf == nil || !cfg.Configuration.Mtlf.Enabled ||
		!cfg.Configuration.Mtlf.TriggerOnStartup {
		return
	}

	mtlfCfg := cfg.Configuration.Mtlf
	delay := mtlfCfg.TriggerDelay
	if delay <= 0 {
		delay = 30
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		p.runDelayedTraining(delay, mtlfCfg)
	}()
}

// runDelayedTraining waits for a delay then triggers training via Daisy
// The POST to Daisy blocks until training completes (HTTP 200 = success)
func (p *Processor) runDelayedTraining(delaySec int, mtlfCfg *factory.MtlfConfig) {
	mtlfLog.Infof("MTLF training scheduled in %d seconds", delaySec)

	select {
	case <-time.After(time.Duration(delaySec) * time.Second):
		// Timer expired, proceed to trigger training
	case <-p.nwdaf.CancelContext().Done():
		mtlfLog.Info("MTLF training canceled (shutdown)")
		return
	}

	mtlfLog.Infof("Triggering MTLF training via Daisy: endpoint=%s", mtlfCfg.Endpoint)

	if err := p.triggerTraining(mtlfCfg); err != nil {
		mtlfLog.Errorf("MTLF training failed: %v", err)
		return
	}

	mtlfLog.Info("MTLF training completed successfully")
}

// triggerTraining sends training task to Daisy (shared by delay + accuracy triggers)
func (p *Processor) triggerTraining(mtlfCfg *factory.MtlfConfig) error {
	client := consumer.NewDaisyClient(mtlfCfg.Endpoint)
	task := mtlfCfg.Task
	if task == nil {
		task = map[string]any{}
	}
	return client.TriggerTraining(task)
}
