package mtlf

import (
	"sync"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

var mtlfLog = logger.MtlfLog

// StartTrainingScheduler starts background MTLF training scheduler.
func (m *MtlfService) StartTrainingScheduler(wg *sync.WaitGroup) {
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
		m.runDelayedTraining(delay, mtlfCfg)
	}()
}

// runDelayedTraining waits for a delay then triggers training via Daisy.
// The POST to Daisy blocks until training completes (HTTP 200 = success).
func (m *MtlfService) runDelayedTraining(delaySec int, mtlfCfg *factory.MtlfConfig) {
	mtlfLog.Infof("MTLF training scheduled in %d seconds", delaySec)

	select {
	case <-time.After(time.Duration(delaySec) * time.Second):
		// Timer expired, proceed to trigger training
	case <-m.nwdaf.CancelContext().Done():
		mtlfLog.Info("MTLF training canceled (shutdown)")
		return
	}

	mtlfLog.Infof("Triggering MTLF training via Daisy: endpoint=%s", mtlfCfg.Endpoint)

	if err := m.triggerTraining(mtlfCfg); err != nil {
		mtlfLog.Errorf("MTLF training failed: %v", err)
		return
	}

	mtlfLog.Info("MTLF training completed successfully")

	// Startup trigger uses the static model as the initial "old" model
	oldModelUrl := mtlfCfg.StaticModelUrl
	m.swapModelAfterRetrain(oldModelUrl, mtlfCfg)
}

// TriggerRetraining initiates retraining for a degraded model (called by accuracy monitor).
// Per TS 23.288 §5C: AnLF reports accuracy degradation → MTLF decides to retrain.
// store.SetRetraining(false) is called on failure so the monitor can re-trigger later.
func (m *MtlfService) TriggerRetraining(
	oldModelUrl string,
	store *nwdaf_context.ModelAccuracyStore,
) {
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil || cfg.Configuration.Mtlf == nil {
		store.SetRetraining(false)
		return
	}
	mtlfCfg := cfg.Configuration.Mtlf

	mtlfLog.Infof("Triggering retraining due to accuracy degradation for model: %s", oldModelUrl)

	go func() {
		if err := m.triggerTraining(mtlfCfg); err != nil {
			mtlfLog.Errorf("Accuracy-triggered retraining failed: %v", err)
			store.SetRetraining(false)
			return
		}
		mtlfLog.Info("Accuracy-triggered retraining completed successfully")
		// swapModelAfterRetrain deletes the old store, so no need to clear the flag.
		m.swapModelAfterRetrain(oldModelUrl, mtlfCfg)
	}()
}

// triggerTraining sends training task to Daisy (shared by delay + accuracy triggers).
func (m *MtlfService) triggerTraining(mtlfCfg *factory.MtlfConfig) error {
	client := consumer.NewDaisyClient(mtlfCfg.Endpoint)
	task := mtlfCfg.Task
	if task == nil {
		task = map[string]any{}
	}
	return client.TriggerTraining(task)
}

// swapModelAfterRetrain handles the hot-swap of models after a successful retraining.
func (m *MtlfService) swapModelAfterRetrain(oldModelUrl string, mtlfCfg *factory.MtlfConfig) {
	nwdafCtx := nwdaf_context.GetSelf()

	// 1. Get new model URL (from Daisy task config MODEL_PATH)
	newModelUrl := "model.npy"
	if taskOption, exists := mtlfCfg.Task["MODEL_PATH"]; exists {
		if pathStr, isStr := taskOption.(string); isStr {
			newModelUrl = pathStr
		}
	}

	mtlfLog.Infof("Starting model hot-swap: old=%s, new=%s", oldModelUrl, newModelUrl)

	// Get ML Service client
	mlCfg := factory.NwdafConfig.Configuration.MlService
	if mlCfg == nil || !mlCfg.Enabled || mlCfg.Endpoint == "" {
		mtlfLog.Error("ML service not configured; cannot perform model swap")
		return
	}
	mlClient := consumer.NewMlServiceClient(mlCfg.Endpoint)

	// 2. Load new model
	newModelId, err := mlClient.InitializeModel(newModelUrl)
	if err != nil {
		mtlfLog.Errorf("Hot-swap failed: failed to load new model %s: %v", newModelUrl, err)
		return
	}

	// 3. Unload old model
	oldShared := nwdafCtx.GetSharedModel(oldModelUrl)
	if oldShared != nil {
		oldModelId := oldShared.GetModelId()
		if oldModelId != "" {
			err = mlClient.UnloadModel(oldModelId)
			if err != nil {
				mtlfLog.Warnf("Failed to unload old model ID %s (url=%s): %v", oldModelId, oldModelUrl, err)
			}
		}
	}

	// 4. Update SharedModelRegistry
	nwdafCtx.DeleteSharedModel(oldModelUrl)
	newShared, _ := nwdafCtx.GetOrCreateSharedModel(newModelUrl, models.NwdafEvent_UE_COMMUNICATION)
	newShared.SetModelId(newModelId)

	// 5. Update all subscriptions currently using the old model
	subs := nwdafCtx.GetAllSubscriptions()
	for _, sub := range subs {
		mlInfo := nwdafCtx.GetMlModelInfo(sub.ID)
		if mlInfo != nil {
			mlInfo.RLock()
			currentUrl := mlInfo.ModelUrl
			mlInfo.RUnlock()

			switch currentUrl {
			case oldModelUrl:
				// Update to new model
				mlInfo.SetModelUrl(newModelUrl)
				mlInfo.SetModelReady(newModelId)
				newShared.AddSubscriber(sub.ID)
				mtlfLog.Infof("Updated subscription %s to new model ID %s", sub.ID, newModelId)
			case newModelUrl:
				// Edge case: same URL but we just reloaded it
				mlInfo.SetModelReady(newModelId)
				newShared.AddSubscriber(sub.ID)
			}
		}
	}

	// 6. Restart accuracy monitor for the new model (wired via callback by processor)
	nwdafCtx.DeleteModelAccuracyStore(oldModelUrl)

	if m.onModelSwapped != nil {
		m.onModelSwapped(newModelUrl, m.wg)
	}

	mtlfLog.Infof("Model hot-swap completed successfully: new modelId=%s", newModelId)
}
