package mtlf

import (
	"maps"
	"sync"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

var mtlfLog = logger.MtlfLog

// inFlightEntry holds the context needed to complete a hot-swap once Daisy
// calls back with the training result.
type inFlightEntry struct {
	oldModelUrl string
	// store is non-nil only for accuracy-triggered retraining; used to clear
	// the IsRetraining flag if training fails so the monitor can re-trigger.
	store *nwdaf_context.ModelAccuracyStore
}

// buildNwdafURL constructs a full NWDAF URL for the given path.
func buildNwdafURL(urlPath string) string {
	cfg := factory.NwdafConfig
	if cfg == nil {
		return ""
	}
	return cfg.GetSbiUri() + urlPath
}

// buildCallbackURL constructs the NWDAF callback URL that Daisy will POST to
// when async training completes.
func buildCallbackURL() string {
	return buildNwdafURL("/mtlf/training-complete")
}

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

// runDelayedTraining waits for a delay then triggers async training via Daisy.
// Daisy responds 202 immediately; swap happens when Daisy calls back.
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
	m.submitDaisyTask(mtlfCfg, "", mtlfCfg.StaticModelUrl, nil)
}

// startRetrainWorkflow decides the retrain path and dispatches accordingly.
// Per TS 23.288 §5C: AnLF reports accuracy degradation → MTLF decides to retrain.
// store.SetRetraining(false) is called on failure so the monitor can re-trigger later.
func (m *MtlfService) startRetrainWorkflow(
	oldModelUrl string,
	store *nwdaf_context.ModelAccuracyStore,
) {
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil || cfg.Configuration.Mtlf == nil {
		store.SetRetraining(false)
		return
	}
	mtlfCfg := cfg.Configuration.Mtlf

	// ADRF path: fetch historical data before submitting to Daisy
	if cfg.Configuration.Adrf.AdrfEnabled() {
		mtlfLog.Infof("ADRF enabled: starting data retrieval before retrain for model=%s", oldModelUrl)
		go m.runAdrfRetrainWorkflow(mtlfCfg, cfg.Configuration.Adrf, oldModelUrl, store)
		return
	}

	go m.submitDaisyTask(mtlfCfg, "", oldModelUrl, store)
}

// submitDaisyTask sends an async training request to Daisy and stores the
// in-flight entry. When Daisy calls back, HandleTrainingComplete completes the swap.
// tid: if non-empty, uses this TID (ADRF path, must match UploadData TID);
//
//	if empty, generates a fresh UUID.
func (m *MtlfService) submitDaisyTask(
	mtlfCfg *factory.MtlfConfig,
	tid string,
	oldModelUrl string,
	store *nwdaf_context.ModelAccuracyStore,
) {
	mtlfLog.Infof("Submitting training task to Daisy: model=%s tid=%s", oldModelUrl, tid)
	cbURL := buildCallbackURL()
	if cbURL == "" {
		mtlfLog.Warn("callback URL is empty; Daisy cannot notify completion")
	}

	task := mtlfCfg.Task
	if task == nil {
		task = map[string]any{}
	}
	// Clone to avoid mutating the shared config map.
	taskCopy := make(map[string]any, len(task)+2)
	maps.Copy(taskCopy, task)

	client := consumer.NewDaisyClient(mtlfCfg.Endpoint)
	taskId, err := client.TriggerTrainingAsync(taskCopy, cbURL, tid)
	if err != nil {
		mtlfLog.Errorf("Failed to send async training request to Daisy: %v", err)
		if store != nil {
			store.SetRetraining(false)
		}
		return
	}

	m.inFlight.Store(taskId, &inFlightEntry{
		oldModelUrl: oldModelUrl,
		store:       store,
	})
	mtlfLog.Infof("Async training request accepted: taskId=%s callbackURL=%s", taskId, cbURL)
}

// HandleTrainingComplete is called when Daisy posts the async training callback.
// It looks up the in-flight entry and either clears the retraining flag (on
// failure) or triggers the model hot-swap (on success).
func (m *MtlfService) HandleTrainingComplete(taskId, modelUrl, status, errMsg string) {
	val, ok := m.inFlight.LoadAndDelete(taskId)
	if !ok {
		mtlfLog.Warnf("HandleTrainingComplete: unknown taskId=%s (already handled or never registered)", taskId)
		return
	}
	entry := val.(*inFlightEntry)

	if status != "success" {
		mtlfLog.Errorf("Async training failed: taskId=%s error=%s", taskId, errMsg)
		if entry.store != nil {
			entry.store.SetRetraining(false)
		}
		return
	}

	mtlfLog.Infof("Async training complete: taskId=%s modelUrl=%s", taskId, modelUrl)
	m.swapModelAfterRetrain(entry.oldModelUrl, modelUrl)
}

// swapModelAfterRetrain handles the hot-swap of models after a successful retraining.
func (m *MtlfService) swapModelAfterRetrain(oldModelUrl, newModelUrl string) {
	if factory.NwdafConfig == nil || factory.NwdafConfig.Configuration == nil {
		mtlfLog.Error("config not initialized; cannot perform model swap")
		return
	}

	nwdafCtx := nwdaf_context.GetSelf()

	mtlfLog.Infof("Starting model hot-swap: old=%s, new=%s", oldModelUrl, newModelUrl)

	// 1. Look up old model ID before modifying the registry
	oldModelId := ""
	oldShared := nwdafCtx.GetSharedModel(oldModelUrl)
	if oldShared != nil {
		oldModelId = oldShared.GetModelId()
	}

	// 2. Delegate ML Service operations to AnLF via callback:
	//    load new model → unload old model → return new model ID
	if m.onModelSwapReady == nil {
		mtlfLog.Error("onModelSwapReady callback not set; cannot perform model swap")
		return
	}
	newModelId, err := m.onModelSwapReady(newModelUrl, oldModelId)
	if err != nil {
		mtlfLog.Errorf("Hot-swap failed: AnLF could not load new model %s: %v", newModelUrl, err)
		return
	}

	// 3. Update SharedModelRegistry
	nwdafCtx.DeleteSharedModel(oldModelUrl)
	newShared, _ := nwdafCtx.GetOrCreateSharedModel(newModelUrl, models.NwdafEvent_UE_COMMUNICATION)
	newShared.SetModelId(newModelId)

	// 4. Update all subscriptions currently using the old model
	subs := nwdafCtx.GetAllSubscriptions()
	for _, sub := range subs {
		mlInfo := nwdafCtx.GetMlModelInfo(sub.ID)
		if mlInfo == nil {
			continue
		}
		mlInfo.RLock()
		currentUrl := mlInfo.ModelUrl
		mlInfo.RUnlock()

		switch currentUrl {
		case oldModelUrl:
			mlInfo.SetModelUrl(newModelUrl)
			mlInfo.SetModelReady(newModelId)
			newShared.AddSubscriber(sub.ID)
			mtlfLog.Infof("Updated subscription %s to new model ID %s", sub.ID, newModelId)
		case newModelUrl:
			mlInfo.SetModelReady(newModelId)
			newShared.AddSubscriber(sub.ID)
		}
	}

	// 5. Restart accuracy monitor for the new model (wired via callback by processor)
	nwdafCtx.DeleteModelAccuracyStore(oldModelUrl)
	if m.stateStore != nil {
		m.stateStore.DeleteModel(oldModelUrl)
	}

	if m.onModelSwapped != nil {
		m.onModelSwapped(newModelUrl, m.wg)
	}

	mtlfLog.Infof("Model hot-swap completed successfully: new modelId=%s", newModelId)
}
