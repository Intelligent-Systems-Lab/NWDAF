package mtlf

import (
	"fmt"
	"maps"
	"sync"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
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

// StartTrainingScheduler starts background MTLF training scheduler.
func (m *MtlfService) StartTrainingScheduler(wg *sync.WaitGroup) {
	cfg := m.config()
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
	mtlfLog.Infof("StartTraining: scheduled delay=%ds", delaySec)

	select {
	case <-time.After(time.Duration(delaySec) * time.Second):
		// Timer expired, proceed to trigger training
	case <-m.nwdaf.CancelContext().Done():
		mtlfLog.Info("StartTraining: canceled reason=shutdown")
		return
	}

	mtlfLog.Info("StartTraining: trigger")
	m.submitDaisyTask(mtlfCfg, "", mtlfCfg.StaticModelUrl, nil)
}

// startRetrainWorkflow decides the retrain path and dispatches accordingly.
// Per TS 23.288 §5C: AnLF reports accuracy degradation → MTLF decides to retrain.
// store.SetRetraining(false) is called on failure so the monitor can re-trigger later.
func (m *MtlfService) startRetrainWorkflow(
	oldModelUrl string,
	store *nwdaf_context.ModelAccuracyStore,
) {
	cfg := m.config()
	if cfg == nil || cfg.Configuration == nil || cfg.Configuration.Mtlf == nil {
		store.SetRetraining(false)
		return
	}
	mtlfCfg := cfg.Configuration.Mtlf

	// ADRF path: fetch historical data before submitting to Daisy
	if cfg.Configuration.Adrf.AdrfEnabled() {
		mtlfLog.Info("StartRetrain: use adrf data retrieval")
		m.launchOwnedTask(func() {
			m.runAdrfRetrainWorkflow(mtlfCfg, cfg.Configuration.Adrf, oldModelUrl, store)
		})
		return
	}

	m.launchOwnedTask(func() {
		m.submitDaisyTask(mtlfCfg, "", oldModelUrl, store)
	})
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
	if m.shutdownStarted() {
		mtlfLog.Infof("SubmitTrainingTask: skipped reason=shutdown tid=%s", tid)
		if store != nil {
			store.SetRetraining(false)
		}
		return
	}

	mtlfLog.Infof("SubmitTrainingTask: dispatch tid=%s", tid)
	cbURL := m.buildCallbackURL()
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

	if m.daisyClient == nil {
		err := fmt.Errorf("daisy client not initialized")
		mtlfLog.Errorf("Failed to send async training request to Daisy: %v", err)
		if store != nil {
			store.SetRetraining(false)
		}
		return
	}

	taskId, err := m.daisyClient.TriggerTrainingAsync(m.nwdaf.CancelContext(), taskCopy, cbURL, tid)
	if err != nil {
		mtlfLog.Errorf("SubmitTrainingTask failed: %v", err)
		if store != nil {
			store.SetRetraining(false)
		}
		return
	}

	m.inFlight.Store(taskId, &inFlightEntry{
		oldModelUrl: oldModelUrl,
		store:       store,
	})
	mtlfLog.Infof("SubmitTrainingTask: accepted task=%s", taskId)
}

// HandleTrainingComplete is called when Daisy posts the async training callback.
// It looks up the in-flight entry and either clears the retraining flag (on
// failure) or triggers the model hot-swap (on success).
func (m *MtlfService) HandleTrainingComplete(taskId, modelUrl, status, errMsg string) {
	val, ok := m.inFlight.LoadAndDelete(taskId)
	if !ok {
		mtlfLog.Warnf("HandleTrainingComplete: unknown task=%s", taskId)
		return
	}
	entry := val.(*inFlightEntry)

	if status != "success" {
		mtlfLog.Errorf("HandleTrainingComplete failed: task=%s err=%s", taskId, errMsg)
		if entry.store != nil {
			entry.store.SetRetraining(false)
		}
		return
	}

	mtlfLog.Infof("HandleTrainingComplete: complete task=%s", taskId)
	m.swapModelAfterRetrain(entry.oldModelUrl, modelUrl)
}

// swapModelAfterRetrain handles the hot-swap of models after a successful retraining.
func (m *MtlfService) swapModelAfterRetrain(oldModelUrl, newModelUrl string) {
	cfg := m.config()
	if cfg == nil || cfg.Configuration == nil {
		mtlfLog.Error("config not initialized; cannot perform model swap")
		return
	}

	nwdafCtx := nwdaf_context.GetSelf()

	mtlfLog.Info("SwapModel: start")

	// 1. Look up old model ID before modifying the registry
	oldModelId := ""
	oldShared := nwdafCtx.GetSharedModel(oldModelUrl)
	if oldShared != nil {
		oldModelId = oldShared.GetModelId()
	}

	// 2. Delegate inference-engine operations to AnLF via callback:
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
			mtlfLog.Debugf("SwapModel: updated sub=%s modelId=%s", sub.ID, newModelId)
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

	mtlfLog.Infof("SwapModel: completed modelId=%s", newModelId)
}
