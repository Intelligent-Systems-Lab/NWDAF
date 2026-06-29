package anlf

import (
	"fmt"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
)

// SwapModel loads a new model and unloads the old one via the ML Service.
// Called by MTLF (via processor callback) during model hot-swap after retraining.
// Returns the new model ID assigned by the ML Service.
func (a *AnlfService) SwapModel(newModelUrl, oldModelId string) (string, error) {
	cfg := a.config()
	mlServiceEndpoint := mlServiceEndpoint(cfg)
	if mlServiceEndpoint == "" {
		return "", fmt.Errorf("ML Service not configured")
	}

	if a.mlClient == nil {
		return "", fmt.Errorf("ML Service client not initialized")
	}

	newModelId, err := a.mlClient.InitializeModel(a.nwdaf.CancelContext(), newModelUrl)
	if err != nil {
		return "", fmt.Errorf("failed to load new model %s: %w", newModelUrl, err)
	}
	logger.AnlfLog.Infof("SwapModel: loaded modelId=%s", newModelId)

	if oldModelId != "" {
		if unloadErr := a.mlClient.UnloadModel(a.nwdaf.CancelContext(), oldModelId); unloadErr != nil {
			logger.AnlfLog.Warnf("SwapModel: unload failed modelId=%s err=%v", oldModelId, unloadErr)
		} else {
			logger.AnlfLog.Infof("SwapModel: unloaded modelId=%s", oldModelId)
		}
	}

	return newModelId, nil
}

// InitializeMlModel initializes the ML model directly using the ML Service.
// Deduplicates model loading: if modelUrl is already loaded by another
// subscription, reuses the existing modelId from SharedModelRegistry.
func (a *AnlfService) InitializeMlModel(
	nwdafSubId string, mlInfo *nwdaf_context.MlModelInfo, modelUrl string,
) {
	logger.AnlfLog.Infof("LoadMlModel: start sub=%s", nwdafSubId)

	// Get ML service configuration
	cfg := a.config()
	mlServiceEndpoint := mlServiceEndpoint(cfg)
	if mlServiceEndpoint == "" {
		logger.AnlfLog.Warnf("ML Service not configured, cannot initialize model")
		mlInfo.SetModelFailed(nil)
		return
	}

	ctx := nwdaf_context.GetSelf()

	// Layer 1: Registry — check if model already loaded
	shared, isNew := ctx.GetOrCreateSharedModel(modelUrl, mlInfo.Event)
	shared.AddSubscriber(nwdafSubId)

	if !isNew {
		// Another goroutine is loading or has already loaded; wait for completion.
		shared.WaitLoaded()
		existingModelId := shared.GetModelId()
		if existingModelId != "" {
			mlInfo.SetModelReady(existingModelId)
			logger.AnlfLog.Infof("LoadMlModel: reused sub=%s modelId=%s",
				nwdafSubId, existingModelId)
		} else {
			mlInfo.SetModelFailed(fmt.Errorf("shared model load failed for url=%s", modelUrl))
			logger.AnlfLog.Errorf("LoadMlModel: shared-load-failed sub=%s", nwdafSubId)
		}
		return
	}

	// isNew=true: this goroutine is responsible for loading
	if a.mlClient == nil {
		err := fmt.Errorf("ML Service client not initialized")
		logger.AnlfLog.Errorf("Failed to initialize ML model: %v", err)
		shared.LoadDone()
		mlInfo.SetModelFailed(err)
		return
	}

	modelId, err := a.mlClient.InitializeModel(a.nwdaf.CancelContext(), modelUrl)
	if err != nil {
		logger.AnlfLog.Errorf("LoadMlModel failed: sub=%s err=%v", nwdafSubId, err)
		shared.LoadDone()
		mlInfo.SetModelFailed(err)
		return
	}

	shared.SetModelId(modelId)
	mlInfo.SetModelReady(modelId)
	logger.AnlfLog.Infof("LoadMlModel: ready sub=%s modelId=%s", nwdafSubId, modelId)
	// Note: accuracy monitor is started by the caller (processor) after this returns
}
