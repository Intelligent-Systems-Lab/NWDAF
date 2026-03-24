package anlf

import (
	"fmt"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
)

// SwapModel loads a new model and unloads the old one via the ML Service.
// Called by MTLF (via processor callback) during model hot-swap after retraining.
// Returns the new model ID assigned by the ML Service.
func (a *AnlfService) SwapModel(newModelUrl, oldModelId string) (string, error) {
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil ||
		cfg.Configuration.MlService == nil || !cfg.Configuration.MlService.Enabled ||
		cfg.Configuration.MlService.Endpoint == "" {
		return "", fmt.Errorf("ML Service not configured")
	}

	mlClient := consumer.NewMlServiceClient(cfg.Configuration.MlService.Endpoint)

	newModelId, err := mlClient.InitializeModel(newModelUrl)
	if err != nil {
		return "", fmt.Errorf("failed to load new model %s: %w", newModelUrl, err)
	}
	logger.AnlfLog.Infof("SwapModel: loaded new model: url=%s modelId=%s", newModelUrl, newModelId)

	if oldModelId != "" {
		if unloadErr := mlClient.UnloadModel(oldModelId); unloadErr != nil {
			logger.AnlfLog.Warnf("SwapModel: failed to unload old model ID %s: %v", oldModelId, unloadErr)
		} else {
			logger.AnlfLog.Infof("SwapModel: unloaded old model ID %s", oldModelId)
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
	logger.AnlfLog.Infof("Initializing ML model: sub=%s, url=%s", nwdafSubId, modelUrl)

	// Get ML service configuration
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil ||
		cfg.Configuration.MlService == nil || !cfg.Configuration.MlService.Enabled {
		logger.AnlfLog.Warnf("ML Service not configured, cannot initialize model")
		mlInfo.SetModelFailed(nil)
		return
	}

	mlServiceEndpoint := cfg.Configuration.MlService.Endpoint
	if mlServiceEndpoint == "" {
		logger.AnlfLog.Warnf("ML Service endpoint not configured")
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
			logger.AnlfLog.Infof("Reusing ML model: sub=%s, modelId=%s (already loaded)",
				nwdafSubId, existingModelId)
		} else {
			mlInfo.SetModelFailed(fmt.Errorf("shared model load failed for url=%s", modelUrl))
			logger.AnlfLog.Errorf("Shared model load failed: sub=%s url=%s", nwdafSubId, modelUrl)
		}
		return
	}

	// isNew=true: this goroutine is responsible for loading
	mlClient := consumer.NewMlServiceClient(mlServiceEndpoint)
	modelId, err := mlClient.InitializeModel(modelUrl)
	if err != nil {
		logger.AnlfLog.Errorf("Failed to initialize ML model: %v", err)
		shared.LoadDone()
		mlInfo.SetModelFailed(err)
		return
	}

	shared.SetModelId(modelId)
	mlInfo.SetModelReady(modelId)
	logger.AnlfLog.Infof("ML model initialized: sub=%s, modelId=%s", nwdafSubId, modelId)
	// Note: accuracy monitor is started by the caller (processor) after this returns
}
