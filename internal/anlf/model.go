package anlf

import (
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
)

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
		existingModelId := shared.GetModelId()
		if existingModelId != "" {
			// Reuse existing model — skip ML service call
			mlInfo.SetModelReady(existingModelId)
			logger.AnlfLog.Infof("Reusing ML model: sub=%s, modelId=%s (already loaded)",
				nwdafSubId, existingModelId)
			return
		}
	}

	// First subscriber — init via ML service
	mlClient := consumer.NewMlServiceClient(mlServiceEndpoint)
	modelId, err := mlClient.InitializeModel(modelUrl)
	if err != nil {
		logger.AnlfLog.Errorf("Failed to initialize ML model: %v", err)
		mlInfo.SetModelFailed(err)
		return
	}

	shared.SetModelId(modelId)
	mlInfo.SetModelReady(modelId)
	logger.AnlfLog.Infof("ML model initialized: sub=%s, modelId=%s", nwdafSubId, modelId)
	// Note: accuracy monitor is started by the caller (processor) after this returns
}
