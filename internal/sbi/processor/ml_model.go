package processor

import (
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
)

// InitializeMlModel initializes the ML model directly using the ML Service
// Can be called by:
// 1. triggerMlModelProvisioning (when using static URL)
// 2. processMlModelNotification (when receiving model URL from MTLF)
func (p *Processor) InitializeMlModel(nwdafSubId string, mlInfo *nwdaf_context.MlModelInfo, modelUrl string) {
	logger.ProcLog.Infof("Initializing ML model for subscription %s from %s", nwdafSubId, modelUrl)

	// Get ML service configuration
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil ||
		cfg.Configuration.MlService == nil || !cfg.Configuration.MlService.Enabled {
		logger.ProcLog.Warnf("ML Service not configured, cannot initialize model")
		mlInfo.SetModelFailed(nil)
		return
	}

	mlServiceEndpoint := cfg.Configuration.MlService.Endpoint
	if mlServiceEndpoint == "" {
		logger.ProcLog.Warnf("ML Service endpoint not configured")
		mlInfo.SetModelFailed(nil)
		return
	}

	// Create ML service client and initialize model
	mlClient := consumer.NewMlServiceClient(mlServiceEndpoint)
	modelId, err := mlClient.InitializeModel(modelUrl)
	if err != nil {
		logger.ProcLog.Errorf("Failed to initialize ML model: %v", err)
		mlInfo.SetModelFailed(err)
		return
	}

	// Update model info with ready status
	mlInfo.SetModelReady(modelId)
	logger.ProcLog.Infof("ML model initialized successfully: subscription=%s, modelId=%s", nwdafSubId, modelId)
}
