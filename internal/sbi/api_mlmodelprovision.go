package sbi

import (
	"net/http"

	"github.com/gin-gonic/gin"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
)

// MlModelAddr represents ML model file address per TS 29.520
type MlModelAddr struct {
	MLModelUrl string `json:"mLModelUrl,omitempty"`
	MlFileFqdn string `json:"mlFileFqdn,omitempty"`
}

// MlEventNotif represents a single ML event notification per TS 29.520
type MlEventNotif struct {
	Event        string       `json:"event"`
	NotifCorreId string       `json:"notifCorreId,omitempty"`
	MLFileAddr   *MlModelAddr `json:"mLFileAddr,omitempty"`
}

// NwdafMlModelProvNotif represents ML Model Provision notification per TS 29.520
type NwdafMlModelProvNotif struct {
	SubscriptionId string         `json:"subscriptionId"`
	EventNotifs    []MlEventNotif `json:"eventNotifs"`
}

// HandleMlModelProvisionNotify handles ML Model Provision notifications from MTLF
// Per TS 29.520 §5.4.5.2: Callback for ML Model Provision notifications
// POST /mlmodel-notify
func (s *Server) HandleMlModelProvisionNotify(c *gin.Context) {
	logger.SBILog.Info("Received ML Model Provision notification")

	// Per TS 29.520, notification body is an array of NwdafMlModelProvNotif
	var notifications []NwdafMlModelProvNotif
	if err := c.ShouldBindJSON(&notifications); err != nil {
		logger.SBILog.Errorf("Failed to parse ML Model Provision notification: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid notification format"})
		return
	}

	if len(notifications) == 0 {
		logger.SBILog.Warn("Empty ML Model Provision notification received")
		c.Status(http.StatusNoContent)
		return
	}

	ctx := nwdaf_context.GetSelf()

	// Process each notification
	for _, notif := range notifications {
		s.processMlModelNotification(ctx, &notif)
	}

	c.Status(http.StatusNoContent)
}

// processMlModelNotification processes a single ML model notification
func (s *Server) processMlModelNotification(ctx *nwdaf_context.NWDAFContext, notif *NwdafMlModelProvNotif) {
	logger.SBILog.Infof("Processing ML Model notification for subscription: %s", notif.SubscriptionId)

	// Find the corresponding NWDAF subscription by correlation ID (we used subscriptionId as notifId)
	// The notifCorreId in eventNotif should match our NWDAF subscription ID
	var nwdafSubId string
	for _, eventNotif := range notif.EventNotifs {
		if eventNotif.NotifCorreId != "" {
			nwdafSubId = eventNotif.NotifCorreId
			break
		}
	}

	if nwdafSubId == "" {
		// Fall back to subscriptionId if no notifCorreId
		nwdafSubId = notif.SubscriptionId
		logger.SBILog.Debugf("Using MTLF subscriptionId as correlation: %s", nwdafSubId)
	}

	// Get ML model info for this subscription
	mlInfo := ctx.GetMlModelInfo(nwdafSubId)
	if mlInfo == nil {
		logger.SBILog.Warnf("No ML model info found for subscription %s", nwdafSubId)
		return
	}

	// Process event notifications
	for _, eventNotif := range notif.EventNotifs {
		if eventNotif.MLFileAddr == nil || eventNotif.MLFileAddr.MLModelUrl == "" {
			logger.SBILog.Warnf("No ML model URL in notification for event %s", eventNotif.Event)
			continue
		}

		modelUrl := eventNotif.MLFileAddr.MLModelUrl
		logger.SBILog.Infof("Received ML model URL: %s for event %s", modelUrl, eventNotif.Event)

		// Update model info with URL
		mlInfo.SetModelUrl(modelUrl)

		// Initialize the model with ML service
		go s.initializeMlModel(nwdafSubId, mlInfo, modelUrl)
	}
}

// initializeMlModel initializes the ML model by calling the ML inference service
func (s *Server) initializeMlModel(nwdafSubId string, mlInfo *nwdaf_context.MlModelInfo, modelUrl string) {
	logger.SBILog.Infof("Initializing ML model for subscription %s from %s", nwdafSubId, modelUrl)

	// Get ML service configuration
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil || cfg.Configuration.DataCollection == nil ||
		cfg.Configuration.DataCollection.MlService == nil || !cfg.Configuration.DataCollection.MlService.Enabled {
		logger.SBILog.Warnf("ML Service not configured, cannot initialize model")
		mlInfo.SetModelFailed(nil)
		return
	}

	mlServiceEndpoint := cfg.Configuration.DataCollection.MlService.Endpoint
	if mlServiceEndpoint == "" {
		logger.SBILog.Warnf("ML Service endpoint not configured")
		mlInfo.SetModelFailed(nil)
		return
	}

	// Create ML service client and initialize model
	mlClient := consumer.NewMlServiceClient(mlServiceEndpoint)
	modelId, err := mlClient.InitializeModel(modelUrl)
	if err != nil {
		logger.SBILog.Errorf("Failed to initialize ML model: %v", err)
		mlInfo.SetModelFailed(err)
		return
	}

	// Update model info with ready status
	mlInfo.SetModelReady(modelId)
	logger.SBILog.Infof("ML model initialized successfully: subscription=%s, modelId=%s", nwdafSubId, modelId)
}

// getMlModelRoutes returns routes for ML Model Provision callback
func (s *Server) getMlModelRoutes() []Route {
	return []Route{
		{
			Name:    "MlModelProvisionNotify",
			Method:  "POST",
			Pattern: "",
			APIFunc: s.HandleMlModelProvisionNotify,
		},
	}
}
