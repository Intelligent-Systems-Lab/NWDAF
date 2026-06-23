package sbi

import (
	"net/http"

	"github.com/gin-gonic/gin"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi"
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
		util.GinProblemJson(c, openapi.ProblemDetailsMalformedReqSyntax(err.Error()))
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
		go s.Processor().InitializeMlModel(nwdafSubId, mlInfo, modelUrl)
	}
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
