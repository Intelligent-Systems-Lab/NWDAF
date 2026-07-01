package anlf

import (
	"net/http"

	"github.com/gin-gonic/gin"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi"
	"github.com/free5gc/openapi/models"
)

// HandleMlModelProvisionNotify handles ML model provision notifications from MTLF.
// Per TS 29.520 §5.4.5.2: callback body is []NwdafMlModelProvNotif.
func (a *AnlfService) HandleMlModelProvisionNotify(c *gin.Context) {
	var notifications []models.NwdafMlModelProvNotif
	requestBody, err := c.GetRawData()
	if err != nil {
		anlfLog.Errorf("Get Request Body error: %+v", err)
		util.GinProblemJson(c, openapi.ProblemDetailsSystemFailure(err.Error()))
		return
	}

	if deserializeErr := openapi.Deserialize(&notifications, requestBody, "application/json"); deserializeErr != nil {
		anlfLog.Errorf("Failed to deserialize ML model provision notification: %v", deserializeErr)
		util.GinProblemJson(c, openapi.ProblemDetailsMalformedReqSyntax(deserializeErr.Error()))
		return
	}

	if len(notifications) == 0 {
		anlfLog.Warn("Empty ML model provision notification received")
		c.Status(http.StatusNoContent)
		return
	}

	anlfLog.Infof("Handle MlModelProvisionNotify: notifications=%d", len(notifications))

	ctx := nwdaf_context.GetSelf()
	for i := range notifications {
		a.processMlModelNotification(ctx, &notifications[i])
	}

	c.Status(http.StatusNoContent)
}

func (a *AnlfService) processMlModelNotification(
	ctx *nwdaf_context.NWDAFContext,
	notif *models.NwdafMlModelProvNotif,
) {
	var nwdafSubID string
	for _, eventNotif := range notif.EventNotifs {
		if eventNotif.NotifCorreId != "" {
			nwdafSubID = eventNotif.NotifCorreId
			break
		}
	}

	if nwdafSubID == "" {
		nwdafSubID = notif.SubscriptionId
		anlfLog.Debugf("Using MTLF subscriptionId as correlation: %s", nwdafSubID)
	}

	mlInfo := ctx.GetMlModelInfo(nwdafSubID)
	if mlInfo == nil {
		anlfLog.Warnf("No ML model info found for subscription %s", nwdafSubID)
		return
	}

	for _, eventNotif := range notif.EventNotifs {
		if eventNotif.MLFileAddr == nil || eventNotif.MLFileAddr.MLModelUrl == "" {
			anlfLog.Warnf("No ML model URL in notification for event %s", eventNotif.Event)
			continue
		}

		modelURL := eventNotif.MLFileAddr.MLModelUrl
		anlfLog.Infof("MlModelProvisionNotify: model available sub=%s event=%s",
			nwdafSubID, eventNotif.Event)
		mlInfo.SetModelUrl(modelURL)

		if cancelCtx := a.nwdaf.CancelContext(); cancelCtx != nil && cancelCtx.Err() != nil {
			anlfLog.Infof("Skipping ML model initialization during shutdown: sub=%s", nwdafSubID)
			continue
		}

		go func(subID string, info *nwdaf_context.MlModelInfo, url string) {
			a.InitializeMlModel(subID, info, url)
			a.StartOwnedAccuracyMonitorForModel(url)
		}(nwdafSubID, mlInfo, modelURL)
	}
}
