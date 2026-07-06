package anlf

import (
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

// ProcessMlModelProvisionNotifications applies a provision-notify callback after
// the HTTP edge has already parsed and validated the payload.
func (a *AnlfService) ProcessMlModelProvisionNotifications(notifications []models.NwdafMlModelProvNotif) {
	anlfLog.Infof("Process MlModelProvisionNotify: notifications=%d", len(notifications))
	ctx := nwdaf_context.GetSelf()
	for i := range notifications {
		a.processMlModelNotification(ctx, &notifications[i])
	}
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
