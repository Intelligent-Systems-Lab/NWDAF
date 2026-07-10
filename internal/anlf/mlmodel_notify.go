package anlf

import (
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

type ModelProvisionAction struct {
	Request ApplySubscriptionRuntimeRequest
}

// PlanModelProvisionActions resolves one callback payload into model-activation
// actions after the HTTP edge has already parsed and validated the body.
func (a *AnlfService) PlanModelProvisionActions(
	notif *models.NwdafMlModelProvNotif,
) []ModelProvisionAction {
	if notif == nil {
		return nil
	}

	actions := make([]ModelProvisionAction, 0, len(notif.EventNotifs))
	for _, eventNotif := range notif.EventNotifs {
		if eventNotif.MLFileAddr == nil || eventNotif.MLFileAddr.MLModelUrl == "" {
			anlfLog.Warnf("No ML model URL in notification for event %s", eventNotif.Event)
			continue
		}

		nwdafSubID := a.resolveNwdafSubscriptionID(eventNotif.NotifCorreId, notif.SubscriptionId)
		if nwdafSubID == "" {
			anlfLog.Warnf("No correlation found for event %s", eventNotif.Event)
			continue
		}

		anlfLog.Infof("MlModelProvisionNotify: model available sub=%s event=%s",
			nwdafSubID, eventNotif.Event)
		mlInfo := nwdaf_context.GetSelf().GetMlModelInfo(nwdafSubID)
		if mlInfo == nil {
			anlfLog.Warnf("No ML model info found for subscription %s", nwdafSubID)
			continue
		}
		mlInfo.RLock()
		mtlfSubscriptionID := mlInfo.MtlfSubId
		mlInfo.RUnlock()
		request, err := a.BuildSubscriptionRuntimeRequest(nwdafSubID, &ProvisionContext{
			Source:              provisionSourceMTLF,
			MtlfSubscriptionID:  mtlfSubscriptionID,
			NotifSubscriptionID: notif.SubscriptionId,
			MLEventNotification: MLEventNotification{MlEventNotif: eventNotif},
		})
		if err != nil {
			anlfLog.Warnf("Cannot build runtime apply request: sub=%s err=%v", nwdafSubID, err)
			continue
		}
		actions = append(actions, ModelProvisionAction{
			Request: request,
		})
	}

	return actions
}

// StartModelProvisionActions runs one callback-derived action batch under the
// AnLF-owned lifecycle boundary.
func (a *AnlfService) StartModelProvisionActions(actions []ModelProvisionAction) {
	if len(actions) == 0 {
		return
	}

	a.launchOwnedTask(func() {
		a.ExecuteModelProvisionActions(actions)
	})
}

// ExecuteModelProvisionActions applies one callback-derived action batch in
// order so shared subscription state stays aligned with the model being loaded.
func (a *AnlfService) ExecuteModelProvisionActions(actions []ModelProvisionAction) {
	for _, action := range actions {
		subscriptionID := action.Request.Subscription.SubscriptionID
		if subscriptionID == "" {
			continue
		}

		if cancelCtx := a.nwdaf.CancelContext(); cancelCtx != nil && cancelCtx.Err() != nil {
			anlfLog.Infof("Skipping runtime apply during shutdown: sub=%s", subscriptionID)
			return
		}

		if _, err := a.ApplySubscriptionRuntime(action.Request); err != nil {
			anlfLog.Errorf("Runtime apply failed: sub=%s err=%v", subscriptionID, err)
		}
	}
}

func (a *AnlfService) resolveNwdafSubscriptionID(notifCorrelationID, mtlfSubscriptionID string) string {
	ctx := nwdaf_context.GetSelf()
	if notifCorrelationID != "" && ctx.GetSubscription(notifCorrelationID) != nil {
		return notifCorrelationID
	}
	if mtlfSubscriptionID == "" {
		return ""
	}
	for _, subscription := range ctx.GetAllSubscriptions() {
		mlInfo := ctx.GetMlModelInfo(subscription.ID)
		if mlInfo == nil {
			continue
		}
		mlInfo.RLock()
		matches := mlInfo.MtlfSubId == mtlfSubscriptionID
		mlInfo.RUnlock()
		if matches {
			return subscription.ID
		}
	}
	return ""
}
