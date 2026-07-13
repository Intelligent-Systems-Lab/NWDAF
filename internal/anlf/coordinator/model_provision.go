package coordinator

import (
	"github.com/google/uuid"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

type ModelProvisionAction struct {
	Request contract.ApplySubscriptionRuntimeRequest
	Event   *contract.ModelProvisionEvent
}

// PlanModelProvisionActions resolves one callback payload into model-activation
// actions after the HTTP edge has already parsed and validated the body.
func (a *Coordinator) PlanModelProvisionActions(
	notif *contract.ModelProvisionNotification,
) []ModelProvisionAction {
	if notif == nil {
		return nil
	}

	actions := make([]ModelProvisionAction, 0, len(notif.EventNotifications))
	for _, eventNotif := range notif.EventNotifications {
		if eventNotif.MLFileAddr == nil || eventNotif.MLFileAddr.MLModelUrl == "" {
			anlfLog.Warnf("No ML model URL in notification for event %s", eventNotif.Event)
			continue
		}

		if eventNotif.ModelUniqueID != nil {
			providerID := eventNotif.ModelProviderID
			if providerID == "" {
				providerID = "external-mtlf"
			}
			actions = append(actions, ModelProvisionAction{Event: &contract.ModelProvisionEvent{
				EventID: "mtlf:" + uuid.NewString(),
				Source:  "MTLF_PROVISION",
				ModelIdentity: contract.ModelIdentity{
					ProviderID: providerID, ModelUniqueID: *eventNotif.ModelUniqueID,
				},
				ModelUpdateInd: eventNotif.ModelUpdateInd,
				Artifact:       contract.ModelArtifact{MLModelURL: eventNotif.MLFileAddr.MLModelUrl},
				AnalyticsEvent: string(eventNotif.Event),
				NotificationCorrelation: contract.ProvisionNotificationCorrelation{
					NotificationCorrelationID: eventNotif.NotifCorreId,
					ProvisionSubscriptionID:   notif.SubscriptionID,
				},
			}})
			continue
		}

		nwdafSubID := a.resolveNwdafSubscriptionID(eventNotif.NotifCorreId, notif.SubscriptionID)
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
		request, err := a.BuildSubscriptionRuntimeRequest(nwdafSubID, &contract.ProvisionContext{
			Source:              provisionSourceMTLF,
			MtlfSubscriptionID:  mtlfSubscriptionID,
			NotifSubscriptionID: notif.SubscriptionID,
			MLEventNotification: contract.MLEventNotification{MlEventNotif: eventNotif.MlEventNotif},
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
func (a *Coordinator) StartModelProvisionActions(actions []ModelProvisionAction) {
	if len(actions) == 0 {
		return
	}

	a.launchOwnedTask(func() {
		a.ExecuteModelProvisionActions(actions)
	})
}

// ExecuteModelProvisionActions applies one callback-derived action batch in
// order so shared subscription state stays aligned with the model being loaded.
func (a *Coordinator) ExecuteModelProvisionActions(actions []ModelProvisionAction) {
	for _, action := range actions {
		if action.Event != nil {
			if _, err := a.ApplyModelProvisionEvent(*action.Event); err != nil {
				anlfLog.Errorf("Model provision event forwarding failed: err=%v", err)
			}
			continue
		}
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

func (a *Coordinator) resolveNwdafSubscriptionID(notifCorrelationID, mtlfSubscriptionID string) string {
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
