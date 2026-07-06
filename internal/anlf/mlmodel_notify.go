package anlf

import (
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

type ModelProvisionAction struct {
	NwdafSubID string
	ModelURL   string
	Event      models.NwdafEvent
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

		nwdafSubID := eventNotif.NotifCorreId
		if nwdafSubID == "" {
			nwdafSubID = notif.SubscriptionId
			anlfLog.Debugf("Using MTLF subscriptionId as correlation: %s", nwdafSubID)
		}
		if nwdafSubID == "" {
			anlfLog.Warnf("No correlation found for event %s", eventNotif.Event)
			continue
		}

		modelURL := eventNotif.MLFileAddr.MLModelUrl
		anlfLog.Infof("MlModelProvisionNotify: model available sub=%s event=%s",
			nwdafSubID, eventNotif.Event)
		actions = append(actions, ModelProvisionAction{
			NwdafSubID: nwdafSubID,
			ModelURL:   modelURL,
			Event:      eventNotif.Event,
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
		if action.NwdafSubID == "" || action.ModelURL == "" {
			continue
		}

		if cancelCtx := a.nwdaf.CancelContext(); cancelCtx != nil && cancelCtx.Err() != nil {
			anlfLog.Infof("Skipping ML model initialization during shutdown: sub=%s", action.NwdafSubID)
			return
		}

		ctx := nwdaf_context.GetSelf()
		mlInfo := ctx.GetMlModelInfo(action.NwdafSubID)
		if mlInfo == nil {
			anlfLog.Warnf("No ML model info found for subscription %s", action.NwdafSubID)
			continue
		}

		mlInfo.RLock()
		oldModelURL := mlInfo.ModelUrl
		mlInfo.RUnlock()
		if oldModelURL != "" && oldModelURL != action.ModelURL {
			// Known limitation: the old model is detached before the new model is
			// proven ready. If the new initialization fails, this callback path does
			// not restore the previous shared-model or monitor state. We are
			// intentionally leaving that deeper replacement workflow for later work
			// because this area is expected to move behind Python-owned services.
			if oldShared := ctx.GetSharedModel(oldModelURL); oldShared != nil {
				remaining := oldShared.RemoveSubscriber(action.NwdafSubID)
				if remaining == 0 {
					a.StopAccuracyMonitorForModel(oldModelURL)
					ctx.DeleteSharedModel(oldModelURL)
				}
			}
		}

		mlInfo.SetModelUrl(action.ModelURL)
		a.InitializeMlModel(action.NwdafSubID, mlInfo, action.ModelURL)
		a.StartOwnedAccuracyMonitorForModel(action.ModelURL)
	}
}
