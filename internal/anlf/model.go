package anlf

import (
	"errors"
	"fmt"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

const provisionSourceMTLF = "MTLF_PROVISION"

func (a *AnlfService) BuildSubscriptionRuntimeRequest(
	subscriptionID string,
	provisionContext *ProvisionContext,
) (ApplySubscriptionRuntimeRequest, error) {
	ctx := nwdaf_context.GetSelf()
	if ctx == nil {
		return ApplySubscriptionRuntimeRequest{}, fmt.Errorf("NWDAF context not initialized")
	}

	subscription := ctx.GetSubscription(subscriptionID)
	if subscription == nil {
		return ApplySubscriptionRuntimeRequest{}, fmt.Errorf("subscription %s not found", subscriptionID)
	}

	return ApplySubscriptionRuntimeRequest{
		Subscription: SubscriptionRuntimeContext{
			SubscriptionID:     subscription.ID,
			NotifCorrID:        subscription.NotifCorrId,
			EvtReq:             subscription.EvtReq,
			EventSubscriptions: subscription.EventSubs,
		},
		ProvisionContext: provisionContext,
	}, nil
}

func (a *AnlfService) ApplySubscriptionRuntime(
	request ApplySubscriptionRuntimeRequest,
) (*ApplySubscriptionRuntimeResponse, error) {
	if a.anlfBackend == nil {
		return nil, fmt.Errorf("AnLF backend client not initialized")
	}

	response, err := a.anlfBackend.ApplySubscriptionRuntime(a.nwdaf.CancelContext(), request)
	if err != nil {
		return nil, err
	}

	subscriptionID := request.Subscription.SubscriptionID
	switch response.Result {
	case ApplyResultPendingProvision:
		logger.AnlfLog.Debugf("AnLF runtime pending provision: sub=%s", subscriptionID)
		return response, nil
	case ApplyResultActivated, ApplyResultReused, ApplyResultReplaced:
		if response.ActiveModelReference == "" {
			return response, fmt.Errorf("AnLF backend returned %s without active model reference", response.Result)
		}
		a.applyModelReferenceCorrelation(subscriptionID, response.ActiveModelReference)
		logger.AnlfLog.Infof("AnLF runtime applied: sub=%s result=%s", subscriptionID, response.Result)
		return response, nil
	case ApplyResultFailedUsingPrevious:
		return response, fmt.Errorf("runtime replacement failed for subscription %s: %s", subscriptionID, response.Message)
	case ApplyResultFailedNoPrevious:
		if mlInfo := nwdaf_context.GetSelf().GetMlModelInfo(subscriptionID); mlInfo != nil {
			mlInfo.SetModelFailed(errors.New(response.Message))
		}
		return response, fmt.Errorf("runtime activation failed for subscription %s: %s", subscriptionID, response.Message)
	default:
		return response, fmt.Errorf("AnLF backend returned unknown apply result %q", response.Result)
	}
}

func (a *AnlfService) ApplySubscriptionRegistration(subscriptionID string) error {
	request, err := a.BuildSubscriptionRuntimeRequest(subscriptionID, nil)
	if err != nil {
		return err
	}
	_, err = a.ApplySubscriptionRuntime(request)
	return err
}

func (a *AnlfService) ReleaseSubscriptionRuntime(subscriptionID string) error {
	defer a.removeModelReferenceCorrelation(subscriptionID)

	if a.anlfBackend == nil {
		return nil
	}
	if err := a.anlfBackend.ReleaseSubscriptionRuntime(a.nwdaf.CancelContext(), subscriptionID); err != nil {
		return err
	}
	return nil
}

func (a *AnlfService) ApplyRetrainedModel(oldModelReference, newModelReference string) error {
	ctx := nwdaf_context.GetSelf()
	if ctx == nil {
		return fmt.Errorf("NWDAF context not initialized")
	}

	var subscriptionIDs []string
	if oldModelReference != "" {
		if shared := ctx.GetSharedModel(oldModelReference); shared != nil {
			subscriptionIDs = shared.GetSubscriberIDs()
		}
	} else {
		for _, subscription := range ctx.GetAllSubscriptions() {
			mlInfo := ctx.GetMlModelInfo(subscription.ID)
			if mlInfo != nil && mlInfo.GetModelURL() == "" {
				subscriptionIDs = append(subscriptionIDs, subscription.ID)
			}
		}
	}

	if len(subscriptionIDs) == 0 {
		return fmt.Errorf("no subscriptions use model reference %q", oldModelReference)
	}

	var applyErrors []error
	for _, subscriptionID := range subscriptionIDs {
		mlInfo := ctx.GetMlModelInfo(subscriptionID)
		if mlInfo == nil {
			continue
		}
		mlInfo.RLock()
		event := mlInfo.Event
		mtlfSubscriptionID := mlInfo.MtlfSubId
		mlInfo.RUnlock()

		request, err := a.BuildSubscriptionRuntimeRequest(subscriptionID, &ProvisionContext{
			Source:              provisionSourceMTLF,
			MtlfSubscriptionID:  mtlfSubscriptionID,
			NotifSubscriptionID: mtlfSubscriptionID,
			MLEventNotification: MLEventNotification{
				MlEventNotif: models.MlEventNotif{
					Event:        event,
					NotifCorreId: subscriptionID,
					MLFileAddr:   &models.MlModelAddr{MLModelUrl: newModelReference},
				},
				ModelUpdateInd: true,
			},
		})
		if err == nil {
			_, err = a.ApplySubscriptionRuntime(request)
		}
		if err != nil {
			applyErrors = append(applyErrors, err)
		}
	}
	return errors.Join(applyErrors...)
}

func (a *AnlfService) applyModelReferenceCorrelation(subscriptionID, modelReference string) {
	ctx := nwdaf_context.GetSelf()
	mlInfo := ctx.GetMlModelInfo(subscriptionID)
	if mlInfo == nil {
		logger.AnlfLog.Warnf("No ML model info found for subscription %s", subscriptionID)
		return
	}

	oldModelReference := mlInfo.GetModelURL()
	if oldModelReference != "" && oldModelReference != modelReference {
		if oldShared := ctx.GetSharedModel(oldModelReference); oldShared != nil {
			if remaining := oldShared.RemoveSubscriber(subscriptionID); remaining == 0 {
				ctx.DeleteSharedModel(oldModelReference)
			}
		}
		a.StopAccuracyMonitorForModel(oldModelReference)
	}

	mlInfo.SetModelUrl(modelReference)
	mlInfo.SetModelReady()
	shared, _ := ctx.GetOrCreateSharedModel(modelReference, mlInfo.Event)
	shared.AddSubscriber(subscriptionID)
	a.StartOwnedAccuracyMonitorForModel(modelReference)
}

func (a *AnlfService) removeModelReferenceCorrelation(subscriptionID string) {
	ctx := nwdaf_context.GetSelf()
	if ctx == nil {
		return
	}
	mlInfo := ctx.GetMlModelInfo(subscriptionID)
	if mlInfo == nil {
		return
	}

	modelReference := mlInfo.GetModelURL()
	if modelReference != "" {
		if shared := ctx.GetSharedModel(modelReference); shared != nil {
			if remaining := shared.RemoveSubscriber(subscriptionID); remaining == 0 {
				ctx.DeleteSharedModel(modelReference)
			}
		}
		a.StopAccuracyMonitorForModel(modelReference)
	}
	ctx.DeleteMlModelInfo(subscriptionID)
}
