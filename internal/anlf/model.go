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

	return a.BuildSubscriptionRuntimeRequestForSubscription(subscription, provisionContext), nil
}

func (a *AnlfService) BuildSubscriptionRuntimeRequestForSubscription(
	subscription *nwdaf_context.Subscription,
	provisionContext *ProvisionContext,
) ApplySubscriptionRuntimeRequest {
	return ApplySubscriptionRuntimeRequest{
		Subscription: SubscriptionRuntimeContext{
			SubscriptionID:     subscription.ID,
			NotifCorrID:        subscription.NotifCorrId,
			EvtReq:             subscription.EvtReq,
			EventSubscriptions: subscription.EventSubs,
		},
		ProvisionContext:  provisionContext,
		ReportCallbackURI: a.BuildAnalyticsReportCallbackURI(subscription.ID),
	}
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
	if response.RuntimeRevision <= 0 || response.CollectionRequirements.SamplingIntervalSeconds <= 0 ||
		len(response.CollectionRequirements.RequiredMeasurements) == 0 {
		return response, fmt.Errorf("AnLF backend returned incomplete runtime metadata")
	}
	if subscription := nwdaf_context.GetSelf().GetSubscription(subscriptionID); subscription != nil {
		_, _, sourceIDs, _ := subscription.RuntimeSnapshot()
		subscription.SetRuntime(
			response.RuntimeRevision,
			nwdaf_context.CollectionRequirements{
				SamplingIntervalSeconds: response.CollectionRequirements.SamplingIntervalSeconds,
				RequiredMeasurements:    response.CollectionRequirements.RequiredMeasurements,
			},
			sourceIDs,
		)
	}
	switch response.Result {
	case ApplyResultPendingProvision:
		logger.AnlfLog.Debugf("AnLF runtime pending provision: sub=%s", subscriptionID)
		return response, nil
	case ApplyResultActivated, ApplyResultReused, ApplyResultReplaced:
		if response.ActiveModelReference == "" {
			return response, fmt.Errorf("AnLF backend returned %s without active model reference", response.Result)
		}
		a.applyModelReferenceCorrelation(subscriptionID, response.ActiveModelReference)
		if request.ProvisionContext != nil {
			if syncErr := a.SyncCurrentObservationBindings(subscriptionID); syncErr != nil {
				return response, fmt.Errorf("sync observation bindings for new runtime revision: %w", syncErr)
			}
		}
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

func (a *AnlfService) BuildCurrentObservationBindings(subscriptionID string) []ObservationBinding {
	ctx := nwdaf_context.GetSelf()
	subscription := ctx.GetSubscription(subscriptionID)
	if subscription == nil {
		return nil
	}
	requirements := subscription.CollectionRequirementsSnapshot()
	bindings := make([]ObservationBinding, 0)
	for _, resource := range ctx.GetNwdafSubResources(subscriptionID) {
		bindings = append(bindings, ObservationBinding{
			ObservationSourceID: resource.CorrelationId,
			Source:              ObservationSource{SourceType: "SMF_UPF", Supi: resource.Supi},
			SubscriptionScope:   SubscriptionScope{OriginalGroupID: resource.OriginalGroupId},
			CollectionProfile: CollectionRequirements{
				SamplingIntervalSeconds: requirements.SamplingIntervalSeconds,
				RequiredMeasurements:    append([]string(nil), requirements.RequiredMeasurements...),
			},
		})
	}
	return bindings
}

func (a *AnlfService) SyncCurrentObservationBindings(subscriptionID string) error {
	ctx := nwdaf_context.GetSelf()
	subscription := ctx.GetSubscription(subscriptionID)
	if subscription == nil {
		return fmt.Errorf("subscription %s not found", subscriptionID)
	}
	revision, _, _, active := subscription.RuntimeSnapshot()
	if !active {
		return fmt.Errorf("subscription %s is inactive", subscriptionID)
	}
	return a.SyncObservationBindings(
		subscriptionID,
		revision,
		a.BuildCurrentObservationBindings(subscriptionID),
	)
}

func (a *AnlfService) ApplyInitialSubscriptionRuntime(
	subscription *nwdaf_context.Subscription,
) (*ApplySubscriptionRuntimeResponse, error) {
	if subscription == nil {
		return nil, fmt.Errorf("subscription is required")
	}
	return a.ApplySubscriptionRuntime(
		a.BuildSubscriptionRuntimeRequestForSubscription(subscription, nil),
	)
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

func (a *AnlfService) SyncObservationBindings(
	subscriptionID string,
	revision int64,
	bindings []ObservationBinding,
) error {
	if a.anlfBackend == nil {
		return fmt.Errorf("AnLF backend client not initialized")
	}
	if err := a.anlfBackend.SyncObservationBindings(
		a.nwdaf.CancelContext(),
		subscriptionID,
		SyncObservationBindingsRequest{RuntimeRevision: revision, Bindings: bindings},
	); err != nil {
		return err
	}
	if subscription := nwdaf_context.GetSelf().GetSubscription(subscriptionID); subscription != nil {
		sourceIDs := make([]string, 0, len(bindings))
		for _, binding := range bindings {
			sourceIDs = append(sourceIDs, binding.ObservationSourceID)
		}
		_, requirements, _, _ := subscription.RuntimeSnapshot()
		subscription.SetRuntime(revision, requirements, sourceIDs)
	}
	return nil
}

// RecordAnalyticsReport keeps the Phase 4 transitional accuracy correlation in Go.
func (a *AnlfService) RecordAnalyticsReport(
	subscriptionID string,
	report *AnalyticsReport,
) {
	if report == nil || !isAccuracyMonitorEnabled(a.config()) {
		return
	}
	ctx := nwdaf_context.GetSelf()
	mlInfo := ctx.GetMlModelInfo(subscriptionID)
	if mlInfo == nil || mlInfo.GetModelURL() == "" {
		return
	}
	store := ctx.GetModelAccuracyStore(mlInfo.GetModelURL())
	if store == nil {
		return
	}
	scopeKey, _ := resolveMonitoringScope(subscriptionID, ctx)
	for _, event := range report.EventNotifications {
		if event.Event != string(models.NwdafEvent_UE_COMMUNICATION) {
			continue
		}
		for _, communication := range event.UeCommunications {
			store.AddPrediction(nwdaf_context.PredictionRecord{
				ModelUrl:       mlInfo.GetModelURL(),
				PredictedAt:    report.GeneratedAt,
				TargetTime:     communication.Timestamp,
				TargetSlotTime: communication.Timestamp,
				PredUlVol:      communication.TrafficCharacterization.UplinkVolume,
				PredDlVol:      communication.TrafficCharacterization.DownlinkVolume,
				NwdafSubId:     subscriptionID,
				ScopeKey:       scopeKey,
			})
		}
	}
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
