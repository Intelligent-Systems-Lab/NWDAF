package coordinator

import (
	"errors"
	"fmt"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
)

const provisionSourceMTLF = "MTLF_PROVISION"

var ErrFutureRuntimeRevision = errors.New("runtime completion revision is newer than current state")

func (a *Coordinator) BuildSubscriptionRuntimeRequest(
	subscriptionID string,
	provisionContext *contract.ProvisionContext,
) (contract.ApplySubscriptionRuntimeRequest, error) {
	ctx := nwdaf_context.GetSelf()
	if ctx == nil {
		return contract.ApplySubscriptionRuntimeRequest{}, fmt.Errorf("NWDAF context not initialized")
	}

	subscription := ctx.GetSubscription(subscriptionID)
	if subscription == nil {
		return contract.ApplySubscriptionRuntimeRequest{}, fmt.Errorf("subscription %s not found", subscriptionID)
	}

	return a.BuildSubscriptionRuntimeRequestForSubscription(subscription, provisionContext), nil
}

func (a *Coordinator) BuildSubscriptionRuntimeRequestForSubscription(
	subscription *nwdaf_context.Subscription,
	provisionContext *contract.ProvisionContext,
) contract.ApplySubscriptionRuntimeRequest {
	return contract.ApplySubscriptionRuntimeRequest{
		Subscription: contract.SubscriptionRuntimeContext{
			SubscriptionID:     subscription.ID,
			NotifCorrID:        subscription.NotifCorrId,
			EvtReq:             subscription.EvtReq,
			EventSubscriptions: subscription.EventSubs,
		},
		ProvisionContext:             provisionContext,
		ReportCallbackURI:            a.BuildAnalyticsReportCallbackURI(subscription.ID),
		RuntimeCompletionCallbackURI: a.BuildRuntimeCompletionCallbackURI(subscription.ID),
	}
}

func (a *Coordinator) CompleteSubscriptionRuntime(event *contract.RuntimeCompletionEvent) error {
	ctx := nwdaf_context.GetSelf()
	if ctx == nil {
		return fmt.Errorf("NWDAF context not initialized")
	}
	if event == nil {
		return fmt.Errorf("runtime completion event is required")
	}

	subscription := ctx.GetSubscription(event.SubscriptionID)
	if subscription == nil {
		logger.AnlfLog.Debugf(
			"Runtime completion consumed for missing subscription: sub=%s revision=%d",
			event.SubscriptionID,
			event.RuntimeRevision,
		)
		return nil
	}

	switch subscription.CompleteRuntime(event.RuntimeRevision) {
	case nwdaf_context.RuntimeCompletionCompleted:
		logger.AnlfLog.Infof(
			"AnLF runtime completed: sub=%s revision=%d reason=%s sequence=%d",
			event.SubscriptionID,
			event.RuntimeRevision,
			event.Reason,
			event.LastReportSequence,
		)
	case nwdaf_context.RuntimeCompletionAlreadyInactive:
		logger.AnlfLog.Debugf(
			"Duplicate runtime completion consumed: sub=%s revision=%d",
			event.SubscriptionID,
			event.RuntimeRevision,
		)
	case nwdaf_context.RuntimeCompletionStale:
		logger.AnlfLog.Debugf(
			"Stale runtime completion consumed: sub=%s revision=%d",
			event.SubscriptionID,
			event.RuntimeRevision,
		)
	case nwdaf_context.RuntimeCompletionFuture:
		return fmt.Errorf(
			"%w: sub=%s completion=%d",
			ErrFutureRuntimeRevision,
			event.SubscriptionID,
			event.RuntimeRevision,
		)
	}
	return nil
}

func (a *Coordinator) ApplySubscriptionRuntime(
	request contract.ApplySubscriptionRuntimeRequest,
) (*contract.ApplySubscriptionRuntimeResponse, error) {
	if !a.backendUsable() {
		return nil, ErrBackendUnavailable
	}

	response, err := a.backend.ApplySubscriptionRuntime(a.nwdaf.CancelContext(), request)
	if err != nil {
		a.reportBackendFailure(err)
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
	case contract.ApplyResultPendingProvision:
		logger.AnlfLog.Debugf("AnLF runtime pending provision: sub=%s", subscriptionID)
		return response, nil
	case contract.ApplyResultActivated, contract.ApplyResultReused, contract.ApplyResultReplaced:
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
	case contract.ApplyResultFailedUsingPrevious:
		return response, fmt.Errorf("runtime replacement failed for subscription %s: %s", subscriptionID, response.Message)
	case contract.ApplyResultFailedNoPrevious:
		if mlInfo := nwdaf_context.GetSelf().GetMlModelInfo(subscriptionID); mlInfo != nil {
			mlInfo.SetModelFailed(errors.New(response.Message))
		}
		return response, fmt.Errorf("runtime activation failed for subscription %s: %s", subscriptionID, response.Message)
	default:
		return response, fmt.Errorf("AnLF backend returned unknown apply result %q", response.Result)
	}
}

func (a *Coordinator) BuildCurrentObservationBindings(subscriptionID string) []contract.ObservationBinding {
	ctx := nwdaf_context.GetSelf()
	subscription := ctx.GetSubscription(subscriptionID)
	if subscription == nil {
		return nil
	}
	requirements := subscription.CollectionRequirementsSnapshot()
	bindings := make([]contract.ObservationBinding, 0)
	for _, resource := range ctx.GetNwdafSubResources(subscriptionID) {
		bindings = append(bindings, contract.ObservationBinding{
			ObservationSourceID: resource.CorrelationId,
			Source:              contract.ObservationSource{SourceType: "SMF_UPF", Supi: resource.Supi},
			SubscriptionScope:   contract.SubscriptionScope{OriginalGroupID: resource.OriginalGroupId},
			CollectionProfile: contract.CollectionRequirements{
				SamplingIntervalSeconds: requirements.SamplingIntervalSeconds,
				RequiredMeasurements:    append([]string(nil), requirements.RequiredMeasurements...),
			},
		})
	}
	return bindings
}

func (a *Coordinator) SyncCurrentObservationBindings(subscriptionID string) error {
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

func (a *Coordinator) ApplyInitialSubscriptionRuntime(
	subscription *nwdaf_context.Subscription,
) (*contract.ApplySubscriptionRuntimeResponse, error) {
	if subscription == nil {
		return nil, fmt.Errorf("subscription is required")
	}
	return a.ApplySubscriptionRuntime(
		a.BuildSubscriptionRuntimeRequestForSubscription(subscription, nil),
	)
}

func (a *Coordinator) ApplySubscriptionRegistration(subscriptionID string) error {
	request, err := a.BuildSubscriptionRuntimeRequest(subscriptionID, nil)
	if err != nil {
		return err
	}
	_, err = a.ApplySubscriptionRuntime(request)
	return err
}

func (a *Coordinator) ReleaseSubscriptionRuntime(subscriptionID string) error {
	defer a.removeModelReferenceCorrelation(subscriptionID)

	if !a.backendUsable() {
		return nil
	}
	if err := a.backend.ReleaseSubscriptionRuntime(a.nwdaf.CancelContext(), subscriptionID); err != nil {
		a.reportBackendFailure(err)
		return err
	}
	return nil
}

func (a *Coordinator) SyncObservationBindings(
	subscriptionID string,
	revision int64,
	bindings []contract.ObservationBinding,
) error {
	if !a.backendUsable() {
		return ErrBackendUnavailable
	}
	if err := a.backend.SyncObservationBindings(
		a.nwdaf.CancelContext(),
		subscriptionID,
		contract.SyncObservationBindingsRequest{RuntimeRevision: revision, Bindings: bindings},
	); err != nil {
		a.reportBackendFailure(err)
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

func (a *Coordinator) SyncModelProvisionBinding(
	subscriptionID string,
	binding contract.ModelProvisionBinding,
) error {
	client, ok := a.backend.(ModelProvisionClient)
	if !ok || !a.backendUsable() {
		return ErrBackendUnavailable
	}
	err := client.SyncModelProvisionBinding(a.nwdaf.CancelContext(), subscriptionID, binding)
	a.reportBackendFailure(err)
	return err
}

func (a *Coordinator) ApplyModelProvisionEvent(
	event contract.ModelProvisionEvent,
) (*contract.ModelProvisionEventResponse, error) {
	client, ok := a.backend.(ModelProvisionClient)
	if !ok || !a.backendUsable() {
		return nil, ErrBackendUnavailable
	}
	response, err := client.ApplyModelProvisionEvent(a.nwdaf.CancelContext(), event)
	a.reportBackendFailure(err)
	return response, err
}

func (a *Coordinator) applyModelReferenceCorrelation(subscriptionID, modelReference string) {
	ctx := nwdaf_context.GetSelf()
	mlInfo := ctx.GetMlModelInfo(subscriptionID)
	if mlInfo == nil {
		logger.AnlfLog.Warnf("No ML model info found for subscription %s", subscriptionID)
		return
	}

	mlInfo.SetModelUrl(modelReference)
	mlInfo.SetModelReady()
}

func (a *Coordinator) removeModelReferenceCorrelation(subscriptionID string) {
	ctx := nwdaf_context.GetSelf()
	if ctx == nil {
		return
	}
	ctx.DeleteMlModelInfo(subscriptionID)
}
