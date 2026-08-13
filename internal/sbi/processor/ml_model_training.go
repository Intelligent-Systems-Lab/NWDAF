package processor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/free5gc/nwdaf/internal/backend"
	wire "github.com/free5gc/nwdaf/internal/compat/mlmodeltraining"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

const mlModelTrainingCallbackPath = "/internal/v1/ml-model-training/notifications"

func (p *Processor) HandleCreateMLModelTraining(
	ctx context.Context,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.handleCreateLocalMLModelTraining(ctx, body, nwdaf_context.MLModelRoutePartyExternal)
}

func (p *Processor) HandleCreateMLModelTrainingFromBackend(
	ctx context.Context,
	body []byte,
	target *backend.SelectedTarget,
) (*backend.StandardResponse, *models.ProblemDetails) {
	if target != nil {
		return p.handleCreateRemoteMLModelTraining(ctx, body, *target)
	}
	return p.handleCreateLocalMLModelTraining(ctx, body, nwdaf_context.MLModelRoutePartyMTLFBackend)
}

func (p *Processor) handleCreateLocalMLModelTraining(
	ctx context.Context,
	body []byte,
	initiator nwdaf_context.MLModelRouteParty,
) (*backend.StandardResponse, *models.ProblemDetails) {
	value, err := wire.ParseNwdafMLModelTrainSubsc(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	if validationErr := wire.ValidateFLSubscription(value, nil); validationErr != nil {
		return nil, mlModelTrainingValidationProblem(validationErr)
	}
	generationLease1, admitted1 := acquireBackend(p.mtlfMLModelBackend, p.mtlfAvailability)
	if !admitted1 {
		return nil, mlModelUnavailableProblem()
	}
	if generationLease1 != nil {
		defer generationLease1.Release()
	}
	backendBody, err := replaceTrainingNotificationURI(
		body, p.mtlfCallbackURI(mlModelTrainingCallbackPath),
	)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	localRouteID := uuid.New().String()
	reserved := trainingRoute(
		localRouteID,
		nil,
		nil,
		value,
		initiator,
		nwdaf_context.MLModelRoutePartyExternal,
	)
	reserved.PeerRoute = nwdaf_context.MLModelPeerRoute{
		Direction:         nwdaf_context.MLModelRouteDirectionInbound,
		LifecycleState:    nwdaf_context.MLModelRouteCreating,
		ProcessGeneration: p.backendGeneration(p.mtlfAvailability),
	}
	p.mlModelMu.Lock()
	reserved.PeerRoute.OperationRevision = p.nextMLModelOperationRevisionLocked()
	revision := reserved.PeerRoute.OperationRevision
	nwdafContext := p.nwdaf.Context()
	added := nwdafContext != nil && nwdafContext.AddMLModelTrainingSubscriptionRoute(reserved)
	p.mlModelMu.Unlock()
	if !added {
		return nil, mlModelInternalProblem(
			"could not reserve ML Model Training route; notifCorreId must be unique",
		)
	}
	response, err := p.mtlfMLModelBackend.CreateMLModelTrainingSubscription(ctx, backendBody)
	if err != nil {
		p.removeCreatingTrainingRoute(localRouteID, revision)
		return nil, p.mlModelBackendProblem(err, p.mtlfAvailability)
	}
	if response == nil || response.StatusCode != http.StatusCreated {
		p.removeCreatingTrainingRoute(localRouteID, revision)
		return nil, mlModelBadGatewayProblem("MTLF backend returned an invalid training create response")
	}
	backendResourceID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		p.removeCreatingTrainingRoute(localRouteID, revision)
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	backendValue, err := wire.ParseNwdafMLModelTrainSubsc(response.Body)
	if err != nil {
		p.cleanupLocalTrainingResource(ctx, backendResourceID)
		p.removeCreatingTrainingRoute(localRouteID, revision)
		return nil, mlModelBadGatewayProblem("MTLF backend returned an invalid training representation")
	}
	backendValue.NotificationURI = value.NotificationURI
	externalBody, err := json.Marshal(backendValue)
	if err != nil {
		p.cleanupLocalTrainingResource(ctx, backendResourceID)
		p.removeCreatingTrainingRoute(localRouteID, revision)
		return nil, mlModelInternalProblem("could not encode ML Model Training representation")
	}
	p.mlModelMu.Lock()
	route, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(localRouteID)
	if !found || !mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		p.mlModelMu.Unlock()
		p.cleanupLocalTrainingResource(ctx, backendResourceID)
		return nil, mlModelUnavailableProblem()
	}
	route.PeerRoute.BackendLocation = response.Location
	route.PeerRoute.BackendResourceID = backendResourceID
	restoreActiveMLModelRoute(&route.PeerRoute)
	route.AcceptedRepresentation = externalBody
	route.BackendRepresentation = backendBody
	if !nwdafContext.UpdateMLModelTrainingSubscriptionRoute(route) {
		p.mlModelMu.Unlock()
		p.cleanupLocalTrainingResource(ctx, backendResourceID)
		return nil, mlModelInternalProblem(
			"could not record ML Model Training route; notifCorreId must be unique",
		)
	}
	p.mlModelMu.Unlock()
	p.mtlfAvailability.Refresh()
	return &backend.StandardResponse{
		StatusCode: http.StatusCreated,
		Location: p.publicResourceLocation(
			factory.NwdafMLModelTrainingResURIPrefix, "subscriptions", localRouteID,
		),
		ContentType: "application/json",
		Body:        externalBody,
	}, nil
}

func (p *Processor) handleCreateRemoteMLModelTraining(
	ctx context.Context,
	body []byte,
	target backend.SelectedTarget,
) (*backend.StandardResponse, *models.ProblemDetails) {
	value, err := wire.ParseNwdafMLModelTrainSubsc(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	if validationErr := wire.ValidateFLSubscription(value, nil); validationErr != nil {
		return nil, mlModelTrainingValidationProblem(validationErr)
	}
	if p.mlModelPeerConsumer == nil {
		return nil, mlModelUnavailableProblem()
	}
	localRouteID := uuid.New().String()
	peerBody, err := replaceTrainingNotificationURI(
		body, p.publicMLModelCallbackURI("ml-model-training", localRouteID),
	)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	reserved := trainingRoute(
		localRouteID, nil, nil, value,
		nwdaf_context.MLModelRoutePartyMTLFBackend,
		nwdaf_context.MLModelRoutePartyMTLFBackend,
	)
	reserved.PeerRoute = nwdaf_context.MLModelPeerRoute{
		Direction:         nwdaf_context.MLModelRouteDirectionOutbound,
		SelectedTarget:    copySelectedTarget(target),
		LifecycleState:    nwdaf_context.MLModelRouteCreating,
		ProcessGeneration: p.backendGeneration(p.mtlfAvailability),
	}
	nwdafContext := p.nwdaf.Context()
	p.mlModelMu.Lock()
	reserved.PeerRoute.OperationRevision = p.nextMLModelOperationRevisionLocked()
	revision := reserved.PeerRoute.OperationRevision
	if nwdafContext == nil || !nwdafContext.AddMLModelTrainingSubscriptionRoute(reserved) {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem(
			"could not reserve remote ML Model Training route; notifCorreId must be unique",
		)
	}
	p.mlModelMu.Unlock()
	response, peerErr := p.mlModelPeerConsumer.CreatePeerMLModelTraining(ctx, target, peerBody)
	if peerErr != nil {
		p.finishFailedPeerTrainingCreate(ctx, localRouteID, revision, response)
		return nil, p.mlModelPeerProblem(peerErr)
	}
	peerLocation, err := resolvedPeerLocation(response)
	if err != nil {
		p.finishFailedPeerTrainingCreate(ctx, localRouteID, revision, response)
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	peerValue, err := wire.ParseNwdafMLModelTrainSubsc(response.Body)
	if err != nil {
		p.finishFailedPeerTrainingCreate(ctx, localRouteID, revision, response)
		return nil, mlModelBadGatewayProblem("peer returned an invalid training representation")
	}
	peerValue.NotificationURI = value.NotificationURI
	backendView, err := json.Marshal(peerValue)
	if err != nil {
		p.finishFailedPeerTrainingCreate(ctx, localRouteID, revision, response)
		return nil, mlModelInternalProblem("could not encode peer ML Model Training representation")
	}
	p.mlModelMu.Lock()
	route, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(localRouteID)
	if !found || !mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		p.mlModelMu.Unlock()
		p.finishFailedPeerTrainingCreate(ctx, localRouteID, revision, response)
		return nil, mlModelUnavailableProblem()
	}
	route.PeerRoute.PeerLocation = peerLocation
	restoreActiveMLModelRoute(&route.PeerRoute)
	route.AcceptedRepresentation = backendView
	route.BackendRepresentation = backendView
	if !nwdafContext.UpdateMLModelTrainingSubscriptionRoute(route) {
		p.mlModelMu.Unlock()
		p.finishFailedPeerTrainingCreate(ctx, localRouteID, revision, response)
		return nil, mlModelInternalProblem("could not record remote ML Model Training route")
	}
	p.mlModelMu.Unlock()
	return &backend.StandardResponse{
		StatusCode: http.StatusCreated,
		Location: p.privateMLModelResourceLocation(
			nwdaf_context.MLModelRoutePartyMTLFBackend,
			"ml-model-training/subscriptions",
			localRouteID,
		),
		ContentType: "application/json",
		Body:        backendView,
	}, nil
}

func (p *Processor) HandleReplaceMLModelTrainingFromBackend(
	ctx context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.HandleReplaceMLModelTraining(ctx, subscriptionID, body)
}

func (p *Processor) HandleReplaceMLModelTraining(
	ctx context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	value, err := wire.ParseNwdafMLModelTrainSubsc(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	p.mlModelMu.Lock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	route, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(subscriptionID)
	if !found {
		p.mlModelMu.Unlock()
		return nil, mlModelResourceNotFoundProblem("ML Model Training subscription", subscriptionID)
	}
	if validationErr := wire.ValidateFLSubscription(value, trainingIdentity(route)); validationErr != nil {
		p.mlModelMu.Unlock()
		return nil, mlModelTrainingValidationProblem(validationErr)
	}
	routedBody, err := replaceTrainingNotificationURI(body, trainingRouteCallbackURI(p, route))
	if err != nil {
		p.mlModelMu.Unlock()
		return nil, malformedMLModelProblem(err)
	}
	revision, problem := p.beginMLModelRouteOperationLocked(
		&route.PeerRoute,
		nwdaf_context.MLModelRouteReplacing,
	)
	if problem != nil {
		p.mlModelMu.Unlock()
		return nil, problem
	}
	if !nwdafContext.UpdateMLModelTrainingSubscriptionRoute(route) {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("could not reserve ML Model Training replacement")
	}
	isPeer := route.PeerRoute.SelectedTarget != nil
	var generationLease *backend.GenerationLease
	if isPeer {
		if p.mlModelPeerConsumer == nil {
			restoreActiveMLModelRoute(&route.PeerRoute)
			nwdafContext.UpdateMLModelTrainingSubscriptionRoute(route)
			p.mlModelMu.Unlock()
			return nil, mlModelUnavailableProblem()
		}
	} else {
		var admitted bool
		generationLease, admitted = acquireBackend(p.mtlfMLModelBackend, p.mtlfAvailability)
		if !admitted {
			restoreActiveMLModelRoute(&route.PeerRoute)
			nwdafContext.UpdateMLModelTrainingSubscriptionRoute(route)
			p.mlModelMu.Unlock()
			return nil, mlModelUnavailableProblem()
		}
	}
	p.mlModelMu.Unlock()
	if generationLease != nil {
		defer generationLease.Release()
	}
	var response *backend.StandardResponse
	if isPeer {
		response, err = p.mlModelPeerConsumer.ReplacePeerMLModelTraining(
			ctx, route.PeerRoute.PeerLocation, routedBody,
		)
	} else {
		response, err = p.mtlfMLModelBackend.ReplaceMLModelTrainingSubscription(
			ctx, route.PeerRoute.BackendResourceID, routedBody,
		)
	}
	if err != nil {
		p.finishTrainingMutationFailure(subscriptionID, revision, false)
		return nil, p.trainingRouteProblem(err, route)
	}
	if response == nil || response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		p.finishTrainingMutationFailure(subscriptionID, revision, false)
		return nil, mlModelBadGatewayProblem("training destination returned an invalid replace response")
	}
	externalBody := append(json.RawMessage(nil), body...)
	backendBody := append(json.RawMessage(nil), routedBody...)
	if response.StatusCode == http.StatusOK {
		responseValue, parseErr := wire.ParseNwdafMLModelTrainSubsc(response.Body)
		if parseErr != nil {
			p.finishTrainingMutationFailure(subscriptionID, revision, false)
			return nil, mlModelBadGatewayProblem("training destination returned an invalid representation")
		}
		responseValue.NotificationURI = value.NotificationURI
		externalBody, err = json.Marshal(responseValue)
		if err != nil {
			p.finishTrainingMutationFailure(subscriptionID, revision, false)
			return nil, mlModelInternalProblem("could not encode ML Model Training representation")
		}
		backendBody = append(json.RawMessage(nil), response.Body...)
	}
	p.mlModelMu.Lock()
	current, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(subscriptionID)
	if !found || !mlModelRouteOperationCurrent(
		current.PeerRoute,
		nwdaf_context.MLModelRouteReplacing,
		revision,
	) {
		p.mlModelMu.Unlock()
		return nil, mlModelUnavailableProblem()
	}
	restoreActiveMLModelRoute(&current.PeerRoute)
	updateTrainingRouteRepresentation(&current, value, externalBody, backendBody)
	if response.PermanentRedirectURI != "" {
		current.PeerRoute.PeerLocation = response.PermanentRedirectURI
	}
	if !nwdafContext.UpdateMLModelTrainingSubscriptionRoute(current) {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("could not update ML Model Training route")
	}
	p.mlModelMu.Unlock()
	if !isPeer {
		p.mtlfAvailability.Refresh()
	}
	return &backend.StandardResponse{
		StatusCode: response.StatusCode, ContentType: response.ContentType,
		Body: externalBodyForStatus(response.StatusCode, externalBody),
	}, nil
}

func (p *Processor) HandlePatchMLModelTrainingFromBackend(
	ctx context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.HandlePatchMLModelTraining(ctx, subscriptionID, body)
}

func (p *Processor) HandlePatchMLModelTraining(
	ctx context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	patch, err := wire.ParseNwdafMLModelTrainSubscPatch(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	p.mlModelMu.Lock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	route, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(subscriptionID)
	if !found {
		p.mlModelMu.Unlock()
		return nil, mlModelResourceNotFoundProblem("ML Model Training subscription", subscriptionID)
	}
	current, err := wire.ParseNwdafMLModelTrainSubsc(route.AcceptedRepresentation)
	if err != nil {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("stored ML Model Training representation is invalid")
	}
	if validationErr := wire.ValidateFLPatch(patch, trainingIdentity(route)); validationErr != nil {
		p.mlModelMu.Unlock()
		return nil, mlModelTrainingValidationProblem(validationErr)
	}
	effective, err := wire.ApplySubscriptionPatch(current, patch)
	if err != nil {
		p.mlModelMu.Unlock()
		return nil, malformedMLModelProblem(err)
	}
	if validationErr := wire.ValidateFLSubscription(
		effective, trainingIdentity(route),
	); validationErr != nil {
		p.mlModelMu.Unlock()
		return nil, mlModelTrainingValidationProblem(validationErr)
	}
	routedPatch := append([]byte(nil), body...)
	if patch.NotificationURI != nil {
		routedPatch, err = replaceTrainingNotificationURI(body, trainingRouteCallbackURI(p, route))
		if err != nil {
			p.mlModelMu.Unlock()
			return nil, malformedMLModelProblem(err)
		}
	}
	revision, problem := p.beginMLModelRouteOperationLocked(
		&route.PeerRoute,
		nwdaf_context.MLModelRouteReplacing,
	)
	if problem != nil {
		p.mlModelMu.Unlock()
		return nil, problem
	}
	if !nwdafContext.UpdateMLModelTrainingSubscriptionRoute(route) {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("could not reserve ML Model Training patch")
	}
	isPeer := route.PeerRoute.SelectedTarget != nil
	var generationLease *backend.GenerationLease
	if isPeer {
		if p.mlModelPeerConsumer == nil {
			restoreActiveMLModelRoute(&route.PeerRoute)
			nwdafContext.UpdateMLModelTrainingSubscriptionRoute(route)
			p.mlModelMu.Unlock()
			return nil, mlModelUnavailableProblem()
		}
	} else {
		var admitted bool
		generationLease, admitted = acquireBackend(p.mtlfMLModelBackend, p.mtlfAvailability)
		if !admitted {
			restoreActiveMLModelRoute(&route.PeerRoute)
			nwdafContext.UpdateMLModelTrainingSubscriptionRoute(route)
			p.mlModelMu.Unlock()
			return nil, mlModelUnavailableProblem()
		}
	}
	p.mlModelMu.Unlock()
	if generationLease != nil {
		defer generationLease.Release()
	}
	var response *backend.StandardResponse
	if isPeer {
		response, err = p.mlModelPeerConsumer.PatchPeerMLModelTraining(
			ctx, route.PeerRoute.PeerLocation, routedPatch,
		)
	} else {
		response, err = p.mtlfMLModelBackend.PatchMLModelTrainingSubscription(
			ctx, route.PeerRoute.BackendResourceID, routedPatch,
		)
	}
	if err != nil {
		p.finishTrainingMutationFailure(subscriptionID, revision, false)
		return nil, p.trainingRouteProblem(err, route)
	}
	if response == nil || response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		p.finishTrainingMutationFailure(subscriptionID, revision, false)
		return nil, mlModelBadGatewayProblem("training destination returned an invalid patch response")
	}
	externalValue := effective
	effectiveBody, err := json.Marshal(externalValue)
	if err != nil {
		p.finishTrainingMutationFailure(subscriptionID, revision, false)
		return nil, mlModelInternalProblem("could not encode patched ML Model Training representation")
	}
	backendEffective := *effective
	backendEffective.NotificationURI = trainingRouteCallbackURI(p, route)
	backendEffectiveBody, err := json.Marshal(&backendEffective)
	if err != nil {
		p.finishTrainingMutationFailure(subscriptionID, revision, false)
		return nil, mlModelInternalProblem("could not encode routed ML Model Training representation")
	}
	if response.StatusCode == http.StatusOK {
		responseValue, parseErr := wire.ParseNwdafMLModelTrainSubsc(response.Body)
		if parseErr != nil {
			p.finishTrainingMutationFailure(subscriptionID, revision, false)
			return nil, mlModelBadGatewayProblem(
				"training destination returned an invalid representation",
			)
		}
		responseValue.NotificationURI = effective.NotificationURI
		externalValue = responseValue
		effectiveBody, err = json.Marshal(responseValue)
		if err != nil {
			p.finishTrainingMutationFailure(subscriptionID, revision, false)
			return nil, mlModelInternalProblem(
				"could not encode ML Model Training representation",
			)
		}
		backendEffectiveBody = append(json.RawMessage(nil), response.Body...)
	}
	p.mlModelMu.Lock()
	currentRoute, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(subscriptionID)
	if !found || !mlModelRouteOperationCurrent(
		currentRoute.PeerRoute,
		nwdaf_context.MLModelRouteReplacing,
		revision,
	) {
		p.mlModelMu.Unlock()
		return nil, mlModelUnavailableProblem()
	}
	restoreActiveMLModelRoute(&currentRoute.PeerRoute)
	updateTrainingRouteRepresentation(
		&currentRoute,
		externalValue,
		effectiveBody,
		backendEffectiveBody,
	)
	if response.PermanentRedirectURI != "" {
		currentRoute.PeerRoute.PeerLocation = response.PermanentRedirectURI
	}
	if !nwdafContext.UpdateMLModelTrainingSubscriptionRoute(currentRoute) {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("could not update ML Model Training route")
	}
	p.mlModelMu.Unlock()
	if !isPeer {
		p.mtlfAvailability.Refresh()
	}
	return &backend.StandardResponse{
		StatusCode: response.StatusCode, ContentType: response.ContentType,
		Body: externalBodyForStatus(response.StatusCode, effectiveBody),
	}, nil
}

func (p *Processor) HandleDeleteMLModelTrainingFromBackend(
	ctx context.Context,
	subscriptionID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.HandleDeleteMLModelTraining(ctx, subscriptionID)
}

func (p *Processor) HandleDeleteMLModelTraining(
	ctx context.Context,
	subscriptionID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	p.mlModelMu.Lock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	if nwdafContext.ConsumeMLModelDeletionRecord(
		nwdaf_context.MLModelResourceTrainingSubscription,
		subscriptionID,
	) {
		p.mlModelMu.Unlock()
		return noContentMLModelResponse(), nil
	}
	route, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(subscriptionID)
	if !found {
		p.mlModelMu.Unlock()
		return nil, mlModelResourceNotFoundProblem("ML Model Training subscription", subscriptionID)
	}
	revision, problem := p.beginMLModelRouteOperationLocked(
		&route.PeerRoute,
		nwdaf_context.MLModelRouteDeleting,
	)
	if problem != nil {
		p.mlModelMu.Unlock()
		return nil, problem
	}
	if !nwdafContext.UpdateMLModelTrainingSubscriptionRoute(route) {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("could not reserve ML Model Training deletion")
	}
	isPeer := route.PeerRoute.SelectedTarget != nil
	var generationLease *backend.GenerationLease
	if isPeer {
		if p.mlModelPeerConsumer == nil {
			restoreActiveMLModelRoute(&route.PeerRoute)
			nwdafContext.UpdateMLModelTrainingSubscriptionRoute(route)
			p.mlModelMu.Unlock()
			return nil, mlModelUnavailableProblem()
		}
	} else {
		var admitted bool
		generationLease, admitted = acquireBackend(p.mtlfMLModelBackend, p.mtlfAvailability)
		if !admitted {
			restoreActiveMLModelRoute(&route.PeerRoute)
			nwdafContext.UpdateMLModelTrainingSubscriptionRoute(route)
			p.mlModelMu.Unlock()
			return nil, mlModelUnavailableProblem()
		}
	}
	p.mlModelMu.Unlock()
	if generationLease != nil {
		defer generationLease.Release()
	}

	var response *backend.StandardResponse
	var operationErr error
	if isPeer {
		response, operationErr = p.mlModelPeerConsumer.DeletePeerMLModelTraining(
			ctx,
			route.PeerRoute.PeerLocation,
		)
	} else {
		response, operationErr = p.mtlfMLModelBackend.DeleteMLModelTrainingSubscription(
			ctx,
			route.PeerRoute.BackendResourceID,
		)
	}
	missingDestination := peerMissing(operationErr)
	terminal := operationErr == nil && response != nil &&
		(response.StatusCode == http.StatusNoContent || response.StatusCode == http.StatusNotFound)
	if missingDestination {
		terminal = true
	}

	p.mlModelMu.Lock()
	current, exists := nwdafContext.GetMLModelTrainingSubscriptionRoute(subscriptionID)
	if !exists || !mlModelRouteOperationCurrent(
		current.PeerRoute,
		nwdaf_context.MLModelRouteDeleting,
		revision,
	) {
		_, resetDeleted := nwdafContext.GetMLModelDeletionRecord(
			nwdaf_context.MLModelResourceTrainingSubscription,
			subscriptionID,
		)
		p.mlModelMu.Unlock()
		if !exists && resetDeleted {
			return noContentMLModelResponse(), nil
		}
		return nil, mlModelUnavailableProblem()
	}
	if terminal {
		nwdafContext.DeleteMLModelTrainingSubscriptionRoute(subscriptionID)
	} else {
		restoreActiveMLModelRoute(&current.PeerRoute)
		nwdafContext.UpdateMLModelTrainingSubscriptionRoute(current)
	}
	p.mlModelMu.Unlock()

	if terminal {
		if !isPeer {
			p.mtlfAvailability.Refresh()
		}
		if missingDestination || response == nil || response.StatusCode == http.StatusNotFound {
			return noContentMLModelResponse(), nil
		}
		return response, nil
	}
	if operationErr != nil {
		return nil, p.trainingRouteProblem(operationErr, route)
	}
	return nil, mlModelBadGatewayProblem("ML Model Training destination returned an invalid delete response")
}

func (p *Processor) HandleMLModelTrainingNotification(
	ctx context.Context,
	localRouteID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	notification, err := wire.ParseNwdafMLModelTrainNotif(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	p.mlModelMu.Lock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	var route nwdaf_context.MLModelTrainingSubscriptionRoute
	var found bool
	if strings.TrimSpace(localRouteID) != "" {
		route, found = nwdafContext.GetMLModelTrainingSubscriptionRoute(localRouteID)
	} else {
		route, found = nwdafContext.FindMLModelTrainingSubscriptionRouteByCorrelation(
			notification.NotificationCorrelationID,
		)
	}
	if !found {
		p.mlModelMu.Unlock()
		return nil, mlModelResourceNotFoundProblem("ML Model Training route", localRouteID)
	}
	if localRouteID != "" {
		if problem := validateOutboundMLModelCallbackRoute(
			route.PeerRoute, p.mtlfAvailability,
		); problem != nil {
			p.mlModelMu.Unlock()
			return nil, problem
		}
	}
	if !mlModelRouteAcceptsCallback(route.PeerRoute) {
		p.mlModelMu.Unlock()
		return nil, mlModelUnavailableProblem()
	}
	if validationErr := wire.ValidateFLNotification(
		notification, trainingIdentity(route),
	); validationErr != nil {
		p.mlModelMu.Unlock()
		return nil, mlModelTrainingValidationProblem(validationErr)
	}
	var generationLease *backend.GenerationLease
	if route.Destination == nwdaf_context.MLModelRoutePartyMTLFBackend {
		var admitted bool
		generationLease, admitted = acquireBackend(p.mtlfMLModelBackend, p.mtlfAvailability)
		if !admitted {
			p.mlModelMu.Unlock()
			return nil, mlModelUnavailableProblem()
		}
	}
	p.mlModelMu.Unlock()
	if generationLease != nil {
		defer generationLease.Release()
	}
	if route.Destination == nwdaf_context.MLModelRoutePartyMTLFBackend {
		response, deliveryErr := p.mtlfMLModelBackend.DeliverMLModelTrainingNotification(ctx, body)
		if deliveryErr != nil {
			return nil, p.mlModelBackendProblem(deliveryErr, p.mtlfAvailability)
		}
		p.mtlfAvailability.Refresh()
		return response, nil
	}
	response, deliveryErr := backend.ExecuteStandardRequest(
		ctx, p.mlModelHTTPClient, 30*time.Second, http.MethodPost,
		route.DestinationNotificationURI, body,
		"deliver external ML Model Training notification",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{http.StatusNoContent: nil},
			ErrorStatuses:     mlModelCallbackErrorStatuses(), FollowRedirects: true,
		},
	)
	if deliveryErr != nil {
		return nil, p.mlModelCallbackProblem(deliveryErr)
	}
	return response, nil
}

func trainingRoute(
	id string,
	backendBody,
	acceptedBody []byte,
	value *wire.NwdafMLModelTrainSubsc,
	initiator,
	destination nwdaf_context.MLModelRouteParty,
) nwdaf_context.MLModelTrainingSubscriptionRoute {
	return nwdaf_context.MLModelTrainingSubscriptionRoute{
		SubscriptionID: id, BackendRepresentation: backendBody,
		AcceptedRepresentation: acceptedBody, Initiator: initiator, Destination: destination,
		DestinationNotificationURI: value.NotificationURI,
		NotificationCorrelationID:  value.NotificationCorrelationID,
		MLCorrelationID:            value.MLCorrelationID,
		ExpectedRoundIndicator:     expectedTrainingRound(value),
	}
}

func updateTrainingRouteRepresentation(
	route *nwdaf_context.MLModelTrainingSubscriptionRoute,
	value *wire.NwdafMLModelTrainSubsc,
	acceptedBody,
	backendBody []byte,
) {
	route.AcceptedRepresentation = append(json.RawMessage(nil), acceptedBody...)
	route.BackendRepresentation = append(json.RawMessage(nil), backendBody...)
	route.DestinationNotificationURI = value.NotificationURI
	route.NotificationCorrelationID = value.NotificationCorrelationID
	route.MLCorrelationID = value.MLCorrelationID
	route.ExpectedRoundIndicator = expectedTrainingRound(value)
}

func expectedTrainingRound(value *wire.NwdafMLModelTrainSubsc) *int64 {
	if value == nil || (value.MLPreparationFlag != nil && *value.MLPreparationFlag) {
		return nil
	}
	return value.RoundIndicator
}

func trainingIdentity(
	route nwdaf_context.MLModelTrainingSubscriptionRoute,
) *wire.TrainingResourceIdentity {
	value, parseErr := wire.ParseNwdafMLModelTrainSubsc(route.AcceptedRepresentation)
	if parseErr != nil {
		value = nil
	}
	var method *string
	if value != nil && value.EventRequest != nil {
		method = value.EventRequest.NotificationMethod
	}
	return &wire.TrainingResourceIdentity{
		SubscriptionID: route.SubscriptionID, MLCorrelationID: route.MLCorrelationID,
		NotificationCorrelationID: route.NotificationCorrelationID,
		ExpectedRoundIndicator:    route.ExpectedRoundIndicator, NotificationMethod: method,
	}
}

func trainingRouteCallbackURI(
	p *Processor,
	route nwdaf_context.MLModelTrainingSubscriptionRoute,
) string {
	if route.PeerRoute.SelectedTarget != nil {
		return p.publicMLModelCallbackURI("ml-model-training", route.SubscriptionID)
	}
	return p.mtlfCallbackURI(mlModelTrainingCallbackPath)
}

func replaceTrainingNotificationURI(body []byte, uri string) ([]byte, error) {
	var value map[string]json.RawMessage
	if err := json.Unmarshal(body, &value); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(uri)
	if err != nil {
		return nil, err
	}
	value["notifUri"] = encoded
	return json.Marshal(value)
}

func mlModelTrainingValidationProblem(err error) *models.ProblemDetails {
	var requirements *wire.RequirementsError
	if errors.As(err, &requirements) {
		invalidParams := make([]models.InvalidParam, 0, len(requirements.Violations))
		for _, violation := range requirements.Violations {
			invalidParams = append(invalidParams, models.InvalidParam{
				Param:  violation.Parameter,
				Reason: violation.Reason,
			})
		}
		return &models.ProblemDetails{
			Status: http.StatusForbidden, Title: http.StatusText(http.StatusForbidden),
			Cause: wire.CauseMLModelTrainingRequirementsNotMet, Detail: err.Error(),
			InvalidParams: invalidParams,
		}
	}
	return malformedMLModelProblem(err)
}

func (p *Processor) removeCreatingTrainingRoute(routeID string, revision uint64) {
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		return
	}
	route, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(routeID)
	if found && mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		nwdafContext.DeleteMLModelTrainingSubscriptionRoute(routeID)
	}
}

func (p *Processor) finishTrainingMutationFailure(
	subscriptionID string,
	revision uint64,
	deleteRoute bool,
) {
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		return
	}
	route, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(subscriptionID)
	if !found || !mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteReplacing,
		revision,
	) {
		return
	}
	if deleteRoute {
		nwdafContext.DeleteMLModelTrainingSubscriptionRoute(subscriptionID)
		return
	}
	restoreActiveMLModelRoute(&route.PeerRoute)
	nwdafContext.UpdateMLModelTrainingSubscriptionRoute(route)
}

func (p *Processor) finishFailedPeerTrainingCreate(
	ctx context.Context,
	routeID string,
	revision uint64,
	response *backend.StandardResponse,
) {
	nwdafContext := p.nwdaf.Context()
	location, locationErr := resolvedPeerLocation(response)
	if locationErr != nil {
		p.removeCreatingTrainingRoute(routeID, revision)
		return
	}
	cleanupResponse, cleanupErr := p.mlModelPeerConsumer.DeletePeerMLModelTraining(ctx, location)
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()
	route, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(routeID)
	if !found || !mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		return
	}
	if cleanupResponseAccepted(cleanupResponse, cleanupErr) {
		nwdafContext.DeleteMLModelTrainingSubscriptionRoute(routeID)
		return
	}
	route.PeerRoute.PeerLocation = location
	p.markPeerRoutePendingCleanupLocked(&route.PeerRoute)
	route.AcceptedRepresentation = nil
	route.BackendRepresentation = nil
	route.DestinationNotificationURI = ""
	if nwdafContext.UpdateMLModelTrainingSubscriptionRoute(route) {
		logPeerCleanupFailure("compensate peer training create", cleanupResponse, cleanupErr)
	}
}

func (p *Processor) cleanupLocalTrainingResource(ctx context.Context, backendResourceID string) {
	if p.mtlfMLModelBackend == nil || backendResourceID == "" {
		return
	}
	if _, err := p.mtlfMLModelBackend.DeleteMLModelTrainingSubscription(
		ctx, backendResourceID,
	); err != nil {
		logger.ProcLog.Errorf(
			"Failed to compensate invalid ML Model Training backend response: "+
				"subscriptionId=%s err=%v",
			backendResourceID,
			err,
		)
	}
}

func (p *Processor) trainingRouteProblem(
	err error,
	route nwdaf_context.MLModelTrainingSubscriptionRoute,
) *models.ProblemDetails {
	if route.PeerRoute.SelectedTarget != nil {
		return p.mlModelPeerProblem(err)
	}
	return p.mlModelBackendProblem(err, p.mtlfAvailability)
}
