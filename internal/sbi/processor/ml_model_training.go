package processor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
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

const (
	mlModelTrainingCallbackPath            = "/internal/v1/ml-model-training/notifications"
	mlModelTrainingTerminationCleanupGrace = 30 * time.Second
)

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
		return nil, mlModelTrainingValidationProblem(err)
	}
	if validationErr := wire.ValidateFLSubscription(value, nil); validationErr != nil {
		return nil, mlModelTrainingValidationProblem(validationErr)
	}
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	if validationErr := wire.ValidateCandidateSubscriptionReceiver(
		value, nwdafContext.NfId,
	); validationErr != nil {
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
	reserved.OwnerNFInstanceID = nwdafContext.NfId
	reserved.BoundParticipantNFInstanceID = nwdafContext.NfId
	localKey := reserved.ResourceKey()
	p.mlModelMu.Lock()
	reserved.PeerRoute.OperationRevision = p.nextMLModelOperationRevisionLocked()
	revision := reserved.PeerRoute.OperationRevision
	added := nwdafContext.AddMLModelTrainingSubscriptionRoute(reserved)
	p.mlModelMu.Unlock()
	if !added {
		return nil, mlModelInternalProblem(
			"could not reserve ML Model Training route; notifCorreId must be unique",
		)
	}
	response, err := p.mtlfMLModelBackend.CreateMLModelTrainingSubscription(ctx, backendBody, localRouteID)
	if err != nil {
		p.removeCreatingTrainingRoute(localKey, revision)
		return nil, p.mlModelBackendProblem(err, p.mtlfAvailability)
	}
	if response == nil || response.StatusCode != http.StatusCreated {
		p.removeCreatingTrainingRoute(localKey, revision)
		return nil, mlModelBadGatewayProblem("MTLF backend returned an invalid training create response")
	}
	backendResourceID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil || backendResourceID != localRouteID ||
		!strings.Contains(response.Location, "/internal/v1/ml-model-training/subscriptions/") {
		p.cleanupLocalTrainingResource(ctx, localRouteID)
		p.removeCreatingTrainingRoute(localKey, revision)
		return nil, mlModelBadGatewayProblem("MTLF backend returned a mismatched training Location")
	}
	backendValue, err := wire.ParseNwdafMLModelTrainSubsc(response.Body)
	if err != nil {
		p.cleanupLocalTrainingResource(ctx, backendResourceID)
		p.removeCreatingTrainingRoute(localKey, revision)
		return nil, mlModelBadGatewayProblem("MTLF backend returned an invalid training representation")
	}
	if validationErr := validateTrainingCreateResponse(
		backendValue, value, nwdafContext.NfId,
	); validationErr != nil {
		p.cleanupLocalTrainingResource(ctx, backendResourceID)
		p.removeCreatingTrainingRoute(localKey, revision)
		return nil, mlModelBadGatewayProblem(validationErr.Error())
	}
	backendAcceptedBody, err := json.Marshal(backendValue)
	if err != nil {
		p.cleanupLocalTrainingResource(ctx, backendResourceID)
		p.removeCreatingTrainingRoute(localKey, revision)
		return nil, mlModelInternalProblem("could not encode ML Model Training representation")
	}
	externalValue := *backendValue
	externalValue.NotificationURI = value.NotificationURI
	externalBody, err := json.Marshal(&externalValue)
	if err != nil {
		p.cleanupLocalTrainingResource(ctx, backendResourceID)
		p.removeCreatingTrainingRoute(localKey, revision)
		return nil, mlModelInternalProblem("could not encode ML Model Training representation")
	}
	p.mlModelMu.Lock()
	route, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(localKey)
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
	restoreActiveMLModelRoute(&route.PeerRoute)
	route.AcceptedRepresentation = externalBody
	route.BackendRepresentation = backendAcceptedBody
	setTrainingRouteFeatureState(&route, value.SupportedFeatures, backendValue.SupportedFeatures)
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
		return nil, mlModelTrainingValidationProblem(err)
	}
	if validationErr := wire.ValidateFLSubscription(value, nil); validationErr != nil {
		return nil, mlModelTrainingValidationProblem(validationErr)
	}
	if validationErr := wire.ValidateCandidateSubscriptionReceiver(
		value, target.NFInstanceID,
	); validationErr != nil {
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
		"", nil, nil, value,
		nwdaf_context.MLModelRoutePartyMTLFBackend,
		nwdaf_context.MLModelRoutePartyMTLFBackend,
	)
	reserved.PeerRoute = nwdaf_context.MLModelPeerRoute{
		Direction:         nwdaf_context.MLModelRouteDirectionOutbound,
		SelectedTarget:    copySelectedTarget(target),
		LifecycleState:    nwdaf_context.MLModelRouteCreating,
		ProcessGeneration: p.backendGeneration(p.mtlfAvailability),
	}
	reserved.CallbackRouteID = localRouteID
	reserved.OwnerNFInstanceID = target.NFInstanceID
	reserved.BoundParticipantNFInstanceID = target.NFInstanceID
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
	peerResourceID, err := trainingResourceIDFromPeerLocation(peerLocation)
	if err != nil {
		p.finishFailedPeerTrainingCreate(ctx, localRouteID, revision, response)
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	peerValue, err := wire.ParseNwdafMLModelTrainSubsc(response.Body)
	if err != nil {
		p.finishFailedPeerTrainingCreate(ctx, localRouteID, revision, response)
		return nil, mlModelBadGatewayProblem("peer returned an invalid training representation")
	}
	if validationErr := validateTrainingCreateResponse(peerValue, value, target.NFInstanceID); validationErr != nil {
		p.finishFailedPeerTrainingCreate(ctx, localRouteID, revision, response)
		return nil, mlModelBadGatewayProblem(validationErr.Error())
	}
	peerAcceptedBody, err := json.Marshal(peerValue)
	if err != nil {
		p.finishFailedPeerTrainingCreate(ctx, localRouteID, revision, response)
		return nil, mlModelInternalProblem("could not encode peer ML Model Training representation")
	}
	backendValue := *peerValue
	backendValue.NotificationURI = value.NotificationURI
	backendView, err := json.Marshal(&backendValue)
	if err != nil {
		p.finishFailedPeerTrainingCreate(ctx, localRouteID, revision, response)
		return nil, mlModelInternalProblem("could not encode peer ML Model Training representation")
	}
	p.mlModelMu.Lock()
	route, found := nwdafContext.GetPendingMLModelTrainingRoute(localRouteID)
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
	route.SubscriptionID = peerResourceID
	restoreActiveMLModelRoute(&route.PeerRoute)
	route.AcceptedRepresentation = backendView
	route.BackendRepresentation = peerAcceptedBody
	setTrainingRouteFeatureState(&route, value.SupportedFeatures, peerValue.SupportedFeatures)
	if !nwdafContext.ActivatePendingMLModelTrainingRoute(localRouteID, route) {
		p.mlModelMu.Unlock()
		p.finishFailedPeerTrainingCreate(ctx, localRouteID, revision, response)
		return nil, mlModelInternalProblem("could not record remote ML Model Training route")
	}
	p.mlModelMu.Unlock()
	return &backend.StandardResponse{
		StatusCode: http.StatusCreated,
		Location: p.mtlfCallbackURI("/internal/v1/ml-model-training/targets/" +
			url.PathEscape(target.NFInstanceID) + "/subscriptions/" + url.PathEscape(peerResourceID)),
		ContentType: "application/json",
		Body:        backendView,
	}, nil
}

func (p *Processor) HandleReplaceMLModelTrainingFromBackend(
	ctx context.Context,
	targetNFInstanceID string,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.handleReplaceMLModelTraining(ctx, p.trainingResourceKey(targetNFInstanceID, subscriptionID), body)
}

func (p *Processor) HandleReplaceMLModelTraining(
	ctx context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.handleReplaceMLModelTraining(ctx, p.trainingResourceKey("", subscriptionID), body)
}

func (p *Processor) handleReplaceMLModelTraining(
	ctx context.Context,
	key nwdaf_context.MLModelTrainingResourceKey,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	subscriptionID := key.SubscriptionID
	value, err := wire.ParseNwdafMLModelTrainSubsc(body)
	if err != nil {
		return nil, mlModelTrainingValidationProblem(err)
	}
	p.mlModelMu.Lock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	route, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(key)
	if !found {
		p.mlModelMu.Unlock()
		return nil, mlModelResourceNotFoundProblem("ML Model Training subscription", subscriptionID)
	}
	if problem := candidateTrainingOperationProblem(
		route, wire.HasCandidateSubscriptionFields(value),
	); problem != nil {
		p.mlModelMu.Unlock()
		return nil, problem
	}
	if validationErr := wire.ValidateCandidateSubscriptionReceiver(
		value, route.BoundParticipantNFInstanceID,
	); validationErr != nil {
		p.mlModelMu.Unlock()
		return nil, mlModelTrainingValidationProblem(validationErr)
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
			ctx, route.SubscriptionID, routedBody,
		)
	}
	if err != nil {
		p.finishTrainingMutationFailure(key, revision, false)
		return nil, p.trainingRouteProblem(err, route)
	}
	if response == nil || response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		p.finishTrainingMutationFailure(key, revision, false)
		return nil, mlModelBadGatewayProblem("training destination returned an invalid replace response")
	}
	wire.StripCandidateOperations(value)
	value.SupportedFeatures = route.NegotiatedSupportedFeatures
	externalValue := value
	externalBody, err := json.Marshal(externalValue)
	if err != nil {
		p.finishTrainingMutationFailure(key, revision, false)
		return nil, mlModelInternalProblem("could not encode ML Model Training representation")
	}
	backendValue := *value
	backendValue.NotificationURI = trainingRouteCallbackURI(p, route)
	backendBody, err := json.Marshal(&backendValue)
	if err != nil {
		p.finishTrainingMutationFailure(key, revision, false)
		return nil, mlModelInternalProblem("could not encode routed ML Model Training representation")
	}
	if response.StatusCode == http.StatusOK {
		responseValue, parseErr := wire.ParseNwdafMLModelTrainSubsc(response.Body)
		if parseErr != nil {
			p.finishTrainingMutationFailure(key, revision, false)
			return nil, mlModelBadGatewayProblem("training destination returned an invalid representation")
		}
		if validationErr := validateTrainingMutationResponse(responseValue, value, route); validationErr != nil {
			p.finishTrainingMutationFailure(key, revision, false)
			return nil, mlModelBadGatewayProblem(validationErr.Error())
		}
		responseValue.NotificationURI = value.NotificationURI
		externalValue = responseValue
		externalBody, err = json.Marshal(responseValue)
		if err != nil {
			p.finishTrainingMutationFailure(key, revision, false)
			return nil, mlModelInternalProblem("could not encode ML Model Training representation")
		}
		backendBody = append(json.RawMessage(nil), response.Body...)
	}
	p.mlModelMu.Lock()
	current, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(key)
	if !found || !mlModelRouteOperationCurrent(
		current.PeerRoute,
		nwdaf_context.MLModelRouteReplacing,
		revision,
	) {
		p.mlModelMu.Unlock()
		return nil, mlModelUnavailableProblem()
	}
	restoreActiveMLModelRoute(&current.PeerRoute)
	updateTrainingRouteRepresentation(&current, externalValue, externalBody, backendBody)
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
	targetNFInstanceID string,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.handlePatchMLModelTraining(ctx, p.trainingResourceKey(targetNFInstanceID, subscriptionID), body)
}

func (p *Processor) HandlePatchMLModelTraining(
	ctx context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.handlePatchMLModelTraining(ctx, p.trainingResourceKey("", subscriptionID), body)
}

func (p *Processor) handlePatchMLModelTraining(
	ctx context.Context,
	key nwdaf_context.MLModelTrainingResourceKey,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	subscriptionID := key.SubscriptionID
	patch, err := wire.ParseNwdafMLModelTrainSubscPatch(body)
	if err != nil {
		return nil, mlModelTrainingValidationProblem(err)
	}
	p.mlModelMu.Lock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	route, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(key)
	if !found {
		p.mlModelMu.Unlock()
		return nil, mlModelResourceNotFoundProblem("ML Model Training subscription", subscriptionID)
	}
	if problem := candidateTrainingOperationProblem(route, wire.HasCandidatePatchFields(patch)); problem != nil {
		p.mlModelMu.Unlock()
		return nil, problem
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
		return nil, mlModelTrainingValidationProblem(err)
	}
	if validationErr := wire.ValidateFLSubscription(
		effective, trainingIdentity(route),
	); validationErr != nil {
		p.mlModelMu.Unlock()
		return nil, mlModelTrainingValidationProblem(validationErr)
	}
	if validationErr := wire.ValidateCandidateSubscriptionReceiver(
		effective, route.BoundParticipantNFInstanceID,
	); validationErr != nil {
		p.mlModelMu.Unlock()
		return nil, mlModelTrainingValidationProblem(validationErr)
	}
	wire.StripCandidateOperations(effective)
	effective.SupportedFeatures = route.NegotiatedSupportedFeatures
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
			ctx, route.SubscriptionID, routedPatch,
		)
	}
	if err != nil {
		p.finishTrainingMutationFailure(key, revision, false)
		return nil, p.trainingRouteProblem(err, route)
	}
	if response == nil || response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		p.finishTrainingMutationFailure(key, revision, false)
		return nil, mlModelBadGatewayProblem("training destination returned an invalid patch response")
	}
	externalValue := effective
	effectiveBody, err := json.Marshal(externalValue)
	if err != nil {
		p.finishTrainingMutationFailure(key, revision, false)
		return nil, mlModelInternalProblem("could not encode patched ML Model Training representation")
	}
	backendEffective := *effective
	backendEffective.NotificationURI = trainingRouteCallbackURI(p, route)
	backendEffectiveBody, err := json.Marshal(&backendEffective)
	if err != nil {
		p.finishTrainingMutationFailure(key, revision, false)
		return nil, mlModelInternalProblem("could not encode routed ML Model Training representation")
	}
	if response.StatusCode == http.StatusOK {
		responseValue, parseErr := wire.ParseNwdafMLModelTrainSubsc(response.Body)
		if parseErr != nil {
			p.finishTrainingMutationFailure(key, revision, false)
			return nil, mlModelBadGatewayProblem(
				"training destination returned an invalid representation",
			)
		}
		if validationErr := validateTrainingMutationResponse(responseValue, effective, route); validationErr != nil {
			p.finishTrainingMutationFailure(key, revision, false)
			return nil, mlModelBadGatewayProblem(validationErr.Error())
		}
		responseValue.NotificationURI = effective.NotificationURI
		externalValue = responseValue
		effectiveBody, err = json.Marshal(responseValue)
		if err != nil {
			p.finishTrainingMutationFailure(key, revision, false)
			return nil, mlModelInternalProblem(
				"could not encode ML Model Training representation",
			)
		}
		backendEffectiveBody = append(json.RawMessage(nil), response.Body...)
	}
	p.mlModelMu.Lock()
	currentRoute, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(key)
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
	targetNFInstanceID string,
	subscriptionID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.handleDeleteMLModelTraining(ctx, p.trainingResourceKey(targetNFInstanceID, subscriptionID))
}

func (p *Processor) HandleDeleteMLModelTraining(
	ctx context.Context,
	subscriptionID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.handleDeleteMLModelTraining(ctx, p.trainingResourceKey("", subscriptionID))
}

func (p *Processor) handleDeleteMLModelTraining(
	ctx context.Context,
	key nwdaf_context.MLModelTrainingResourceKey,
) (*backend.StandardResponse, *models.ProblemDetails) {
	subscriptionID := key.SubscriptionID
	p.mlModelMu.Lock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	if key.Direction == nwdaf_context.MLModelRouteDirectionInbound &&
		nwdafContext.ConsumeMLModelDeletionRecord(
			nwdaf_context.MLModelResourceTrainingSubscription,
			subscriptionID,
		) {
		p.mlModelMu.Unlock()
		return noContentMLModelResponse(), nil
	}
	route, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(key)
	if !found {
		p.mlModelMu.Unlock()
		return nil, mlModelResourceNotFoundProblem("ML Model Training subscription", subscriptionID)
	}
	previousLifecycle := route.PeerRoute.LifecycleState
	revision, problem := p.beginMLModelRouteOperationFromLocked(
		&route.PeerRoute,
		nwdaf_context.MLModelRouteDeleting,
		nwdaf_context.MLModelRouteActive,
		nwdaf_context.MLModelRouteTerminating,
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
			route.PeerRoute.LifecycleState = previousLifecycle
			nwdafContext.UpdateMLModelTrainingSubscriptionRoute(route)
			p.mlModelMu.Unlock()
			return nil, mlModelUnavailableProblem()
		}
	} else {
		var admitted bool
		generationLease, admitted = acquireBackend(p.mtlfMLModelBackend, p.mtlfAvailability)
		if !admitted {
			route.PeerRoute.LifecycleState = previousLifecycle
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
			route.SubscriptionID,
		)
	}
	missingDestination := peerMissing(operationErr)
	terminal := operationErr == nil && response != nil &&
		(response.StatusCode == http.StatusNoContent || response.StatusCode == http.StatusNotFound)
	if missingDestination {
		terminal = true
	}

	p.mlModelMu.Lock()
	current, exists := nwdafContext.GetMLModelTrainingSubscriptionRoute(key)
	if !exists || !mlModelRouteOperationCurrent(
		current.PeerRoute,
		nwdaf_context.MLModelRouteDeleting,
		revision,
	) {
		_, resetDeleted := nwdafContext.GetMLModelDeletionRecord(
			nwdaf_context.MLModelResourceTrainingSubscription,
			subscriptionID,
		)
		resetDeleted = resetDeleted && key.Direction == nwdaf_context.MLModelRouteDirectionInbound
		p.mlModelMu.Unlock()
		if !exists && resetDeleted {
			return noContentMLModelResponse(), nil
		}
		return nil, mlModelUnavailableProblem()
	}
	if terminal {
		nwdafContext.DeleteMLModelTrainingSubscriptionRoute(key)
	} else {
		current.PeerRoute.LifecycleState = previousLifecycle
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
		return nil, mlModelTrainingValidationProblem(err)
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
		route, found = nwdafContext.FindMLModelTrainingSubscriptionRouteByCallback(localRouteID)
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
	if problem := candidateTrainingOperationProblem(
		route, wire.HasCandidateNotificationFields(notification),
	); problem != nil {
		p.mlModelMu.Unlock()
		return nil, problem
	}
	if validationErr := wire.ValidateFLNotification(
		notification, trainingIdentity(route),
	); validationErr != nil {
		p.mlModelMu.Unlock()
		return nil, mlModelTrainingValidationProblem(validationErr)
	}
	terminalInboundRoute := strings.TrimSpace(localRouteID) == "" &&
		notification.TerminationRequest != "" &&
		route.PeerRoute.Direction == nwdaf_context.MLModelRouteDirectionInbound &&
		route.Destination == nwdaf_context.MLModelRoutePartyExternal
	var terminalRevision uint64
	if terminalInboundRoute {
		terminalRevision = p.nextMLModelOperationRevisionLocked()
		route.PeerRoute.OperationRevision = terminalRevision
		route.PeerRoute.LifecycleState = nwdaf_context.MLModelRouteTerminating
		route.PeerRoute.CleanupAttempts = 0
		route.PeerRoute.NextCleanupAt = time.Now().Add(
			mlModelTrainingTerminationCleanupGrace,
		)
		if !nwdafContext.UpdateMLModelTrainingSubscriptionRoute(route) {
			p.mlModelMu.Unlock()
			return nil, mlModelInternalProblem(
				"could not mark ML Model Training route as terminating",
			)
		}
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
		if terminalInboundRoute {
			p.finishFailedInboundTrainingTermination(route, terminalRevision)
		}
		return nil, p.mlModelCallbackProblem(deliveryErr)
	}
	return response, nil
}

func (p *Processor) finishFailedInboundTrainingTermination(
	route nwdaf_context.MLModelTrainingSubscriptionRoute,
	revision uint64,
) {
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		return
	}
	current, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(route.ResourceKey())
	if !found || !mlModelRouteOperationCurrent(
		current.PeerRoute,
		nwdaf_context.MLModelRouteTerminating,
		revision,
	) {
		return
	}
	nwdafContext.DeleteMLModelTrainingSubscriptionRoute(route.ResourceKey())
	nwdafContext.TombstoneMLModelResource(
		nwdaf_context.MLModelDeletionRecord{
			ResourceID:        route.SubscriptionID,
			ProcessGeneration: route.PeerRoute.ProcessGeneration,
			CleanupAttempted:  false,
		},
		nwdaf_context.MLModelResourceTrainingSubscription,
	)
	logger.ProcLog.Infof(
		"Retired terminating inbound ML Model Training route after callback delivery failure subscription_id=%s",
		route.SubscriptionID,
	)
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
		OfferedSupportedFeatures:   value.SupportedFeatures,
	}
}

func (p *Processor) trainingResourceKey(
	targetNFInstanceID, subscriptionID string,
) nwdaf_context.MLModelTrainingResourceKey {
	if targetNFInstanceID != "" {
		return nwdaf_context.MLModelTrainingResourceKey{
			Direction:         nwdaf_context.MLModelRouteDirectionOutbound,
			OwnerNFInstanceID: targetNFInstanceID, SubscriptionID: subscriptionID,
		}
	}
	localNFInstanceID := ""
	if p.nwdaf != nil && p.nwdaf.Context() != nil {
		localNFInstanceID = p.nwdaf.Context().NfId
	}
	return nwdaf_context.MLModelTrainingResourceKey{
		Direction:         nwdaf_context.MLModelRouteDirectionInbound,
		OwnerNFInstanceID: localNFInstanceID, SubscriptionID: subscriptionID,
	}
}

func trainingResourceIDFromPeerLocation(location string) (string, error) {
	parsed, err := url.Parse(location)
	if err != nil {
		return "", errors.New("peer training Location is invalid")
	}
	const marker = "/nnwdaf-mlmodeltraining/v1/subscriptions/"
	escaped := parsed.EscapedPath()
	index := strings.LastIndex(escaped, marker)
	if index < 0 || index+len(marker) >= len(escaped) || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("peer training Location is not a subscription resource")
	}
	id, err := url.PathUnescape(escaped[index+len(marker):])
	if err != nil || id == "" || id == "." || id == ".." || strings.ContainsAny(id, "/\\") {
		return "", errors.New("peer training Location has an invalid subscription ID")
	}
	return id, nil
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
		BoundParticipantNFInstanceID: route.BoundParticipantNFInstanceID,
	}
}

func validateTrainingCreateResponse(
	response *wire.NwdafMLModelTrainSubsc,
	request *wire.NwdafMLModelTrainSubsc,
	expectedReceiverNFInstanceID string,
) error {
	if response == nil || request == nil {
		return errors.New("training create response is missing its representation")
	}
	if wire.ContainsCandidateOperations(response) {
		return errors.New("training create response contains a write-only candidate instruction")
	}
	identity := trainingIdentityFromValue(request)
	identity.BoundParticipantNFInstanceID = expectedReceiverNFInstanceID
	if err := wire.ValidateFLSubscription(response, identity); err != nil {
		return err
	}
	if err := wire.ValidateCandidateSubscriptionReceiver(
		response, expectedReceiverNFInstanceID,
	); err != nil {
		return err
	}
	if !wire.SupportedFeaturesAreSubset(request.SupportedFeatures, response.SupportedFeatures) {
		return errors.New("training create response negotiated unsupported features")
	}
	if wire.HasCandidateNotificationFields(response.ImmediateReport) {
		if err := wire.ValidateFLNotification(response.ImmediateReport, identity); err != nil {
			return err
		}
	}
	return nil
}

func validateTrainingMutationResponse(
	response *wire.NwdafMLModelTrainSubsc,
	expected *wire.NwdafMLModelTrainSubsc,
	route nwdaf_context.MLModelTrainingSubscriptionRoute,
) error {
	if response == nil || expected == nil {
		return errors.New("training mutation response is missing its representation")
	}
	if wire.ContainsCandidateOperations(response) {
		return errors.New("training mutation response contains a write-only candidate instruction")
	}
	identity := trainingIdentityFromValue(expected)
	identity.BoundParticipantNFInstanceID = route.BoundParticipantNFInstanceID
	if err := wire.ValidateFLSubscription(response, identity); err != nil {
		return err
	}
	if err := wire.ValidateCandidateSubscriptionReceiver(
		response, route.BoundParticipantNFInstanceID,
	); err != nil {
		return err
	}
	if !wire.SupportedFeaturesAreSubset(
		route.OfferedSupportedFeatures, response.SupportedFeatures,
	) || !wire.SupportedFeaturesEqual(
		route.NegotiatedSupportedFeatures, response.SupportedFeatures,
	) {
		return errors.New("training mutation response changed negotiated features")
	}
	if wire.HasCandidateNotificationFields(response.ImmediateReport) {
		if err := wire.ValidateFLNotification(response.ImmediateReport, identity); err != nil {
			return err
		}
	}
	return nil
}

func trainingIdentityFromValue(value *wire.NwdafMLModelTrainSubsc) *wire.TrainingResourceIdentity {
	if value == nil {
		return nil
	}
	var method *string
	if value.EventRequest != nil {
		method = value.EventRequest.NotificationMethod
	}
	return &wire.TrainingResourceIdentity{
		MLCorrelationID:           value.MLCorrelationID,
		NotificationCorrelationID: value.NotificationCorrelationID,
		ExpectedRoundIndicator:    expectedTrainingRound(value),
		NotificationMethod:        method,
	}
}

func setTrainingRouteFeatureState(
	route *nwdaf_context.MLModelTrainingSubscriptionRoute,
	offered string,
	negotiated string,
) {
	if route == nil {
		return
	}
	route.OfferedSupportedFeatures = offered
	route.NegotiatedSupportedFeatures = negotiated
	route.HierarchicalFLFeatureNegotiated = wire.SupportedFeaturesInclude(
		negotiated, wire.HierarchicalFLOrchestrationFeature,
	)
}

func candidateTrainingOperationProblem(
	route nwdaf_context.MLModelTrainingSubscriptionRoute,
	hasCandidateFields bool,
) *models.ProblemDetails {
	if !hasCandidateFields || route.HierarchicalFLFeatureNegotiated {
		return nil
	}
	return mlModelTrainingValidationProblem(&wire.RequirementsError{
		Violations: []wire.InvalidParameter{{
			Parameter: "suppFeats",
			Reason:    "HierarchicalFLOrch was not negotiated for this resource",
		}},
	})
}

func trainingRouteCallbackURI(
	p *Processor,
	route nwdaf_context.MLModelTrainingSubscriptionRoute,
) string {
	if route.PeerRoute.SelectedTarget != nil {
		return p.publicMLModelCallbackURI("ml-model-training", route.CallbackRouteID)
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
	if problem, ok := wire.ProblemDetailsForValidation(err); ok {
		return problem
	}
	return malformedMLModelProblem(err)
}

func (p *Processor) removeCreatingTrainingRoute(key nwdaf_context.MLModelTrainingResourceKey, revision uint64) {
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		return
	}
	route, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(key)
	if found && mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		nwdafContext.DeleteMLModelTrainingSubscriptionRoute(key)
	}
}

func (p *Processor) removeCreatingPendingTrainingRoute(callbackRouteID string, revision uint64) {
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		return
	}
	route, found := nwdafContext.GetPendingMLModelTrainingRoute(callbackRouteID)
	if found && mlModelRouteOperationCurrent(route.PeerRoute, nwdaf_context.MLModelRouteCreating, revision) {
		nwdafContext.DeletePendingMLModelTrainingRoute(callbackRouteID)
	}
}

func (p *Processor) finishTrainingMutationFailure(
	key nwdaf_context.MLModelTrainingResourceKey,
	revision uint64,
	deleteRoute bool,
) {
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		return
	}
	route, found := nwdafContext.GetMLModelTrainingSubscriptionRoute(key)
	if !found || !mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteReplacing,
		revision,
	) {
		return
	}
	if deleteRoute {
		nwdafContext.DeleteMLModelTrainingSubscriptionRoute(key)
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
		p.removeCreatingPendingTrainingRoute(routeID, revision)
		return
	}
	cleanupResponse, cleanupErr := p.mlModelPeerConsumer.DeletePeerMLModelTraining(ctx, location)
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()
	route, found := nwdafContext.GetPendingMLModelTrainingRoute(routeID)
	if !found || !mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		return
	}
	if cleanupResponseAccepted(cleanupResponse, cleanupErr) {
		nwdafContext.DeletePendingMLModelTrainingRoute(routeID)
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
