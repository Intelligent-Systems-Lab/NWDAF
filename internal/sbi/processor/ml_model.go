package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/free5gc/nwdaf/internal/backend"
	wire "github.com/free5gc/nwdaf/internal/compat/mlmodel"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

const (
	mlModelProvisionCallbackPath = "/internal/v1/ml-model-provision/notifications"
	mlModelMonitorCallbackPath   = "/internal/v1/ml-model-monitor/notifications"
)

func (p *Processor) HandleCreateMLModelProvision(
	requestContext context.Context,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.handleCreateMLModelProvision(
		requestContext,
		body,
		nwdaf_context.MLModelRoutePartyExternal,
		nwdaf_context.MLModelRoutePartyExternal,
	)
}

func (p *Processor) HandleCreateMLModelProvisionFromBackend(
	requestContext context.Context,
	body []byte,
	target *backend.SelectedTarget,
) (*backend.StandardResponse, *models.ProblemDetails) {
	if target != nil {
		nwdafContext := p.nwdaf.Context()
		if nwdafContext != nil && target.NFInstanceID == nwdafContext.NfId {
			return p.handleCreateMLModelProvision(
				requestContext,
				body,
				nwdaf_context.MLModelRoutePartyAnLFBackend,
				nwdaf_context.MLModelRoutePartyAnLFBackend,
			)
		}
		return p.handleCreateRemoteMLModelProvision(requestContext, body, *target)
	}
	return p.handleCreateMLModelProvision(
		requestContext,
		body,
		nwdaf_context.MLModelRoutePartyAnLFBackend,
		nwdaf_context.MLModelRoutePartyAnLFBackend,
	)
}

func (p *Processor) handleCreateMLModelProvision(
	requestContext context.Context,
	body []byte,
	initiator nwdaf_context.MLModelRouteParty,
	destination nwdaf_context.MLModelRouteParty,
) (*backend.StandardResponse, *models.ProblemDetails) {
	parsed, err := wire.ParseMLModelProvisionSubscription(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	generationLease1, admitted1 := acquireBackend(p.mtlfMLModelBackend, p.mtlfAvailability)
	if !admitted1 {
		return nil, mlModelUnavailableProblem()
	}
	if generationLease1 != nil {
		defer generationLease1.Release()
	}
	backendBody, err := wire.ReplaceStringField(
		body, "notifUri", p.mtlfCallbackURI(mlModelProvisionCallbackPath),
	)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	localRouteID := uuid.New().String()
	peerRoute := nwdaf_context.MLModelPeerRoute{
		Direction:         nwdaf_context.MLModelRouteDirectionInbound,
		LifecycleState:    nwdaf_context.MLModelRouteCreating,
		ProcessGeneration: p.backendGeneration(p.mtlfAvailability),
	}
	if initiator == nwdaf_context.MLModelRoutePartyAnLFBackend {
		peerRoute.RelatedBackend = backend.KindAnLF
		peerRoute.RelatedGeneration = p.backendGeneration(p.anlfAvailability)
	}
	p.mlModelMu.Lock()
	peerRoute.OperationRevision = p.nextMLModelOperationRevisionLocked()
	revision := peerRoute.OperationRevision
	nwdafContext := p.nwdaf.Context()
	reserved := nwdafContext != nil && nwdafContext.AddMLModelProvisionSubscriptionRoute(
		nwdaf_context.MLModelProvisionSubscriptionRoute{
			SubscriptionID:             localRouteID,
			PeerRoute:                  peerRoute,
			Initiator:                  initiator,
			Destination:                destination,
			DestinationNotificationURI: parsed.NotificationURI,
			NotificationCorrelationID:  parsed.NotificationID,
		},
	)
	p.mlModelMu.Unlock()
	if !reserved {
		return nil, mlModelInternalProblem("could not reserve ML Model Provision subscription route")
	}
	response, err := p.mtlfMLModelBackend.CreateMLModelProvisionSubscription(requestContext, backendBody)
	if err != nil {
		p.removeCreatingProvisionRoute(localRouteID, revision)
		return nil, p.mlModelBackendProblem(err, p.mtlfAvailability)
	}
	if response == nil || response.StatusCode != http.StatusCreated {
		p.removeCreatingProvisionRoute(localRouteID, revision)
		return nil, mlModelBadGatewayProblem("MTLF backend returned an invalid create response")
	}
	backendResourceID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		p.removeCreatingProvisionRoute(localRouteID, revision)
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	externalBody, err := wire.ReplaceStringField(response.Body, "notifUri", parsed.NotificationURI)
	if err != nil {
		p.removeCreatingProvisionRoute(localRouteID, revision)
		p.compensateLocalProvisionCreate(requestContext, backendResourceID)
		return nil, mlModelBadGatewayProblem("MTLF backend returned an invalid provision representation")
	}
	p.mlModelMu.Lock()
	current, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(localRouteID)
	if !found || !mlModelRouteOperationCurrent(
		current.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		p.mlModelMu.Unlock()
		p.compensateLocalProvisionCreate(requestContext, backendResourceID)
		return nil, mlModelUnavailableProblem()
	}
	current.PeerRoute.BackendLocation = response.Location
	current.PeerRoute.BackendResourceID = backendResourceID
	restoreActiveMLModelRoute(&current.PeerRoute)
	current.AcceptedRepresentation = externalBody
	current.BackendRepresentation = response.Body
	if !nwdafContext.UpdateMLModelProvisionSubscriptionRoute(current) {
		p.mlModelMu.Unlock()
		p.compensateLocalProvisionCreate(requestContext, backendResourceID)
		return nil, mlModelInternalProblem("could not record ML Model Provision subscription route")
	}
	p.mlModelMu.Unlock()
	p.mtlfAvailability.Refresh()
	return &backend.StandardResponse{
		StatusCode:  http.StatusCreated,
		Location:    p.publicResourceLocation(factory.NwdafMLModelProvisionResURIPrefix, "subscriptions", localRouteID),
		ContentType: "application/json",
		Body:        externalBody,
	}, nil
}

func (p *Processor) removeCreatingProvisionRoute(routeID string, revision uint64) {
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		return
	}
	route, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(routeID)
	if found && mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		nwdafContext.DeleteMLModelProvisionSubscriptionRoute(routeID)
	}
}

func (p *Processor) compensateLocalProvisionCreate(ctx context.Context, backendResourceID string) {
	if _, cleanupErr := p.mtlfMLModelBackend.DeleteMLModelProvisionSubscription(
		ctx,
		backendResourceID,
	); cleanupErr != nil {
		logger.ProcLog.Errorf(
			"Failed to compensate ML Model Provision create: subscriptionId=%s err=%v",
			backendResourceID,
			cleanupErr,
		)
	}
}

func (p *Processor) HandleReplaceMLModelProvisionFromBackend(
	requestContext context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.HandleReplaceMLModelProvision(requestContext, subscriptionID, body)
}

func (p *Processor) HandleDeleteMLModelProvisionFromBackend(
	requestContext context.Context,
	subscriptionID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.HandleDeleteMLModelProvision(requestContext, subscriptionID)
}

func (p *Processor) HandleMLModelProvisionNotification(
	requestContext context.Context,
	pathSubscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	notifications, err := wire.ParseMLModelProvisionNotifications(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	p.mlModelMu.Lock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	subscriptionID := notifications[0].SubscriptionID
	if pathSubscriptionID != "" {
		pathRoute, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(pathSubscriptionID)
		if !found {
			p.mlModelMu.Unlock()
			return nil, mlModelResourceNotFoundProblem(
				"ML Model Provision subscription",
				pathSubscriptionID,
			)
		}
		if problem := validateOutboundMLModelCallbackRoute(
			pathRoute.PeerRoute,
			p.anlfAvailability,
		); problem != nil {
			p.mlModelMu.Unlock()
			return nil, problem
		}
		if hint := peerResourceIDHint(pathRoute.PeerRoute.PeerLocation); hint != "" &&
			hint != subscriptionID {
			p.mlModelMu.Unlock()
			return nil, malformedMLModelProblem(errors.New(
				"notification subscriptionId does not match peer resource",
			))
		}
		if subscriptionID != pathSubscriptionID {
			body, err = wire.ReplaceProvisionNotificationSubscriptionID(body, pathSubscriptionID)
			if err != nil {
				p.mlModelMu.Unlock()
				return nil, malformedMLModelProblem(err)
			}
			notifications, err = wire.ParseMLModelProvisionNotifications(body)
			if err != nil {
				p.mlModelMu.Unlock()
				return nil, malformedMLModelProblem(err)
			}
		}
		subscriptionID = pathSubscriptionID
	} else {
		if _, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(subscriptionID); !found {
			backendRoute, backendFound := nwdafContext.
				FindMLModelProvisionSubscriptionRouteByBackendResourceID(subscriptionID)
			if !backendFound {
				p.mlModelMu.Unlock()
				return nil, mlModelResourceNotFoundProblem(
					"ML Model Provision subscription",
					subscriptionID,
				)
			}
			body, err = wire.ReplaceProvisionNotificationSubscriptionID(
				body,
				backendRoute.SubscriptionID,
			)
			if err != nil {
				p.mlModelMu.Unlock()
				return nil, malformedMLModelProblem(err)
			}
			notifications, err = wire.ParseMLModelProvisionNotifications(body)
			if err != nil {
				p.mlModelMu.Unlock()
				return nil, malformedMLModelProblem(err)
			}
			subscriptionID = backendRoute.SubscriptionID
		}
	}
	route, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(subscriptionID)
	if !found {
		p.mlModelMu.Unlock()
		return nil, mlModelResourceNotFoundProblem(
			"ML Model Provision subscription",
			subscriptionID,
		)
	}
	if !mlModelRouteAcceptsCallback(route.PeerRoute) {
		p.mlModelMu.Unlock()
		return nil, mlModelUnavailableProblem()
	}
	for _, notification := range notifications {
		if notification.SubscriptionID != subscriptionID {
			p.mlModelMu.Unlock()
			return nil, malformedMLModelProblem(errors.New(
				"notification array must reference one provision subscription",
			))
		}
		for _, event := range notification.EventNotifications {
			if route.NotificationCorrelationID != "" &&
				event.NotificationID != route.NotificationCorrelationID {
				p.mlModelMu.Unlock()
				return nil, malformedMLModelProblem(errors.New(
					"notification correlation does not match provision subscription",
				))
			}
		}
	}

	var response *backend.StandardResponse
	var generationLease *backend.GenerationLease
	if route.Destination == nwdaf_context.MLModelRoutePartyAnLFBackend {
		var admitted bool
		generationLease, admitted = acquireBackend(p.anlfMLModelBackend, p.anlfAvailability)
		if !admitted {
			p.mlModelMu.Unlock()
			return nil, mlModelUnavailableProblem()
		}
	}
	p.mlModelMu.Unlock()
	if generationLease != nil {
		defer generationLease.Release()
	}
	switch route.Destination {
	case nwdaf_context.MLModelRoutePartyAnLFBackend:
		response, err = p.anlfMLModelBackend.DeliverMLModelProvisionNotification(
			requestContext,
			subscriptionID,
			body,
		)
		if err != nil {
			return nil, p.mlModelBackendProblem(err, p.anlfAvailability)
		}
		p.anlfAvailability.Refresh()
	case nwdaf_context.MLModelRoutePartyExternal:
		response, err = backend.ExecuteStandardRequest(
			requestContext,
			p.mlModelHTTPClient,
			10*time.Second,
			http.MethodPost,
			route.DestinationNotificationURI,
			body,
			"deliver external ML Model Provision notification",
			backend.StandardOperationContract{
				SuccessValidators: map[int]func([]byte) error{http.StatusNoContent: nil},
				ErrorStatuses:     mlModelCallbackErrorStatuses(),
				FollowRedirects:   true,
			},
		)
		if err != nil {
			return nil, p.mlModelCallbackProblem(err)
		}
	default:
		return nil, mlModelInternalProblem("provision notification destination is invalid")
	}
	if response == nil {
		return nil, mlModelBadGatewayProblem("notification destination returned no response")
	}
	return response, nil
}

func (p *Processor) HandleReplaceMLModelProvision(
	requestContext context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	parsed, err := wire.ParseMLModelProvisionSubscription(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	p.mlModelMu.Lock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	route, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(subscriptionID)
	if !found {
		p.mlModelMu.Unlock()
		return nil, mlModelResourceNotFoundProblem("ML Model Provision subscription", subscriptionID)
	}
	revision, problem := p.beginMLModelRouteOperationLocked(
		&route.PeerRoute,
		nwdaf_context.MLModelRouteReplacing,
	)
	if problem != nil {
		p.mlModelMu.Unlock()
		return nil, problem
	}
	if !nwdafContext.UpdateMLModelProvisionSubscriptionRoute(route) {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("could not reserve ML Model Provision replacement")
	}
	isPeer := route.PeerRoute.SelectedTarget != nil
	var generationLease *backend.GenerationLease
	if isPeer {
		if p.mlModelPeerConsumer == nil {
			restoreActiveMLModelRoute(&route.PeerRoute)
			nwdafContext.UpdateMLModelProvisionSubscriptionRoute(route)
			p.mlModelMu.Unlock()
			return nil, mlModelUnavailableProblem()
		}
	} else {
		var admitted bool
		generationLease, admitted = acquireBackend(p.mtlfMLModelBackend, p.mtlfAvailability)
		if !admitted {
			restoreActiveMLModelRoute(&route.PeerRoute)
			nwdafContext.UpdateMLModelProvisionSubscriptionRoute(route)
			p.mlModelMu.Unlock()
			return nil, mlModelUnavailableProblem()
		}
	}
	callbackURI := p.mtlfCallbackURI(mlModelProvisionCallbackPath)
	if isPeer {
		callbackURI = p.publicMLModelCallbackURI("ml-model-provision", subscriptionID)
	}
	routedBody, err := wire.ReplaceStringField(body, "notifUri", callbackURI)
	if err != nil {
		restoreActiveMLModelRoute(&route.PeerRoute)
		nwdafContext.UpdateMLModelProvisionSubscriptionRoute(route)
		p.mlModelMu.Unlock()
		return nil, malformedMLModelProblem(err)
	}
	p.mlModelMu.Unlock()
	if generationLease != nil {
		defer generationLease.Release()
	}

	var response *backend.StandardResponse
	var operationErr error
	if isPeer {
		response, operationErr = p.mlModelPeerConsumer.ReplacePeerMLModelProvision(
			requestContext,
			route.PeerRoute.PeerLocation,
			routedBody,
		)
	} else {
		response, operationErr = p.mtlfMLModelBackend.ReplaceMLModelProvisionSubscription(
			requestContext,
			route.PeerRoute.BackendResourceID,
			routedBody,
		)
	}
	if operationErr != nil {
		p.finishProvisionReplaceFailure(subscriptionID, revision, isPeer && peerMissing(operationErr))
		if isPeer {
			return nil, p.mlModelPeerProblem(operationErr)
		}
		return nil, p.mlModelBackendProblem(operationErr, p.mtlfAvailability)
	}
	if response == nil || response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		p.finishProvisionReplaceFailure(subscriptionID, revision, false)
		return nil, mlModelBadGatewayProblem("ML Model Provision destination returned an invalid replace response")
	}
	externalBody := append(json.RawMessage(nil), body...)
	backendRepresentation := append(json.RawMessage(nil), routedBody...)
	if response.StatusCode == http.StatusOK {
		externalBody, err = wire.ReplaceStringField(response.Body, "notifUri", parsed.NotificationURI)
		if err != nil {
			p.finishProvisionReplaceFailure(subscriptionID, revision, false)
			return nil, mlModelBadGatewayProblem("ML Model Provision destination returned an invalid representation")
		}
		backendRepresentation = append(json.RawMessage(nil), response.Body...)
	}
	p.mlModelMu.Lock()
	current, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(subscriptionID)
	if !found || !mlModelRouteOperationCurrent(
		current.PeerRoute,
		nwdaf_context.MLModelRouteReplacing,
		revision,
	) {
		p.mlModelMu.Unlock()
		return nil, mlModelUnavailableProblem()
	}
	restoreActiveMLModelRoute(&current.PeerRoute)
	current.AcceptedRepresentation = externalBody
	current.BackendRepresentation = backendRepresentation
	current.DestinationNotificationURI = parsed.NotificationURI
	current.NotificationCorrelationID = parsed.NotificationID
	if response.PermanentRedirectURI != "" {
		current.PeerRoute.PeerLocation = response.PermanentRedirectURI
	}
	if !nwdafContext.UpdateMLModelProvisionSubscriptionRoute(current) {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("could not update ML Model Provision subscription route")
	}
	p.mlModelMu.Unlock()
	if !isPeer {
		p.mtlfAvailability.Refresh()
	}
	return &backend.StandardResponse{
		StatusCode:  response.StatusCode,
		ContentType: response.ContentType,
		Body:        externalBodyForStatus(response.StatusCode, externalBody),
	}, nil
}

func (p *Processor) finishProvisionReplaceFailure(
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
	route, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(subscriptionID)
	if !found || !mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteReplacing,
		revision,
	) {
		return
	}
	if deleteRoute {
		nwdafContext.DeleteMLModelProvisionSubscriptionRoute(subscriptionID)
		return
	}
	restoreActiveMLModelRoute(&route.PeerRoute)
	nwdafContext.UpdateMLModelProvisionSubscriptionRoute(route)
}

func (p *Processor) HandleDeleteMLModelProvision(
	requestContext context.Context,
	subscriptionID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	p.mlModelMu.Lock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	if nwdafContext.ConsumeMLModelDeletionRecord(
		nwdaf_context.MLModelResourceProvisionSubscription,
		subscriptionID,
	) {
		p.mlModelMu.Unlock()
		return noContentMLModelResponse(), nil
	}
	route, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(subscriptionID)
	if !found {
		p.mlModelMu.Unlock()
		return nil, mlModelResourceNotFoundProblem("ML Model Provision subscription", subscriptionID)
	}
	revision, problem := p.beginMLModelRouteOperationLocked(
		&route.PeerRoute,
		nwdaf_context.MLModelRouteDeleting,
	)
	if problem != nil {
		p.mlModelMu.Unlock()
		return nil, problem
	}
	if !nwdafContext.UpdateMLModelProvisionSubscriptionRoute(route) {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("could not reserve ML Model Provision deletion")
	}
	isPeer := route.PeerRoute.SelectedTarget != nil
	var generationLease *backend.GenerationLease
	if isPeer {
		if p.mlModelPeerConsumer == nil {
			restoreActiveMLModelRoute(&route.PeerRoute)
			nwdafContext.UpdateMLModelProvisionSubscriptionRoute(route)
			p.mlModelMu.Unlock()
			return nil, mlModelUnavailableProblem()
		}
	} else {
		var admitted bool
		generationLease, admitted = acquireBackend(p.mtlfMLModelBackend, p.mtlfAvailability)
		if !admitted {
			restoreActiveMLModelRoute(&route.PeerRoute)
			nwdafContext.UpdateMLModelProvisionSubscriptionRoute(route)
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
		response, operationErr = p.mlModelPeerConsumer.DeletePeerMLModelProvision(
			requestContext,
			route.PeerRoute.PeerLocation,
		)
	} else {
		response, operationErr = p.mtlfMLModelBackend.DeleteMLModelProvisionSubscription(
			requestContext,
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
	current, exists := nwdafContext.GetMLModelProvisionSubscriptionRoute(subscriptionID)
	if !exists || !mlModelRouteOperationCurrent(
		current.PeerRoute,
		nwdaf_context.MLModelRouteDeleting,
		revision,
	) {
		_, resetDeleted := nwdafContext.GetMLModelDeletionRecord(
			nwdaf_context.MLModelResourceProvisionSubscription,
			subscriptionID,
		)
		p.mlModelMu.Unlock()
		if !exists && resetDeleted {
			return noContentMLModelResponse(), nil
		}
		return nil, mlModelUnavailableProblem()
	}
	if terminal {
		nwdafContext.DeleteMLModelProvisionSubscriptionRoute(subscriptionID)
	} else {
		restoreActiveMLModelRoute(&current.PeerRoute)
		nwdafContext.UpdateMLModelProvisionSubscriptionRoute(current)
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
		if isPeer {
			return nil, p.mlModelPeerProblem(operationErr)
		}
		return nil, p.mlModelBackendProblem(operationErr, p.mtlfAvailability)
	}
	return nil, mlModelBadGatewayProblem("ML Model Provision destination returned an invalid delete response")
}

func (p *Processor) HandleCreateMLModelMonitorRegistration(
	requestContext context.Context,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.handleCreateMLModelMonitorRegistration(
		requestContext,
		body,
		nwdaf_context.MLModelRoutePartyExternal,
	)
}

func (p *Processor) HandleCreateMLModelMonitorRegistrationFromBackend(
	requestContext context.Context,
	body []byte,
	target *backend.SelectedTarget,
) (*backend.StandardResponse, *models.ProblemDetails) {
	if target != nil {
		nwdafContext := p.nwdaf.Context()
		if nwdafContext != nil && target.NFInstanceID == nwdafContext.NfId {
			return p.handleCreateMLModelMonitorRegistration(
				requestContext,
				body,
				nwdaf_context.MLModelRoutePartyAnLFBackend,
			)
		}
		return p.handleCreateRemoteMLModelMonitorRegistration(requestContext, body, *target)
	}
	return p.handleCreateMLModelMonitorRegistration(
		requestContext,
		body,
		nwdaf_context.MLModelRoutePartyAnLFBackend,
	)
}

func (p *Processor) handleCreateMLModelMonitorRegistration(
	requestContext context.Context,
	body []byte,
	initiator nwdaf_context.MLModelRouteParty,
) (*backend.StandardResponse, *models.ProblemDetails) {
	if _, err := wire.ParseMLModelMonitorRegistration(body); err != nil {
		return nil, malformedMLModelProblem(err)
	}
	generationLease5, admitted5 := acquireBackend(p.mtlfMLModelBackend, p.mtlfAvailability)
	if !admitted5 {
		return nil, mlModelUnavailableProblem()
	}
	if generationLease5 != nil {
		defer generationLease5.Release()
	}
	localRouteID := uuid.New().String()
	peerRoute := nwdaf_context.MLModelPeerRoute{
		Direction:         nwdaf_context.MLModelRouteDirectionInbound,
		LifecycleState:    nwdaf_context.MLModelRouteCreating,
		ProcessGeneration: p.backendGeneration(p.mtlfAvailability),
	}
	if initiator == nwdaf_context.MLModelRoutePartyAnLFBackend {
		peerRoute.RelatedBackend = backend.KindAnLF
		peerRoute.RelatedGeneration = p.backendGeneration(p.anlfAvailability)
	}
	p.mlModelMu.Lock()
	peerRoute.OperationRevision = p.nextMLModelOperationRevisionLocked()
	revision := peerRoute.OperationRevision
	nwdafContext := p.nwdaf.Context()
	reserved := nwdafContext != nil && nwdafContext.AddMLModelMonitorRegistrationRoute(
		nwdaf_context.MLModelMonitorRegistrationRoute{
			RegistrationID: localRouteID,
			PeerRoute:      peerRoute,
			Initiator:      initiator,
		},
	)
	p.mlModelMu.Unlock()
	if !reserved {
		return nil, mlModelInternalProblem("could not reserve ML Model Monitor registration route")
	}
	response, err := p.mtlfMLModelBackend.CreateMLModelMonitorRegistration(requestContext, body)
	if err != nil {
		p.removeCreatingRegistrationRoute(localRouteID, revision)
		return nil, p.mlModelBackendProblem(err, p.mtlfAvailability)
	}
	if response == nil || response.StatusCode != http.StatusCreated {
		p.removeCreatingRegistrationRoute(localRouteID, revision)
		return nil, mlModelBadGatewayProblem("MTLF backend returned an invalid registration response")
	}
	backendResourceID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		p.removeCreatingRegistrationRoute(localRouteID, revision)
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	p.mlModelMu.Lock()
	current, found := nwdafContext.GetMLModelMonitorRegistrationRoute(localRouteID)
	if !found || !mlModelRouteOperationCurrent(
		current.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		p.mlModelMu.Unlock()
		p.compensateLocalRegistrationCreate(requestContext, backendResourceID)
		return nil, mlModelUnavailableProblem()
	}
	current.PeerRoute.BackendLocation = response.Location
	current.PeerRoute.BackendResourceID = backendResourceID
	restoreActiveMLModelRoute(&current.PeerRoute)
	current.AcceptedRepresentation = response.Body
	current.BackendRepresentation = response.Body
	if !nwdafContext.UpdateMLModelMonitorRegistrationRoute(current) {
		p.mlModelMu.Unlock()
		p.compensateLocalRegistrationCreate(requestContext, backendResourceID)
		return nil, mlModelInternalProblem("could not record ML Model Monitor registration route")
	}
	p.mlModelMu.Unlock()
	p.mtlfAvailability.Refresh()
	return &backend.StandardResponse{
		StatusCode:  http.StatusCreated,
		Location:    p.publicResourceLocation(factory.NwdafMLModelMonitorResURIPrefix, "registrations", localRouteID),
		ContentType: "application/json",
		Body:        append(json.RawMessage(nil), response.Body...),
	}, nil
}

func (p *Processor) removeCreatingRegistrationRoute(routeID string, revision uint64) {
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		return
	}
	route, found := nwdafContext.GetMLModelMonitorRegistrationRoute(routeID)
	if found && mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		nwdafContext.DeleteMLModelMonitorRegistrationRoute(routeID)
	}
}

func (p *Processor) compensateLocalRegistrationCreate(ctx context.Context, backendResourceID string) {
	if _, cleanupErr := p.mtlfMLModelBackend.DeleteMLModelMonitorRegistration(
		ctx,
		backendResourceID,
	); cleanupErr != nil {
		logger.ProcLog.Errorf(
			"Failed to compensate ML Model Monitor registration create: registrationId=%s err=%v",
			backendResourceID,
			cleanupErr,
		)
	}
}

func (p *Processor) HandleDeleteMLModelMonitorRegistrationFromBackend(
	requestContext context.Context,
	registrationID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.HandleDeleteMLModelMonitorRegistration(requestContext, registrationID)
}

func (p *Processor) HandleDeleteMLModelMonitorRegistration(
	requestContext context.Context,
	registrationID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	p.mlModelMu.Lock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	if nwdafContext.ConsumeMLModelDeletionRecord(
		nwdaf_context.MLModelResourceMonitorRegistration,
		registrationID,
	) {
		p.mlModelMu.Unlock()
		return noContentMLModelResponse(), nil
	}
	route, found := nwdafContext.GetMLModelMonitorRegistrationRoute(registrationID)
	if !found {
		p.mlModelMu.Unlock()
		return nil, mlModelResourceNotFoundProblem("ML Model Monitor registration", registrationID)
	}
	revision, problem := p.beginMLModelRouteOperationLocked(
		&route.PeerRoute,
		nwdaf_context.MLModelRouteDeleting,
	)
	if problem != nil {
		p.mlModelMu.Unlock()
		return nil, problem
	}
	if !nwdafContext.UpdateMLModelMonitorRegistrationRoute(route) {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("could not reserve ML Model Monitor registration deletion")
	}
	isPeer := route.PeerRoute.SelectedTarget != nil
	var generationLease *backend.GenerationLease
	if isPeer {
		if p.mlModelPeerConsumer == nil {
			restoreActiveMLModelRoute(&route.PeerRoute)
			nwdafContext.UpdateMLModelMonitorRegistrationRoute(route)
			p.mlModelMu.Unlock()
			return nil, mlModelUnavailableProblem()
		}
	} else {
		var admitted bool
		generationLease, admitted = acquireBackend(p.mtlfMLModelBackend, p.mtlfAvailability)
		if !admitted {
			restoreActiveMLModelRoute(&route.PeerRoute)
			nwdafContext.UpdateMLModelMonitorRegistrationRoute(route)
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
		response, operationErr = p.mlModelPeerConsumer.DeletePeerMLModelMonitorRegistration(
			requestContext,
			route.PeerRoute.PeerLocation,
		)
	} else {
		response, operationErr = p.mtlfMLModelBackend.DeleteMLModelMonitorRegistration(
			requestContext,
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
	current, exists := nwdafContext.GetMLModelMonitorRegistrationRoute(registrationID)
	if !exists || !mlModelRouteOperationCurrent(
		current.PeerRoute,
		nwdaf_context.MLModelRouteDeleting,
		revision,
	) {
		_, resetDeleted := nwdafContext.GetMLModelDeletionRecord(
			nwdaf_context.MLModelResourceMonitorRegistration,
			registrationID,
		)
		p.mlModelMu.Unlock()
		if !exists && resetDeleted {
			return noContentMLModelResponse(), nil
		}
		return nil, mlModelUnavailableProblem()
	}
	if terminal {
		nwdafContext.DeleteMLModelMonitorRegistrationRoute(registrationID)
	} else {
		restoreActiveMLModelRoute(&current.PeerRoute)
		nwdafContext.UpdateMLModelMonitorRegistrationRoute(current)
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
		if isPeer {
			return nil, p.mlModelPeerProblem(operationErr)
		}
		return nil, p.mlModelBackendProblem(operationErr, p.mtlfAvailability)
	}
	return nil, mlModelBadGatewayProblem("ML Model Monitor registration destination returned an invalid delete response")
}

func (p *Processor) HandleCreateMLModelMonitorSubscription(
	requestContext context.Context,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.handleCreateMLModelMonitorSubscription(
		requestContext,
		body,
		nwdaf_context.MLModelRoutePartyExternal,
		"",
	)
}

func (p *Processor) HandleCreateMLModelMonitorSubscriptionFromBackend(
	requestContext context.Context,
	body []byte,
	ownerRegistrationID string,
	target *backend.SelectedTarget,
) (*backend.StandardResponse, *models.ProblemDetails) {
	if target != nil {
		nwdafContext := p.nwdaf.Context()
		if nwdafContext != nil && target.NFInstanceID == nwdafContext.NfId {
			p.mlModelMu.Lock()
			normalizedOwnerID, err := normalizeMonitorOwnerRegistrationID(
				nwdafContext,
				ownerRegistrationID,
			)
			p.mlModelMu.Unlock()
			if err != nil {
				return nil, malformedMLModelProblem(err)
			}
			return p.handleCreateMLModelMonitorSubscription(
				requestContext,
				body,
				nwdaf_context.MLModelRoutePartyMTLFBackend,
				normalizedOwnerID,
			)
		}
		return p.handleCreateRemoteMLModelMonitorSubscription(
			requestContext,
			body,
			ownerRegistrationID,
			*target,
		)
	}
	return p.handleCreateMLModelMonitorSubscription(
		requestContext,
		body,
		nwdaf_context.MLModelRoutePartyMTLFBackend,
		ownerRegistrationID,
	)
}

func (p *Processor) handleCreateMLModelMonitorSubscription(
	requestContext context.Context,
	body []byte,
	destination nwdaf_context.MLModelRouteParty,
	ownerRegistrationID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	parsed, err := wire.ParseMLModelMonitorSubscription(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	generationLease7, admitted7 := acquireBackend(p.anlfMLModelBackend, p.anlfAvailability)
	if !admitted7 {
		return nil, mlModelUnavailableProblem()
	}
	if generationLease7 != nil {
		defer generationLease7.Release()
	}
	backendBody, err := wire.ReplaceStringField(
		body, "notificationUri", p.anlfCallbackURI(mlModelMonitorCallbackPath),
	)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	localRouteID := uuid.New().String()
	peerRoute := nwdaf_context.MLModelPeerRoute{
		Direction:         nwdaf_context.MLModelRouteDirectionInbound,
		LifecycleState:    nwdaf_context.MLModelRouteCreating,
		ProcessGeneration: p.backendGeneration(p.anlfAvailability),
	}
	if destination == nwdaf_context.MLModelRoutePartyMTLFBackend {
		peerRoute.RelatedBackend = backend.KindMTLF
		peerRoute.RelatedGeneration = p.backendGeneration(p.mtlfAvailability)
	}
	p.mlModelMu.Lock()
	peerRoute.OperationRevision = p.nextMLModelOperationRevisionLocked()
	revision := peerRoute.OperationRevision
	nwdafContext := p.nwdaf.Context()
	reserved := nwdafContext != nil && nwdafContext.AddMLModelMonitorSubscriptionRoute(
		nwdaf_context.MLModelMonitorSubscriptionRoute{
			SubscriptionID:             localRouteID,
			PeerRoute:                  peerRoute,
			OwnerRegistrationID:        ownerRegistrationID,
			Destination:                destination,
			DestinationNotificationURI: parsed.NotificationURI,
			NotificationCorrelationID:  parsed.NotificationID,
		},
	)
	p.mlModelMu.Unlock()
	if !reserved {
		return nil, mlModelInternalProblem("could not reserve ML Model Monitor subscription route")
	}
	response, err := p.anlfMLModelBackend.CreateMLModelMonitorSubscription(requestContext, backendBody)
	if err != nil {
		p.removeCreatingMonitorRoute(localRouteID, revision)
		return nil, p.mlModelBackendProblem(err, p.anlfAvailability)
	}
	if response == nil || response.StatusCode != http.StatusCreated {
		p.removeCreatingMonitorRoute(localRouteID, revision)
		return nil, mlModelBadGatewayProblem("AnLF backend returned an invalid subscription response")
	}
	backendResourceID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		p.removeCreatingMonitorRoute(localRouteID, revision)
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	externalBody, err := wire.ReplaceStringField(response.Body, "notificationUri", parsed.NotificationURI)
	if err != nil {
		p.removeCreatingMonitorRoute(localRouteID, revision)
		p.compensateLocalMonitorCreate(requestContext, backendResourceID)
		return nil, mlModelBadGatewayProblem("AnLF backend returned an invalid monitor representation")
	}
	p.mlModelMu.Lock()
	current, found := nwdafContext.GetMLModelMonitorSubscriptionRoute(localRouteID)
	if !found || !mlModelRouteOperationCurrent(
		current.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		p.mlModelMu.Unlock()
		p.compensateLocalMonitorCreate(requestContext, backendResourceID)
		return nil, mlModelUnavailableProblem()
	}
	current.PeerRoute.BackendLocation = response.Location
	current.PeerRoute.BackendResourceID = backendResourceID
	restoreActiveMLModelRoute(&current.PeerRoute)
	current.AcceptedRepresentation = externalBody
	current.BackendRepresentation = response.Body
	if !nwdafContext.UpdateMLModelMonitorSubscriptionRoute(current) {
		p.mlModelMu.Unlock()
		p.compensateLocalMonitorCreate(requestContext, backendResourceID)
		return nil, mlModelInternalProblem("could not record ML Model Monitor subscription route")
	}
	p.mlModelMu.Unlock()
	p.anlfAvailability.Refresh()
	return &backend.StandardResponse{
		StatusCode:  http.StatusCreated,
		Location:    p.publicResourceLocation(factory.NwdafMLModelMonitorResURIPrefix, "subscriptions", localRouteID),
		ContentType: "application/json",
		Body:        externalBody,
	}, nil
}

func (p *Processor) removeCreatingMonitorRoute(routeID string, revision uint64) {
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		return
	}
	route, found := nwdafContext.GetMLModelMonitorSubscriptionRoute(routeID)
	if found && mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		nwdafContext.DeleteMLModelMonitorSubscriptionRoute(routeID)
	}
}

func (p *Processor) compensateLocalMonitorCreate(ctx context.Context, backendResourceID string) {
	if _, cleanupErr := p.anlfMLModelBackend.DeleteMLModelMonitorSubscription(
		ctx,
		backendResourceID,
	); cleanupErr != nil {
		logger.ProcLog.Errorf(
			"Failed to compensate ML Model Monitor create: subscriptionId=%s err=%v",
			backendResourceID,
			cleanupErr,
		)
	}
}

func (p *Processor) HandleReplaceMLModelMonitorSubscriptionFromBackend(
	requestContext context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.HandleReplaceMLModelMonitorSubscription(requestContext, subscriptionID, body)
}

func (p *Processor) HandleDeleteMLModelMonitorSubscriptionFromBackend(
	requestContext context.Context,
	subscriptionID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return p.HandleDeleteMLModelMonitorSubscription(requestContext, subscriptionID)
}

func (p *Processor) HandleMLModelMonitorNotification(
	requestContext context.Context,
	pathSubscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	notification, err := wire.ParseMLModelMonitorNotification(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	p.mlModelMu.Lock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	var route nwdaf_context.MLModelMonitorSubscriptionRoute
	var found bool
	if pathSubscriptionID != "" {
		route, found = nwdafContext.GetMLModelMonitorSubscriptionRoute(pathSubscriptionID)
	} else {
		route, found = nwdafContext.FindMLModelMonitorSubscriptionRouteByCorrelation(
			notification.NotificationID,
		)
	}
	if !found {
		p.mlModelMu.Unlock()
		return nil, mlModelResourceNotFoundProblem(
			"ML Model Monitor subscription",
			pathSubscriptionID,
		)
	}
	if pathSubscriptionID != "" {
		if problem := validateOutboundMLModelCallbackRoute(
			route.PeerRoute,
			p.mtlfAvailability,
		); problem != nil {
			p.mlModelMu.Unlock()
			return nil, problem
		}
	}
	if !mlModelRouteAcceptsCallback(route.PeerRoute) {
		p.mlModelMu.Unlock()
		return nil, mlModelUnavailableProblem()
	}
	if notification.NotificationID != route.NotificationCorrelationID {
		p.mlModelMu.Unlock()
		return nil, malformedMLModelProblem(errors.New(
			"notification correlation does not match monitor subscription",
		))
	}
	if validationErr := validateMonitorNotificationModels(
		route.AcceptedRepresentation,
		notification,
	); validationErr != nil {
		p.mlModelMu.Unlock()
		return nil, malformedMLModelProblem(validationErr)
	}

	var response *backend.StandardResponse
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
	switch route.Destination {
	case nwdaf_context.MLModelRoutePartyMTLFBackend:
		response, err = p.mtlfMLModelBackend.DeliverMLModelMonitorNotification(
			requestContext,
			body,
		)
		if err != nil {
			return nil, p.mlModelBackendProblem(err, p.mtlfAvailability)
		}
		p.mtlfAvailability.Refresh()
	case nwdaf_context.MLModelRoutePartyExternal:
		response, err = backend.ExecuteStandardRequest(
			requestContext,
			p.mlModelHTTPClient,
			10*time.Second,
			http.MethodPost,
			route.DestinationNotificationURI,
			body,
			"deliver external ML Model Monitor notification",
			backend.StandardOperationContract{
				SuccessValidators: map[int]func([]byte) error{http.StatusNoContent: nil},
				ErrorStatuses:     mlModelCallbackErrorStatuses(),
				FollowRedirects:   true,
			},
		)
		if err != nil {
			return nil, p.mlModelCallbackProblem(err)
		}
	default:
		return nil, mlModelInternalProblem("monitor notification destination is invalid")
	}
	if response == nil {
		return nil, mlModelBadGatewayProblem("notification destination returned no response")
	}
	return response, nil
}

func (p *Processor) HandleReplaceMLModelMonitorSubscription(
	requestContext context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	parsed, err := wire.ParseMLModelMonitorSubscription(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	p.mlModelMu.Lock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	route, found := nwdafContext.GetMLModelMonitorSubscriptionRoute(subscriptionID)
	if !found {
		p.mlModelMu.Unlock()
		return nil, mlModelResourceNotFoundProblem("ML Model Monitor subscription", subscriptionID)
	}
	revision, problem := p.beginMLModelRouteOperationLocked(
		&route.PeerRoute,
		nwdaf_context.MLModelRouteReplacing,
	)
	if problem != nil {
		p.mlModelMu.Unlock()
		return nil, problem
	}
	if !nwdafContext.UpdateMLModelMonitorSubscriptionRoute(route) {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("could not reserve ML Model Monitor replacement")
	}
	isPeer := route.PeerRoute.SelectedTarget != nil
	var generationLease *backend.GenerationLease
	if isPeer {
		if p.mlModelPeerConsumer == nil {
			restoreActiveMLModelRoute(&route.PeerRoute)
			nwdafContext.UpdateMLModelMonitorSubscriptionRoute(route)
			p.mlModelMu.Unlock()
			return nil, mlModelUnavailableProblem()
		}
	} else {
		var admitted bool
		generationLease, admitted = acquireBackend(p.anlfMLModelBackend, p.anlfAvailability)
		if !admitted {
			restoreActiveMLModelRoute(&route.PeerRoute)
			nwdafContext.UpdateMLModelMonitorSubscriptionRoute(route)
			p.mlModelMu.Unlock()
			return nil, mlModelUnavailableProblem()
		}
	}
	callbackURI := p.anlfCallbackURI(mlModelMonitorCallbackPath)
	if isPeer {
		callbackURI = p.publicMLModelCallbackURI("ml-model-monitor", subscriptionID)
	}
	routedBody, err := wire.ReplaceStringField(body, "notificationUri", callbackURI)
	if err != nil {
		restoreActiveMLModelRoute(&route.PeerRoute)
		nwdafContext.UpdateMLModelMonitorSubscriptionRoute(route)
		p.mlModelMu.Unlock()
		return nil, malformedMLModelProblem(err)
	}
	p.mlModelMu.Unlock()
	if generationLease != nil {
		defer generationLease.Release()
	}

	var response *backend.StandardResponse
	var operationErr error
	if isPeer {
		response, operationErr = p.mlModelPeerConsumer.ReplacePeerMLModelMonitorSubscription(
			requestContext,
			route.PeerRoute.PeerLocation,
			routedBody,
		)
	} else {
		response, operationErr = p.anlfMLModelBackend.ReplaceMLModelMonitorSubscription(
			requestContext,
			route.PeerRoute.BackendResourceID,
			routedBody,
		)
	}
	if operationErr != nil {
		p.finishMonitorReplaceFailure(subscriptionID, revision, isPeer && peerMissing(operationErr))
		if isPeer {
			return nil, p.mlModelPeerProblem(operationErr)
		}
		return nil, p.mlModelBackendProblem(operationErr, p.anlfAvailability)
	}
	if response == nil || response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		p.finishMonitorReplaceFailure(subscriptionID, revision, false)
		return nil, mlModelBadGatewayProblem("ML Model Monitor destination returned an invalid replace response")
	}
	externalBody := append(json.RawMessage(nil), body...)
	backendRepresentation := append(json.RawMessage(nil), routedBody...)
	if response.StatusCode == http.StatusOK {
		externalBody, err = wire.ReplaceStringField(response.Body, "notificationUri", parsed.NotificationURI)
		if err != nil {
			p.finishMonitorReplaceFailure(subscriptionID, revision, false)
			return nil, mlModelBadGatewayProblem("ML Model Monitor destination returned an invalid representation")
		}
		backendRepresentation = append(json.RawMessage(nil), response.Body...)
	}
	p.mlModelMu.Lock()
	current, found := nwdafContext.GetMLModelMonitorSubscriptionRoute(subscriptionID)
	if !found || !mlModelRouteOperationCurrent(
		current.PeerRoute,
		nwdaf_context.MLModelRouteReplacing,
		revision,
	) {
		p.mlModelMu.Unlock()
		return nil, mlModelUnavailableProblem()
	}
	restoreActiveMLModelRoute(&current.PeerRoute)
	current.AcceptedRepresentation = externalBody
	current.BackendRepresentation = backendRepresentation
	current.DestinationNotificationURI = parsed.NotificationURI
	current.NotificationCorrelationID = parsed.NotificationID
	if response.PermanentRedirectURI != "" {
		current.PeerRoute.PeerLocation = response.PermanentRedirectURI
	}
	if !nwdafContext.UpdateMLModelMonitorSubscriptionRoute(current) {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("could not update ML Model Monitor subscription route")
	}
	p.mlModelMu.Unlock()
	if !isPeer {
		p.anlfAvailability.Refresh()
	}
	return &backend.StandardResponse{
		StatusCode:  response.StatusCode,
		ContentType: response.ContentType,
		Body:        externalBodyForStatus(response.StatusCode, externalBody),
	}, nil
}

func (p *Processor) finishMonitorReplaceFailure(
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
	route, found := nwdafContext.GetMLModelMonitorSubscriptionRoute(subscriptionID)
	if !found || !mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteReplacing,
		revision,
	) {
		return
	}
	if deleteRoute {
		nwdafContext.DeleteMLModelMonitorSubscriptionRoute(subscriptionID)
		return
	}
	restoreActiveMLModelRoute(&route.PeerRoute)
	nwdafContext.UpdateMLModelMonitorSubscriptionRoute(route)
}

func (p *Processor) HandleDeleteMLModelMonitorSubscription(
	requestContext context.Context,
	subscriptionID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	p.mlModelMu.Lock()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	if nwdafContext.ConsumeMLModelDeletionRecord(
		nwdaf_context.MLModelResourceMonitorSubscription,
		subscriptionID,
	) {
		p.mlModelMu.Unlock()
		return noContentMLModelResponse(), nil
	}
	route, found := nwdafContext.GetMLModelMonitorSubscriptionRoute(subscriptionID)
	if !found {
		p.mlModelMu.Unlock()
		return nil, mlModelResourceNotFoundProblem("ML Model Monitor subscription", subscriptionID)
	}
	revision, problem := p.beginMLModelRouteOperationLocked(
		&route.PeerRoute,
		nwdaf_context.MLModelRouteDeleting,
	)
	if problem != nil {
		p.mlModelMu.Unlock()
		return nil, problem
	}
	if !nwdafContext.UpdateMLModelMonitorSubscriptionRoute(route) {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("could not reserve ML Model Monitor subscription deletion")
	}
	isPeer := route.PeerRoute.SelectedTarget != nil
	var generationLease *backend.GenerationLease
	if isPeer {
		if p.mlModelPeerConsumer == nil {
			restoreActiveMLModelRoute(&route.PeerRoute)
			nwdafContext.UpdateMLModelMonitorSubscriptionRoute(route)
			p.mlModelMu.Unlock()
			return nil, mlModelUnavailableProblem()
		}
	} else {
		var admitted bool
		generationLease, admitted = acquireBackend(p.anlfMLModelBackend, p.anlfAvailability)
		if !admitted {
			restoreActiveMLModelRoute(&route.PeerRoute)
			nwdafContext.UpdateMLModelMonitorSubscriptionRoute(route)
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
		response, operationErr = p.mlModelPeerConsumer.DeletePeerMLModelMonitorSubscription(
			requestContext,
			route.PeerRoute.PeerLocation,
		)
	} else {
		response, operationErr = p.anlfMLModelBackend.DeleteMLModelMonitorSubscription(
			requestContext,
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
	current, exists := nwdafContext.GetMLModelMonitorSubscriptionRoute(subscriptionID)
	if !exists || !mlModelRouteOperationCurrent(
		current.PeerRoute,
		nwdaf_context.MLModelRouteDeleting,
		revision,
	) {
		_, resetDeleted := nwdafContext.GetMLModelDeletionRecord(
			nwdaf_context.MLModelResourceMonitorSubscription,
			subscriptionID,
		)
		p.mlModelMu.Unlock()
		if !exists && resetDeleted {
			return noContentMLModelResponse(), nil
		}
		return nil, mlModelUnavailableProblem()
	}
	if terminal {
		nwdafContext.DeleteMLModelMonitorSubscriptionRoute(subscriptionID)
	} else {
		restoreActiveMLModelRoute(&current.PeerRoute)
		nwdafContext.UpdateMLModelMonitorSubscriptionRoute(current)
	}
	p.mlModelMu.Unlock()

	if terminal {
		if !isPeer {
			p.anlfAvailability.Refresh()
		}
		if missingDestination || response == nil || response.StatusCode == http.StatusNotFound {
			return noContentMLModelResponse(), nil
		}
		return response, nil
	}
	if operationErr != nil {
		if isPeer {
			return nil, p.mlModelPeerProblem(operationErr)
		}
		return nil, p.mlModelBackendProblem(operationErr, p.anlfAvailability)
	}
	return nil, mlModelBadGatewayProblem("ML Model Monitor destination returned an invalid delete response")
}

func (p *Processor) anlfCallbackURI(path string) string {
	if config := p.config(); config != nil {
		return config.GetAnlfServerURI() + path
	}
	return ""
}

func (p *Processor) mtlfCallbackURI(path string) string {
	if config := p.config(); config != nil {
		return config.GetMtlfServerURI() + path
	}
	return ""
}

func (p *Processor) publicResourceLocation(prefix, collection, resourceID string) string {
	config := p.config()
	if config == nil {
		return ""
	}
	return fmt.Sprintf("%s%s/%s/%s", config.GetSbiUri(), prefix, collection, resourceID)
}

func validateOutboundMLModelCallbackRoute(
	route nwdaf_context.MLModelPeerRoute,
	availability backendAvailability,
) *models.ProblemDetails {
	if route.Direction != nwdaf_context.MLModelRouteDirectionOutbound ||
		route.SelectedTarget == nil {
		return malformedMLModelProblem(errors.New(
			"callback resource does not identify an outbound peer route",
		))
	}
	if !mlModelRouteAcceptsCallback(route) ||
		availability == nil ||
		!availability.Usable() {
		return mlModelUnavailableProblem()
	}
	provider, ok := availability.(interface{ Snapshot() backend.Snapshot })
	if !ok {
		return mlModelUnavailableProblem()
	}
	snapshot := provider.Snapshot()
	if route.ProcessGeneration == "" ||
		snapshot.ProcessInstanceID == "" ||
		snapshot.ProcessInstanceID != route.ProcessGeneration {
		return mlModelUnavailableProblem()
	}
	return nil
}

func (p *Processor) mlModelBackendProblem(
	err error,
	availability backendAvailability,
) *models.ProblemDetails {
	var standardError *backend.StandardError
	if errors.As(err, &standardError) {
		return standardError.StandardProblemDetails()
	}
	var transportError *backend.TransportError
	if errors.As(err, &transportError) {
		if availability != nil {
			availability.MarkUnavailable("transport")
		}
		return mlModelUnavailableProblem()
	}
	var contractError *backend.ContractError
	if errors.As(err, &contractError) {
		return mlModelBadGatewayProblem(contractError.Detail)
	}
	return mlModelBadGatewayProblem("backend request failed")
}

func (p *Processor) mlModelCallbackProblem(err error) *models.ProblemDetails {
	var standardError *backend.StandardError
	if errors.As(err, &standardError) {
		return standardError.StandardProblemDetails()
	}
	var transportError *backend.TransportError
	if errors.As(err, &transportError) {
		return mlModelBadGatewayProblem("notification destination is unavailable")
	}
	var contractError *backend.ContractError
	if errors.As(err, &contractError) {
		return mlModelBadGatewayProblem(contractError.Detail)
	}
	return mlModelBadGatewayProblem("notification delivery failed")
}

func externalBodyForStatus(status int, body []byte) json.RawMessage {
	if status == http.StatusNoContent {
		return nil
	}
	return append(json.RawMessage(nil), body...)
}

func mlModelCallbackErrorStatuses() map[int]struct{} {
	return backend.ErrorStatuses(
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusLengthRequired,
		http.StatusRequestEntityTooLarge,
		http.StatusUnsupportedMediaType,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
	)
}

func malformedMLModelProblem(err error) *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusBadRequest,
		Title:  http.StatusText(http.StatusBadRequest),
		Cause:  "INVALID_MSG_FORMAT",
		Detail: err.Error(),
	}
}

func mlModelUnavailableProblem() *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusServiceUnavailable,
		Title:  http.StatusText(http.StatusServiceUnavailable),
		Detail: "requested ML model service capability is temporarily unavailable",
	}
}

func mlModelBadGatewayProblem(detail string) *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusBadGateway,
		Title:  http.StatusText(http.StatusBadGateway),
		Detail: detail,
	}
}

func mlModelInternalProblem(detail string) *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusInternalServerError,
		Title:  http.StatusText(http.StatusInternalServerError),
		Detail: detail,
	}
}

func mlModelResourceNotFoundProblem(kind, resourceID string) *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusNotFound,
		Title:  http.StatusText(http.StatusNotFound),
		Cause:  "RESOURCE_NOT_FOUND",
		Detail: fmt.Sprintf("%s %s was not found", kind, resourceID),
	}
}

func validateMonitorNotificationModels(
	representation []byte,
	notification *wire.MLModelMonitorNotification,
) error {
	subscription, err := wire.ParseMLModelMonitorSubscription(representation)
	if err != nil {
		return errors.New("monitor route has an invalid accepted representation")
	}
	allowed := make(map[int64]struct{}, len(subscription.ModelIDs))
	for _, modelID := range subscription.ModelIDs {
		allowed[modelID] = struct{}{}
	}
	for _, info := range notification.ModelAccuracyInfo {
		if info.ModelID == nil {
			return errors.New("monitor notification modelId is required")
		}
		if _, ok := allowed[*info.ModelID]; !ok {
			return errors.New("monitor notification modelId does not match subscription")
		}
	}
	for _, feedback := range notification.AnalyticsFeedback {
		for _, modelID := range feedback.ModelIDs {
			if _, ok := allowed[modelID]; !ok {
				return errors.New("monitor feedback modelId does not match subscription")
			}
		}
	}
	return nil
}
