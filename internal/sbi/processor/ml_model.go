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
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()

	parsed, err := wire.ParseMLModelProvisionSubscription(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	if !backendUsable(p.mtlfMLModelBackend, p.mtlfAvailability) {
		return nil, mlModelUnavailableProblem()
	}
	backendBody, err := wire.ReplaceStringField(
		body, "notifUri", p.mtlfCallbackURI(mlModelProvisionCallbackPath),
	)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	response, err := p.mtlfMLModelBackend.CreateMLModelProvisionSubscription(requestContext, backendBody)
	if err != nil {
		return nil, p.mlModelBackendProblem(err, p.mtlfAvailability)
	}
	if response == nil || response.StatusCode != http.StatusCreated {
		return nil, mlModelBadGatewayProblem("MTLF backend returned an invalid create response")
	}
	backendResourceID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	localRouteID := uuid.New().String()
	externalBody, err := wire.ReplaceStringField(response.Body, "notifUri", parsed.NotificationURI)
	if err != nil {
		return nil, mlModelBadGatewayProblem("MTLF backend returned an invalid provision representation")
	}
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil || !nwdafContext.AddMLModelProvisionSubscriptionRoute(
		nwdaf_context.MLModelProvisionSubscriptionRoute{
			SubscriptionID: localRouteID,
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:         nwdaf_context.MLModelRouteDirectionInbound,
				BackendLocation:   response.Location,
				BackendResourceID: backendResourceID,
				LifecycleState:    nwdaf_context.MLModelRouteActive,
				ProcessGeneration: p.backendGeneration(p.mtlfAvailability),
			},
			AcceptedRepresentation:     externalBody,
			BackendRepresentation:      response.Body,
			Initiator:                  initiator,
			Destination:                destination,
			DestinationNotificationURI: parsed.NotificationURI,
			NotificationCorrelationID:  parsed.NotificationID,
		},
	) {
		if _, cleanupErr := p.mtlfMLModelBackend.DeleteMLModelProvisionSubscription(
			requestContext, backendResourceID,
		); cleanupErr != nil {
			logger.ProcLog.Errorf(
				"Failed to compensate ML Model Provision route collision: subscriptionId=%s err=%v",
				backendResourceID,
				cleanupErr,
			)
		}
		return nil, mlModelInternalProblem("could not record ML Model Provision subscription route")
	}
	p.mtlfAvailability.Refresh()
	return &backend.StandardResponse{
		StatusCode:  http.StatusCreated,
		Location:    p.publicResourceLocation(factory.NwdafMLModelProvisionResURIPrefix, "subscriptions", localRouteID),
		ContentType: "application/json",
		Body:        externalBody,
	}, nil
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
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()

	notifications, err := wire.ParseMLModelProvisionNotifications(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	subscriptionID := notifications[0].SubscriptionID
	if pathSubscriptionID != "" {
		pathRoute, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(pathSubscriptionID)
		if !found {
			return nil, mlModelResourceNotFoundProblem(
				"ML Model Provision subscription",
				pathSubscriptionID,
			)
		}
		if problem := validateOutboundMLModelCallbackRoute(
			pathRoute.PeerRoute,
			p.anlfAvailability,
		); problem != nil {
			return nil, problem
		}
		if hint := peerResourceIDHint(pathRoute.PeerRoute.PeerLocation); hint != "" &&
			hint != subscriptionID {
			return nil, malformedMLModelProblem(errors.New(
				"notification subscriptionId does not match peer resource",
			))
		}
		if subscriptionID != pathSubscriptionID {
			body, err = wire.ReplaceProvisionNotificationSubscriptionID(body, pathSubscriptionID)
			if err != nil {
				return nil, malformedMLModelProblem(err)
			}
			notifications, err = wire.ParseMLModelProvisionNotifications(body)
			if err != nil {
				return nil, malformedMLModelProblem(err)
			}
		}
		subscriptionID = pathSubscriptionID
	} else {
		if _, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(subscriptionID); !found {
			backendRoute, backendFound := nwdafContext.
				FindMLModelProvisionSubscriptionRouteByBackendResourceID(subscriptionID)
			if !backendFound {
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
				return nil, malformedMLModelProblem(err)
			}
			notifications, err = wire.ParseMLModelProvisionNotifications(body)
			if err != nil {
				return nil, malformedMLModelProblem(err)
			}
			subscriptionID = backendRoute.SubscriptionID
		}
	}
	route, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(subscriptionID)
	if !found {
		return nil, mlModelResourceNotFoundProblem(
			"ML Model Provision subscription",
			subscriptionID,
		)
	}
	for _, notification := range notifications {
		if notification.SubscriptionID != subscriptionID {
			return nil, malformedMLModelProblem(errors.New(
				"notification array must reference one provision subscription",
			))
		}
		for _, event := range notification.EventNotifications {
			if route.NotificationCorrelationID != "" &&
				event.NotificationID != route.NotificationCorrelationID {
				return nil, malformedMLModelProblem(errors.New(
					"notification correlation does not match provision subscription",
				))
			}
		}
	}

	var response *backend.StandardResponse
	switch route.Destination {
	case nwdaf_context.MLModelRoutePartyAnLFBackend:
		if !backendUsable(p.anlfMLModelBackend, p.anlfAvailability) {
			return nil, mlModelUnavailableProblem()
		}
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
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()

	parsed, err := wire.ParseMLModelProvisionSubscription(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	route, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(subscriptionID)
	if !found {
		return nil, mlModelResourceNotFoundProblem("ML Model Provision subscription", subscriptionID)
	}
	if route.PeerRoute.SelectedTarget != nil {
		if p.mlModelPeerConsumer == nil {
			return nil, mlModelUnavailableProblem()
		}
		peerBody, replaceErr := wire.ReplaceStringField(
			body,
			"notifUri",
			p.publicMLModelCallbackURI("ml-model-provision", subscriptionID),
		)
		if replaceErr != nil {
			return nil, malformedMLModelProblem(replaceErr)
		}
		response, peerErr := p.mlModelPeerConsumer.ReplacePeerMLModelProvision(
			requestContext,
			route.PeerRoute.PeerLocation,
			peerBody,
		)
		if peerErr != nil {
			if peerMissing(peerErr) {
				nwdafContext.DeleteMLModelProvisionSubscriptionRoute(subscriptionID)
			}
			return nil, p.mlModelPeerProblem(peerErr)
		}
		externalBody := append(json.RawMessage(nil), body...)
		if response.StatusCode == http.StatusOK {
			externalBody, replaceErr = wire.ReplaceStringField(
				response.Body,
				"notifUri",
				parsed.NotificationURI,
			)
			if replaceErr != nil {
				return nil, mlModelBadGatewayProblem(
					"peer returned an invalid provision representation",
				)
			}
		}
		route.AcceptedRepresentation = externalBody
		route.BackendRepresentation = externalBody
		route.DestinationNotificationURI = parsed.NotificationURI
		route.NotificationCorrelationID = parsed.NotificationID
		if response.PermanentRedirectURI != "" {
			route.PeerRoute.PeerLocation = response.PermanentRedirectURI
		}
		if !nwdafContext.UpdateMLModelProvisionSubscriptionRoute(route) {
			return nil, mlModelInternalProblem("could not update remote provision route")
		}
		return &backend.StandardResponse{
			StatusCode:  response.StatusCode,
			ContentType: response.ContentType,
			Body:        externalBodyForStatus(response.StatusCode, externalBody),
		}, nil
	}
	if !backendUsable(p.mtlfMLModelBackend, p.mtlfAvailability) {
		return nil, mlModelUnavailableProblem()
	}
	backendBody, err := wire.ReplaceStringField(
		body, "notifUri", p.mtlfCallbackURI(mlModelProvisionCallbackPath),
	)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	response, err := p.mtlfMLModelBackend.ReplaceMLModelProvisionSubscription(
		requestContext, route.PeerRoute.BackendResourceID, backendBody,
	)
	if err != nil {
		return nil, p.mlModelBackendProblem(err, p.mtlfAvailability)
	}
	if response == nil {
		return nil, mlModelBadGatewayProblem("MTLF backend returned no replace response")
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		return nil, mlModelBadGatewayProblem("MTLF backend returned an invalid replace status")
	}
	externalBody := append(json.RawMessage(nil), body...)
	backendRepresentation := append(json.RawMessage(nil), backendBody...)
	if response.StatusCode == http.StatusOK {
		externalBody, err = wire.ReplaceStringField(response.Body, "notifUri", parsed.NotificationURI)
		if err != nil {
			return nil, mlModelBadGatewayProblem("MTLF backend returned an invalid provision representation")
		}
		backendRepresentation = append(json.RawMessage(nil), response.Body...)
	}
	route.AcceptedRepresentation = externalBody
	route.BackendRepresentation = backendRepresentation
	route.DestinationNotificationURI = parsed.NotificationURI
	route.NotificationCorrelationID = parsed.NotificationID
	if !nwdafContext.UpdateMLModelProvisionSubscriptionRoute(route) {
		return nil, mlModelInternalProblem("could not update ML Model Provision subscription route")
	}
	p.mtlfAvailability.Refresh()
	return &backend.StandardResponse{
		StatusCode:  response.StatusCode,
		ContentType: response.ContentType,
		Body:        externalBodyForStatus(response.StatusCode, externalBody),
	}, nil
}

func (p *Processor) HandleDeleteMLModelProvision(
	requestContext context.Context,
	subscriptionID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()

	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	route, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(subscriptionID)
	if !found {
		return nil, mlModelResourceNotFoundProblem("ML Model Provision subscription", subscriptionID)
	}
	if route.PeerRoute.SelectedTarget != nil {
		if p.mlModelPeerConsumer == nil {
			return nil, mlModelUnavailableProblem()
		}
		response, peerErr := p.mlModelPeerConsumer.DeletePeerMLModelProvision(
			requestContext,
			route.PeerRoute.PeerLocation,
		)
		if peerErr != nil && !peerMissing(peerErr) {
			return nil, p.mlModelPeerProblem(peerErr)
		}
		nwdafContext.DeleteMLModelProvisionSubscriptionRoute(subscriptionID)
		if response == nil {
			return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
		}
		return response, nil
	}
	if !backendUsable(p.mtlfMLModelBackend, p.mtlfAvailability) {
		return nil, mlModelUnavailableProblem()
	}
	response, err := p.mtlfMLModelBackend.DeleteMLModelProvisionSubscription(
		requestContext,
		route.PeerRoute.BackendResourceID,
	)
	if err != nil {
		return nil, p.mlModelBackendProblem(err, p.mtlfAvailability)
	}
	if response == nil {
		return nil, mlModelBadGatewayProblem("MTLF backend returned no delete response")
	}
	if response.StatusCode != http.StatusNoContent {
		return nil, mlModelBadGatewayProblem("MTLF backend returned an invalid delete status")
	}
	if !nwdafContext.DeleteMLModelProvisionSubscriptionRoute(subscriptionID) {
		return nil, mlModelInternalProblem("could not remove ML Model Provision subscription route")
	}
	p.mtlfAvailability.Refresh()
	return response, nil
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
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()

	if _, err := wire.ParseMLModelMonitorRegistration(body); err != nil {
		return nil, malformedMLModelProblem(err)
	}
	if !backendUsable(p.mtlfMLModelBackend, p.mtlfAvailability) {
		return nil, mlModelUnavailableProblem()
	}
	response, err := p.mtlfMLModelBackend.CreateMLModelMonitorRegistration(requestContext, body)
	if err != nil {
		return nil, p.mlModelBackendProblem(err, p.mtlfAvailability)
	}
	if response == nil || response.StatusCode != http.StatusCreated {
		return nil, mlModelBadGatewayProblem("MTLF backend returned an invalid registration response")
	}
	backendResourceID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	localRouteID := uuid.New().String()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil || !nwdafContext.AddMLModelMonitorRegistrationRoute(
		nwdaf_context.MLModelMonitorRegistrationRoute{
			RegistrationID: localRouteID,
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:         nwdaf_context.MLModelRouteDirectionInbound,
				BackendLocation:   response.Location,
				BackendResourceID: backendResourceID,
				LifecycleState:    nwdaf_context.MLModelRouteActive,
				ProcessGeneration: p.backendGeneration(p.mtlfAvailability),
			},
			AcceptedRepresentation: response.Body,
			BackendRepresentation:  response.Body,
			Initiator:              initiator,
		},
	) {
		if _, cleanupErr := p.mtlfMLModelBackend.DeleteMLModelMonitorRegistration(
			requestContext, backendResourceID,
		); cleanupErr != nil {
			logger.ProcLog.Errorf(
				"Failed to compensate ML Model Monitor registration collision: registrationId=%s err=%v",
				backendResourceID,
				cleanupErr,
			)
		}
		return nil, mlModelInternalProblem("could not record ML Model Monitor registration route")
	}
	p.mtlfAvailability.Refresh()
	return &backend.StandardResponse{
		StatusCode:  http.StatusCreated,
		Location:    p.publicResourceLocation(factory.NwdafMLModelMonitorResURIPrefix, "registrations", localRouteID),
		ContentType: "application/json",
		Body:        append(json.RawMessage(nil), response.Body...),
	}, nil
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
	defer p.mlModelMu.Unlock()

	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	route, found := nwdafContext.GetMLModelMonitorRegistrationRoute(registrationID)
	if !found {
		return nil, mlModelResourceNotFoundProblem("ML Model Monitor registration", registrationID)
	}
	if route.PeerRoute.SelectedTarget != nil {
		if p.mlModelPeerConsumer == nil {
			return nil, mlModelUnavailableProblem()
		}
		response, peerErr := p.mlModelPeerConsumer.DeletePeerMLModelMonitorRegistration(
			requestContext,
			route.PeerRoute.PeerLocation,
		)
		if peerErr != nil && !peerMissing(peerErr) {
			return nil, p.mlModelPeerProblem(peerErr)
		}
		nwdafContext.DeleteMLModelMonitorRegistrationRoute(registrationID)
		if response == nil {
			return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
		}
		return response, nil
	}
	if !backendUsable(p.mtlfMLModelBackend, p.mtlfAvailability) {
		return nil, mlModelUnavailableProblem()
	}
	backendResourceID := route.PeerRoute.BackendResourceID
	response, err := p.mtlfMLModelBackend.DeleteMLModelMonitorRegistration(
		requestContext,
		backendResourceID,
	)
	if err != nil {
		return nil, p.mlModelBackendProblem(err, p.mtlfAvailability)
	}
	if response == nil {
		return nil, mlModelBadGatewayProblem("MTLF backend returned no registration delete response")
	}
	if response.StatusCode != http.StatusNoContent {
		return nil, mlModelBadGatewayProblem("MTLF backend returned an invalid registration delete status")
	}
	if !nwdafContext.DeleteMLModelMonitorRegistrationRoute(registrationID) {
		return nil, mlModelInternalProblem("could not remove ML Model Monitor registration route")
	}
	p.mtlfAvailability.Refresh()
	return response, nil
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
			normalizedOwnerID, err := normalizeMonitorOwnerRegistrationID(
				nwdafContext,
				ownerRegistrationID,
			)
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
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()

	parsed, err := wire.ParseMLModelMonitorSubscription(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	if !backendUsable(p.anlfMLModelBackend, p.anlfAvailability) {
		return nil, mlModelUnavailableProblem()
	}
	backendBody, err := wire.ReplaceStringField(
		body, "notificationUri", p.anlfCallbackURI(mlModelMonitorCallbackPath),
	)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	response, err := p.anlfMLModelBackend.CreateMLModelMonitorSubscription(requestContext, backendBody)
	if err != nil {
		return nil, p.mlModelBackendProblem(err, p.anlfAvailability)
	}
	if response == nil || response.StatusCode != http.StatusCreated {
		return nil, mlModelBadGatewayProblem("AnLF backend returned an invalid subscription response")
	}
	backendResourceID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	localRouteID := uuid.New().String()
	externalBody, err := wire.ReplaceStringField(response.Body, "notificationUri", parsed.NotificationURI)
	if err != nil {
		return nil, mlModelBadGatewayProblem("AnLF backend returned an invalid monitor representation")
	}
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil || !nwdafContext.AddMLModelMonitorSubscriptionRoute(
		nwdaf_context.MLModelMonitorSubscriptionRoute{
			SubscriptionID: localRouteID,
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:         nwdaf_context.MLModelRouteDirectionInbound,
				BackendLocation:   response.Location,
				BackendResourceID: backendResourceID,
				LifecycleState:    nwdaf_context.MLModelRouteActive,
				ProcessGeneration: p.backendGeneration(p.anlfAvailability),
			},
			OwnerRegistrationID:        ownerRegistrationID,
			AcceptedRepresentation:     externalBody,
			BackendRepresentation:      response.Body,
			Destination:                destination,
			DestinationNotificationURI: parsed.NotificationURI,
			NotificationCorrelationID:  parsed.NotificationID,
		},
	) {
		if _, cleanupErr := p.anlfMLModelBackend.DeleteMLModelMonitorSubscription(
			requestContext, backendResourceID,
		); cleanupErr != nil {
			logger.ProcLog.Errorf(
				"Failed to compensate ML Model Monitor subscription collision: subscriptionId=%s err=%v",
				backendResourceID,
				cleanupErr,
			)
		}
		return nil, mlModelInternalProblem("could not record ML Model Monitor subscription route")
	}
	p.anlfAvailability.Refresh()
	return &backend.StandardResponse{
		StatusCode:  http.StatusCreated,
		Location:    p.publicResourceLocation(factory.NwdafMLModelMonitorResURIPrefix, "subscriptions", localRouteID),
		ContentType: "application/json",
		Body:        externalBody,
	}, nil
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
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()

	notification, err := wire.ParseMLModelMonitorNotification(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
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
			return nil, problem
		}
	}
	if notification.NotificationID != route.NotificationCorrelationID {
		return nil, malformedMLModelProblem(errors.New(
			"notification correlation does not match monitor subscription",
		))
	}
	if validationErr := validateMonitorNotificationModels(
		route.AcceptedRepresentation,
		notification,
	); validationErr != nil {
		return nil, malformedMLModelProblem(validationErr)
	}

	var response *backend.StandardResponse
	switch route.Destination {
	case nwdaf_context.MLModelRoutePartyMTLFBackend:
		if !backendUsable(p.mtlfMLModelBackend, p.mtlfAvailability) {
			return nil, mlModelUnavailableProblem()
		}
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
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()

	parsed, err := wire.ParseMLModelMonitorSubscription(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	route, found := nwdafContext.GetMLModelMonitorSubscriptionRoute(subscriptionID)
	if !found {
		return nil, mlModelResourceNotFoundProblem("ML Model Monitor subscription", subscriptionID)
	}
	if route.PeerRoute.SelectedTarget != nil {
		if p.mlModelPeerConsumer == nil {
			return nil, mlModelUnavailableProblem()
		}
		peerBody, replaceErr := wire.ReplaceStringField(
			body,
			"notificationUri",
			p.publicMLModelCallbackURI("ml-model-monitor", subscriptionID),
		)
		if replaceErr != nil {
			return nil, malformedMLModelProblem(replaceErr)
		}
		response, peerErr := p.mlModelPeerConsumer.ReplacePeerMLModelMonitorSubscription(
			requestContext,
			route.PeerRoute.PeerLocation,
			peerBody,
		)
		if peerErr != nil {
			if peerMissing(peerErr) {
				nwdafContext.DeleteMLModelMonitorSubscriptionRoute(subscriptionID)
			}
			return nil, p.mlModelPeerProblem(peerErr)
		}
		externalBody := append(json.RawMessage(nil), body...)
		if response.StatusCode == http.StatusOK {
			externalBody, replaceErr = wire.ReplaceStringField(
				response.Body,
				"notificationUri",
				parsed.NotificationURI,
			)
			if replaceErr != nil {
				return nil, mlModelBadGatewayProblem(
					"peer returned an invalid monitor representation",
				)
			}
		}
		route.AcceptedRepresentation = externalBody
		route.BackendRepresentation = externalBody
		route.DestinationNotificationURI = parsed.NotificationURI
		route.NotificationCorrelationID = parsed.NotificationID
		if response.PermanentRedirectURI != "" {
			route.PeerRoute.PeerLocation = response.PermanentRedirectURI
		}
		if !nwdafContext.UpdateMLModelMonitorSubscriptionRoute(route) {
			return nil, mlModelInternalProblem("could not update remote monitor route")
		}
		return &backend.StandardResponse{
			StatusCode:  response.StatusCode,
			ContentType: response.ContentType,
			Body:        externalBodyForStatus(response.StatusCode, externalBody),
		}, nil
	}
	if !backendUsable(p.anlfMLModelBackend, p.anlfAvailability) {
		return nil, mlModelUnavailableProblem()
	}
	backendBody, err := wire.ReplaceStringField(
		body, "notificationUri", p.anlfCallbackURI(mlModelMonitorCallbackPath),
	)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	response, err := p.anlfMLModelBackend.ReplaceMLModelMonitorSubscription(
		requestContext, route.PeerRoute.BackendResourceID, backendBody,
	)
	if err != nil {
		return nil, p.mlModelBackendProblem(err, p.anlfAvailability)
	}
	if response == nil {
		return nil, mlModelBadGatewayProblem("AnLF backend returned no replace response")
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		return nil, mlModelBadGatewayProblem("AnLF backend returned an invalid replace status")
	}
	externalBody := append(json.RawMessage(nil), body...)
	backendRepresentation := append(json.RawMessage(nil), backendBody...)
	if response.StatusCode == http.StatusOK {
		externalBody, err = wire.ReplaceStringField(response.Body, "notificationUri", parsed.NotificationURI)
		if err != nil {
			return nil, mlModelBadGatewayProblem("AnLF backend returned an invalid monitor representation")
		}
		backendRepresentation = append(json.RawMessage(nil), response.Body...)
	}
	route.AcceptedRepresentation = externalBody
	route.BackendRepresentation = backendRepresentation
	route.DestinationNotificationURI = parsed.NotificationURI
	route.NotificationCorrelationID = parsed.NotificationID
	if !nwdafContext.UpdateMLModelMonitorSubscriptionRoute(route) {
		return nil, mlModelInternalProblem("could not update ML Model Monitor subscription route")
	}
	p.anlfAvailability.Refresh()
	return &backend.StandardResponse{
		StatusCode:  response.StatusCode,
		ContentType: response.ContentType,
		Body:        externalBodyForStatus(response.StatusCode, externalBody),
	}, nil
}

func (p *Processor) HandleDeleteMLModelMonitorSubscription(
	requestContext context.Context,
	subscriptionID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()

	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
	}
	route, found := nwdafContext.GetMLModelMonitorSubscriptionRoute(subscriptionID)
	if !found {
		return nil, mlModelResourceNotFoundProblem("ML Model Monitor subscription", subscriptionID)
	}
	if route.PeerRoute.SelectedTarget != nil {
		if p.mlModelPeerConsumer == nil {
			return nil, mlModelUnavailableProblem()
		}
		response, peerErr := p.mlModelPeerConsumer.DeletePeerMLModelMonitorSubscription(
			requestContext,
			route.PeerRoute.PeerLocation,
		)
		if peerErr != nil && !peerMissing(peerErr) {
			return nil, p.mlModelPeerProblem(peerErr)
		}
		nwdafContext.DeleteMLModelMonitorSubscriptionRoute(subscriptionID)
		if response == nil {
			return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
		}
		return response, nil
	}
	if !backendUsable(p.anlfMLModelBackend, p.anlfAvailability) {
		return nil, mlModelUnavailableProblem()
	}
	response, err := p.anlfMLModelBackend.DeleteMLModelMonitorSubscription(
		requestContext,
		route.PeerRoute.BackendResourceID,
	)
	if err != nil {
		return nil, p.mlModelBackendProblem(err, p.anlfAvailability)
	}
	if response == nil {
		return nil, mlModelBadGatewayProblem("AnLF backend returned no delete response")
	}
	if response.StatusCode != http.StatusNoContent {
		return nil, mlModelBadGatewayProblem("AnLF backend returned an invalid delete status")
	}
	if !nwdafContext.DeleteMLModelMonitorSubscriptionRoute(subscriptionID) {
		return nil, mlModelInternalProblem("could not remove ML Model Monitor subscription route")
	}
	p.anlfAvailability.Refresh()
	return response, nil
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

func backendUsable(client any, availability backendAvailability) bool {
	return client != nil && availability != nil && availability.Usable()
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
	if route.LifecycleState != nwdaf_context.MLModelRouteActive ||
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
