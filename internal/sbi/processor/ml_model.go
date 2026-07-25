package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

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
) (*backend.StandardResponse, *models.ProblemDetails) {
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
	subscriptionID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	externalBody, err := wire.ReplaceStringField(response.Body, "notifUri", parsed.NotificationURI)
	if err != nil {
		return nil, mlModelBadGatewayProblem("MTLF backend returned an invalid provision representation")
	}
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil || !nwdafContext.AddMLModelProvisionSubscriptionRoute(
		nwdaf_context.MLModelProvisionSubscriptionRoute{
			SubscriptionID:             subscriptionID,
			AcceptedRepresentation:     externalBody,
			BackendRepresentation:      response.Body,
			Initiator:                  initiator,
			Destination:                destination,
			DestinationNotificationURI: parsed.NotificationURI,
			NotificationCorrelationID:  parsed.NotificationID,
		},
	) {
		if _, cleanupErr := p.mtlfMLModelBackend.DeleteMLModelProvisionSubscription(
			requestContext, subscriptionID,
		); cleanupErr != nil {
			logger.ProcLog.Errorf(
				"Failed to compensate ML Model Provision route collision: subscriptionId=%s err=%v",
				subscriptionID,
				cleanupErr,
			)
		}
		return nil, mlModelInternalProblem("could not record ML Model Provision subscription route")
	}
	p.mtlfAvailability.Refresh()
	return &backend.StandardResponse{
		StatusCode:  http.StatusCreated,
		Location:    p.publicResourceLocation(factory.NwdafMLModelProvisionResURIPrefix, "subscriptions", subscriptionID),
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
	subscriptionID := notifications[0].SubscriptionID
	if pathSubscriptionID != "" && pathSubscriptionID != subscriptionID {
		return nil, malformedMLModelProblem(errors.New(
			"notification subscriptionId does not match callback resource",
		))
	}
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil {
		return nil, mlModelInternalProblem("NWDAF context is unavailable")
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
		requestContext, subscriptionID, backendBody,
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
	if _, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(subscriptionID); !found {
		return nil, mlModelResourceNotFoundProblem("ML Model Provision subscription", subscriptionID)
	}
	if !backendUsable(p.mtlfMLModelBackend, p.mtlfAvailability) {
		return nil, mlModelUnavailableProblem()
	}
	response, err := p.mtlfMLModelBackend.DeleteMLModelProvisionSubscription(requestContext, subscriptionID)
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
) (*backend.StandardResponse, *models.ProblemDetails) {
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
	registrationID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil || !nwdafContext.AddMLModelMonitorRegistrationRoute(
		nwdaf_context.MLModelMonitorRegistrationRoute{
			RegistrationID:         registrationID,
			AcceptedRepresentation: response.Body,
			BackendRepresentation:  response.Body,
			Initiator:              initiator,
		},
	) {
		if _, cleanupErr := p.mtlfMLModelBackend.DeleteMLModelMonitorRegistration(
			requestContext, registrationID,
		); cleanupErr != nil {
			logger.ProcLog.Errorf(
				"Failed to compensate ML Model Monitor registration collision: registrationId=%s err=%v",
				registrationID,
				cleanupErr,
			)
		}
		return nil, mlModelInternalProblem("could not record ML Model Monitor registration route")
	}
	p.mtlfAvailability.Refresh()
	return &backend.StandardResponse{
		StatusCode:  http.StatusCreated,
		Location:    p.publicResourceLocation(factory.NwdafMLModelMonitorResURIPrefix, "registrations", registrationID),
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
	if _, found := nwdafContext.GetMLModelMonitorRegistrationRoute(registrationID); !found {
		return nil, mlModelResourceNotFoundProblem("ML Model Monitor registration", registrationID)
	}
	if !backendUsable(p.mtlfMLModelBackend, p.mtlfAvailability) {
		return nil, mlModelUnavailableProblem()
	}
	response, err := p.mtlfMLModelBackend.DeleteMLModelMonitorRegistration(requestContext, registrationID)
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
) (*backend.StandardResponse, *models.ProblemDetails) {
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
	subscriptionID, err := backend.ResourceIDFromLocation(response.Location)
	if err != nil {
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	externalBody, err := wire.ReplaceStringField(response.Body, "notificationUri", parsed.NotificationURI)
	if err != nil {
		return nil, mlModelBadGatewayProblem("AnLF backend returned an invalid monitor representation")
	}
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil || !nwdafContext.AddMLModelMonitorSubscriptionRoute(
		nwdaf_context.MLModelMonitorSubscriptionRoute{
			SubscriptionID:             subscriptionID,
			OwnerRegistrationID:        ownerRegistrationID,
			AcceptedRepresentation:     externalBody,
			BackendRepresentation:      response.Body,
			Destination:                destination,
			DestinationNotificationURI: parsed.NotificationURI,
			NotificationCorrelationID:  parsed.NotificationID,
		},
	) {
		if _, cleanupErr := p.anlfMLModelBackend.DeleteMLModelMonitorSubscription(
			requestContext, subscriptionID,
		); cleanupErr != nil {
			logger.ProcLog.Errorf(
				"Failed to compensate ML Model Monitor subscription collision: subscriptionId=%s err=%v",
				subscriptionID,
				cleanupErr,
			)
		}
		return nil, mlModelInternalProblem("could not record ML Model Monitor subscription route")
	}
	p.anlfAvailability.Refresh()
	return &backend.StandardResponse{
		StatusCode:  http.StatusCreated,
		Location:    p.publicResourceLocation(factory.NwdafMLModelMonitorResURIPrefix, "subscriptions", subscriptionID),
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
	if notification.NotificationID != route.NotificationCorrelationID {
		return nil, malformedMLModelProblem(errors.New(
			"notification correlation does not match monitor subscription",
		))
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
		requestContext, subscriptionID, backendBody,
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
	if _, found := nwdafContext.GetMLModelMonitorSubscriptionRoute(subscriptionID); !found {
		return nil, mlModelResourceNotFoundProblem("ML Model Monitor subscription", subscriptionID)
	}
	if !backendUsable(p.anlfMLModelBackend, p.anlfAvailability) {
		return nil, mlModelUnavailableProblem()
	}
	response, err := p.anlfMLModelBackend.DeleteMLModelMonitorSubscription(requestContext, subscriptionID)
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
