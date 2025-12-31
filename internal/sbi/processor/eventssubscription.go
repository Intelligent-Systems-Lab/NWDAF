package processor

import (
	"fmt"
	"net/http"

	"github.com/free5gc/openapi/models"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
)

// HandleCreateSubscription processes new subscription requests
func (p *Processor) HandleCreateSubscription(
	req *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, string, *models.ProblemDetails) {
	logger.ProcLog.Infof("Processing CreateSubscription request")

	// Validate request
	if problemDetails := p.validateSubscription(req); problemDetails != nil {
		return nil, "", problemDetails
	}

	// Generate subscription ID
	subscriptionId := nwdaf_context.NewSubscriptionId()

	// Create subscription object
	subscription := &nwdaf_context.Subscription{
		ID:              subscriptionId,
		NotificationURI: req.NotificationURI,
		NotifCorrId:     req.NotifCorrId,
		EventSubs:       req.EventSubscriptions,
		EvtReq:          req.EvtReq,
	}

	// Store subscription
	ctx := nwdaf_context.GetSelf()
	ctx.AddSubscription(subscription)

	logger.ProcLog.Infof("Subscription created: %s", subscriptionId)

	// Prepare response
	response := &models.NnwdafEventsSubscription{
		EventSubscriptions: req.EventSubscriptions,
		NotificationURI:    req.NotificationURI,
		NotifCorrId:        req.NotifCorrId,
		EvtReq:             req.EvtReq,
	}

	return response, subscriptionId, nil
}

// HandleUpdateSubscription processes subscription update requests
func (p *Processor) HandleUpdateSubscription(
	subscriptionId string,
	req *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, *models.ProblemDetails) {
	logger.ProcLog.Infof("Processing UpdateSubscription: %s", subscriptionId)

	ctx := nwdaf_context.GetSelf()

	// Check if subscription exists
	existing := ctx.GetSubscription(subscriptionId)
	if existing == nil {
		return nil, &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "SUBSCRIPTION_NOT_FOUND",
			Detail: fmt.Sprintf("Subscription %s not found", subscriptionId),
		}
	}

	// Validate request
	if problemDetails := p.validateSubscription(req); problemDetails != nil {
		return nil, problemDetails
	}

	// Update subscription
	subscription := &nwdaf_context.Subscription{
		ID:              subscriptionId,
		NotificationURI: req.NotificationURI,
		NotifCorrId:     req.NotifCorrId,
		EventSubs:       req.EventSubscriptions,
		EvtReq:          req.EvtReq,
		CreatedAt:       existing.CreatedAt,
	}

	ctx.UpdateSubscription(subscription)

	logger.ProcLog.Infof("Subscription updated: %s", subscriptionId)

	return req, nil
}

// HandleDeleteSubscription processes subscription deletion requests
func (p *Processor) HandleDeleteSubscription(subscriptionId string) *models.ProblemDetails {
	logger.ProcLog.Infof("Processing DeleteSubscription: %s", subscriptionId)

	ctx := nwdaf_context.GetSelf()

	if !ctx.DeleteSubscription(subscriptionId) {
		return &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "SUBSCRIPTION_NOT_FOUND",
			Detail: fmt.Sprintf("Subscription %s not found", subscriptionId),
		}
	}

	logger.ProcLog.Infof("Subscription deleted: %s", subscriptionId)
	return nil
}

// validateSubscription validates the subscription request
func (p *Processor) validateSubscription(req *models.NnwdafEventsSubscription) *models.ProblemDetails {
	if req == nil {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "Request body is empty",
		}
	}

	if len(req.EventSubscriptions) == 0 {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "eventSubscriptions is required",
		}
	}

	if req.NotificationURI == "" {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "notificationURI is required",
		}
	}

	// Validate each event subscription
	for i, eventSub := range req.EventSubscriptions {
		if eventSub.Event == "" {
			return &models.ProblemDetails{
				Status: http.StatusBadRequest,
				Cause:  "INVALID_REQUEST",
				Detail: fmt.Sprintf("event is required in eventSubscriptions[%d]", i),
			}
		}

		// Check if event type is supported
		if err := p.validateSupportedEvent(eventSub.Event); err != nil {
			return err
		}

		// For ABNORMAL_BEHAVIOUR, validate specific requirements
		if eventSub.Event == models.NwdafEvent_ABNORMAL_BEHAVIOUR {
			if err := p.validateAbnormalBehaviour(&eventSub); err != nil {
				return err
			}
		}
	}

	return nil
}

// Supported events list
var supportedEvents = []models.NwdafEvent{
	models.NwdafEvent_ABNORMAL_BEHAVIOUR,
}

// validateSupportedEvent checks if the event type is supported
func (p *Processor) validateSupportedEvent(event models.NwdafEvent) *models.ProblemDetails {
	for _, supported := range supportedEvents {
		if event == supported {
			return nil
		}
	}
	return &models.ProblemDetails{
		Status: http.StatusBadRequest,
		Cause:  "UNSUPPORTED_EVENT",
		Detail: fmt.Sprintf("Event type %s is not supported. Supported events: %v", event, supportedEvents),
	}
}

// Mobility-related exception IDs
var mobilityExceptionIds = []models.ExceptionId{
	models.ExceptionId_UNEXPECTED_UE_LOCATION,
	models.ExceptionId_PING_PONG_ACROSS_CELLS,
	models.ExceptionId_UNEXPECTED_WAKEUP,
	models.ExceptionId_UNEXPECTED_RADIO_LINK_FAILURES,
}

// Communication-related exception IDs
var communExceptionIds = []models.ExceptionId{
	models.ExceptionId_UNEXPECTED_LONG_LIVE_FLOW,
	models.ExceptionId_UNEXPECTED_LARGE_RATE_FLOW,
	models.ExceptionId_SUSPICION_OF_DDOS_ATTACK,
	models.ExceptionId_WRONG_DESTINATION_ADDRESS,
	models.ExceptionId_TOO_FREQUENT_SERVICE_ACCESS,
}

// validateAbnormalBehaviour validates ABNORMAL_BEHAVIOUR specific requirements
func (p *Processor) validateAbnormalBehaviour(
	eventSub *models.NwdafEventsSubscriptionEventSubscription,
) *models.ProblemDetails {
	// Must have target UE information
	if eventSub.TgtUe == nil {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "tgtUe is required for ABNORMAL_BEHAVIOUR",
		}
	}

	// tgtUe must have supis, intGroupIds, or anyUe=true
	hasTarget := len(eventSub.TgtUe.Supis) > 0 ||
		len(eventSub.TgtUe.IntGroupIds) > 0 ||
		eventSub.TgtUe.AnyUe

	if !hasTarget {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "tgtUe must contain supis, intGroupIds, or anyUe=true",
		}
	}

	// Check excepRequs and exptAnaType
	hasExcepRequs := len(eventSub.ExcepRequs) > 0
	hasExptAnaType := eventSub.ExptAnaType != ""

	// Must have either excepRequs or exptAnaType (but not both)
	if !hasExcepRequs && !hasExptAnaType {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "excepRequs or exptAnaType is required for ABNORMAL_BEHAVIOUR",
		}
	}

	// excepRequs and exptAnaType are mutually exclusive
	if hasExcepRequs && hasExptAnaType {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "excepRequs and exptAnaType cannot be provided together",
		}
	}

	// If anyUe=true, validate additional requirements
	if eventSub.TgtUe.AnyUe {
		if err := p.validateAnyUeRequirements(eventSub); err != nil {
			return err
		}
	}

	return nil
}

// validateAnyUeRequirements validates requirements when anyUe=true
func (p *Processor) validateAnyUeRequirements(
	eventSub *models.NwdafEventsSubscriptionEventSubscription,
) *models.ProblemDetails {
	isMobility := p.isMobilityRelated(eventSub)
	isCommun := p.isCommunRelated(eventSub)

	// For anyUe, cannot request both mobility and communication at the same time
	if isMobility && isCommun {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "when anyUe=true, cannot request both mobility and communication related analytics",
		}
	}

	// Check if has networkArea or snssais
	hasNetworkArea := eventSub.NetworkArea != nil
	hasSnssais := len(eventSub.Snssaia) > 0
	hasAppIds := len(eventSub.AppIds) > 0
	hasDnns := len(eventSub.Dnns) > 0

	// Mobility-related: requires networkArea or snssais
	if isMobility && !hasNetworkArea && !hasSnssais {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "when anyUe=true with mobility-related analytics, networkArea or snssais is required",
		}
	}

	// Communication-related: requires networkArea, appIds, dnns, or snssais
	if isCommun && !hasNetworkArea && !hasAppIds && !hasDnns && !hasSnssais {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "when anyUe=true with communication-related analytics, networkArea, appIds, dnns, or snssais is required",
		}
	}

	return nil
}

// isMobilityRelated checks if the subscription is mobility-related
func (p *Processor) isMobilityRelated(eventSub *models.NwdafEventsSubscriptionEventSubscription) bool {
	// Check exptAnaType
	if eventSub.ExptAnaType == models.ExpectedAnalyticsType_MOBILITY ||
		eventSub.ExptAnaType == models.ExpectedAnalyticsType_MOBILITY_AND_COMMUN {
		return true
	}

	// Check excepRequs
	for _, excep := range eventSub.ExcepRequs {
		for _, mobilityId := range mobilityExceptionIds {
			if excep.ExcepId == mobilityId {
				return true
			}
		}
	}

	return false
}

// isCommunRelated checks if the subscription is communication-related
func (p *Processor) isCommunRelated(eventSub *models.NwdafEventsSubscriptionEventSubscription) bool {
	// Check exptAnaType
	if eventSub.ExptAnaType == models.ExpectedAnalyticsType_COMMUN ||
		eventSub.ExptAnaType == models.ExpectedAnalyticsType_MOBILITY_AND_COMMUN {
		return true
	}

	// Check excepRequs
	for _, excep := range eventSub.ExcepRequs {
		for _, communId := range communExceptionIds {
			if excep.ExcepId == communId {
				return true
			}
		}
	}

	return false
}
