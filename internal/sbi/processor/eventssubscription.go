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

		// For ABNORMAL_BEHAVIOUR, validate specific requirements
		if eventSub.Event == models.NwdafEvent_ABNORMAL_BEHAVIOUR {
			if err := p.validateAbnormalBehaviour(&eventSub); err != nil {
				return err
			}
		}
	}

	return nil
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

	// Must have either excepRequs or exptAnaType
	hasExcepRequs := len(eventSub.ExcepRequs) > 0
	hasExptAnaType := eventSub.ExptAnaType != ""

	if !hasExcepRequs && !hasExptAnaType {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "excepRequs or exptAnaType is required for ABNORMAL_BEHAVIOUR",
		}
	}

	return nil
}
