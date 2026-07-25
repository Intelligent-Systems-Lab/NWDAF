package processor

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

const eventsSubscriptionNotificationPath = "/internal/v1/events-subscription-notifications"

func (p *Processor) createBackendSubscription(
	requestCtx context.Context,
	req *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, string, *models.ProblemDetails) {
	p.eventsMu.Lock()
	defer p.eventsMu.Unlock()

	backendRequest, problem := p.internalizeEventsSubscription(req)
	if problem != nil {
		return nil, "", problem
	}
	response, subscriptionID, err := p.eventsBackend.CreateEventsSubscription(requestCtx, backendRequest)
	if err != nil {
		p.recordEventsBackendFailure(err)
		return nil, "", eventsSubscriptionBackendProblem(err)
	}
	if response == nil || subscriptionID == "" {
		return nil, "", analyticsRuntimeUnavailableProblem()
	}

	accepted := *response
	route := nwdaf_context.AnalyticsSubscriptionRoute{
		SubscriptionID:          subscriptionID,
		ExternalNotificationURI: req.NotificationURI,
		AcceptedSubscription:    accepted,
	}
	ctx := nwdaf_context.GetSelf()
	if ctx == nil || !ctx.AddAnalyticsSubscriptionRoute(route) {
		if cleanupErr := p.eventsBackend.DeleteEventsSubscription(requestCtx, subscriptionID); cleanupErr != nil {
			logger.ProcLog.Errorf(
				"Failed to compensate backend subscription after route collision: sub=%s err=%v",
				subscriptionID,
				cleanupErr,
			)
		}
		return nil, "", &models.ProblemDetails{
			Status: http.StatusInternalServerError,
			Title:  http.StatusText(http.StatusInternalServerError),
			Detail: "could not record the analytics subscription route",
		}
	}
	if p.eventsAvailability != nil {
		p.eventsAvailability.Refresh()
	}

	externalResponse := accepted
	externalResponse.NotificationURI = req.NotificationURI
	logger.ProcLog.Infof("CreateSubscription: routed sub=%s to AnLF backend", subscriptionID)
	return &externalResponse, subscriptionID, nil
}

func (p *Processor) replaceBackendSubscription(
	requestCtx context.Context,
	subscriptionID string,
	req *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, *models.ProblemDetails) {
	p.eventsMu.Lock()
	defer p.eventsMu.Unlock()

	ctx := nwdaf_context.GetSelf()
	if ctx == nil {
		return nil, analyticsRuntimeUnavailableProblem()
	}
	if _, exists := ctx.GetAnalyticsSubscriptionRoute(subscriptionID); !exists {
		return nil, subscriptionNotFoundProblem(subscriptionID)
	}
	backendRequest, problem := p.internalizeEventsSubscription(req)
	if problem != nil {
		return nil, problem
	}
	response, err := p.eventsBackend.ReplaceEventsSubscription(
		requestCtx,
		subscriptionID,
		backendRequest,
	)
	if err != nil {
		p.recordEventsBackendFailure(err)
		return nil, eventsSubscriptionBackendProblem(err)
	}
	if response == nil {
		return nil, analyticsRuntimeUnavailableProblem()
	}

	accepted := *response
	if !ctx.UpdateAnalyticsSubscriptionRoute(nwdaf_context.AnalyticsSubscriptionRoute{
		SubscriptionID:          subscriptionID,
		ExternalNotificationURI: req.NotificationURI,
		AcceptedSubscription:    accepted,
	}) {
		return nil, &models.ProblemDetails{
			Status: http.StatusInternalServerError,
			Title:  http.StatusText(http.StatusInternalServerError),
			Detail: "could not update the analytics subscription route",
		}
	}
	if p.eventsAvailability != nil {
		p.eventsAvailability.Refresh()
	}

	externalResponse := accepted
	externalResponse.NotificationURI = req.NotificationURI
	logger.ProcLog.Infof("UpdateSubscription: routed sub=%s to AnLF backend", subscriptionID)
	return &externalResponse, nil
}

func (p *Processor) deleteBackendSubscription(subscriptionID string) *models.ProblemDetails {
	p.eventsMu.Lock()
	defer p.eventsMu.Unlock()

	ctx := nwdaf_context.GetSelf()
	if ctx == nil {
		return analyticsRuntimeUnavailableProblem()
	}
	if _, exists := ctx.GetAnalyticsSubscriptionRoute(subscriptionID); !exists {
		return subscriptionNotFoundProblem(subscriptionID)
	}
	if err := p.eventsBackend.DeleteEventsSubscription(p.nwdaf.CancelContext(), subscriptionID); err != nil {
		p.recordEventsBackendFailure(err)
		return eventsSubscriptionBackendProblem(err)
	}
	if !ctx.DeleteAnalyticsSubscriptionRoute(subscriptionID) {
		return &models.ProblemDetails{
			Status: http.StatusInternalServerError,
			Title:  http.StatusText(http.StatusInternalServerError),
			Detail: "could not remove the analytics subscription route",
		}
	}
	if p.eventsAvailability != nil {
		p.eventsAvailability.Refresh()
	}
	logger.ProcLog.Infof("DeleteSubscription: routed sub=%s to AnLF backend", subscriptionID)
	return nil
}

func (p *Processor) internalizeEventsSubscription(
	req *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, *models.ProblemDetails) {
	if req == nil {
		return nil, &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Title:  http.StatusText(http.StatusBadRequest),
			Detail: "Events Subscription request body is required",
		}
	}
	cfg := p.config()
	if cfg == nil {
		return nil, analyticsRuntimeUnavailableProblem()
	}
	internalized := *req
	internalized.NotificationURI = cfg.GetAnlfServerURI() + eventsSubscriptionNotificationPath
	return &internalized, nil
}

func eventsSubscriptionBackendProblem(err error) *models.ProblemDetails {
	if err == nil {
		return nil
	}
	var standardError interface {
		StandardProblemDetails() *models.ProblemDetails
	}
	if errors.As(err, &standardError) {
		if problem := standardError.StandardProblemDetails(); problem != nil {
			return problem
		}
	}
	return analyticsRuntimeUnavailableProblem()
}

func (p *Processor) recordEventsBackendFailure(err error) {
	if err == nil || p.eventsAvailability == nil || errors.Is(err, context.Canceled) {
		return
	}
	var statusError interface{ HTTPStatusCode() int }
	if errors.As(err, &statusError) {
		status := statusError.HTTPStatusCode()
		if status >= http.StatusBadRequest && status < http.StatusInternalServerError {
			return
		}
	}
	p.eventsAvailability.MarkUnavailable("operation_failure")
}

func subscriptionNotFoundProblem(subscriptionID string) *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusNotFound,
		Title:  http.StatusText(http.StatusNotFound),
		Cause:  "SUBSCRIPTION_NOT_FOUND",
		Detail: fmt.Sprintf("Subscription %s not found", subscriptionID),
	}
}
