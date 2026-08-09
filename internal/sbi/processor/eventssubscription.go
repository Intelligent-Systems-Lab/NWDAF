package processor

import (
	"context"
	"net/http"

	"github.com/free5gc/openapi/models"
)

func (p *Processor) HandleCreateSubscription(
	requestCtx context.Context,
	req *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, string, *models.ProblemDetails) {
	lease, admitted := acquireBackend(p.eventsBackend, p.eventsAvailability)
	if !admitted {
		return nil, "", analyticsRuntimeUnavailableProblem()
	}
	if lease != nil {
		defer lease.Release()
	}
	return p.createBackendSubscription(requestCtx, req)
}

func (p *Processor) HandleUpdateSubscription(
	requestCtx context.Context,
	subscriptionID string,
	req *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, *models.ProblemDetails) {
	lease, admitted := acquireBackend(p.eventsBackend, p.eventsAvailability)
	if !admitted {
		return nil, analyticsRuntimeUnavailableProblem()
	}
	if lease != nil {
		defer lease.Release()
	}
	return p.replaceBackendSubscription(requestCtx, subscriptionID, req)
}

func (p *Processor) HandleDeleteSubscription(subscriptionID string) *models.ProblemDetails {
	ctx := p.nwdaf.Context()
	if ctx != nil && ctx.IsAnalyticsSubscriptionTombstoned(subscriptionID) {
		return nil
	}
	lease, admitted := acquireBackend(p.eventsBackend, p.eventsAvailability)
	if !admitted {
		return analyticsRuntimeUnavailableProblem()
	}
	if lease != nil {
		defer lease.Release()
	}
	return p.deleteBackendSubscription(subscriptionID)
}

func analyticsRuntimeUnavailableProblem() *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusServiceUnavailable,
		Title:  http.StatusText(http.StatusServiceUnavailable),
		Detail: "requested NWDAF capability is temporarily unavailable",
	}
}
