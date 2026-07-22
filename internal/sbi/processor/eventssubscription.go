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
	if p.eventsBackend == nil {
		return nil, "", analyticsRuntimeUnavailableProblem()
	}
	return p.createBackendSubscription(requestCtx, req)
}

func (p *Processor) HandleUpdateSubscription(
	requestCtx context.Context,
	subscriptionID string,
	req *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, *models.ProblemDetails) {
	if p.eventsBackend == nil {
		return nil, analyticsRuntimeUnavailableProblem()
	}
	return p.replaceBackendSubscription(requestCtx, subscriptionID, req)
}

func (p *Processor) HandleDeleteSubscription(subscriptionID string) *models.ProblemDetails {
	if p.eventsBackend == nil {
		return analyticsRuntimeUnavailableProblem()
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
