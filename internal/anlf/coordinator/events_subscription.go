package coordinator

import (
	"context"

	"github.com/free5gc/openapi/models"
)

func (a *Coordinator) CreateEventsSubscription(
	ctx context.Context,
	subscription *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, string, error) {
	if !a.backendUsable() || a.eventsBackend == nil {
		return nil, "", ErrBackendUnavailable
	}
	response, subscriptionID, err := a.eventsBackend.CreateEventsSubscription(ctx, subscription)
	if err != nil {
		a.reportBackendFailure(err)
	}
	return response, subscriptionID, err
}

func (a *Coordinator) ReplaceEventsSubscription(
	ctx context.Context,
	subscriptionID string,
	subscription *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, error) {
	if !a.backendUsable() || a.eventsBackend == nil {
		return nil, ErrBackendUnavailable
	}
	response, err := a.eventsBackend.ReplaceEventsSubscription(ctx, subscriptionID, subscription)
	if err != nil {
		a.reportBackendFailure(err)
	}
	return response, err
}

func (a *Coordinator) DeleteEventsSubscription(ctx context.Context, subscriptionID string) error {
	if !a.backendUsable() || a.eventsBackend == nil {
		return ErrBackendUnavailable
	}
	err := a.eventsBackend.DeleteEventsSubscription(ctx, subscriptionID)
	if err != nil {
		a.reportBackendFailure(err)
	}
	return err
}

func (a *Coordinator) RefreshBackendSync() {
	if refresher, ok := a.availability.(interface{ Refresh() }); ok {
		refresher.Refresh()
	}
}
