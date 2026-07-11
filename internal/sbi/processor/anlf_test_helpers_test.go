package processor

import (
	"context"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/anlf/coordinator"
)

type provisionBindingBackend struct{}

func (*provisionBindingBackend) ApplySubscriptionRuntime(
	context.Context,
	contract.ApplySubscriptionRuntimeRequest,
) (*contract.ApplySubscriptionRuntimeResponse, error) {
	return nil, nil
}

func (*provisionBindingBackend) ReleaseSubscriptionRuntime(context.Context, string) error { return nil }

func (*provisionBindingBackend) SyncObservationBindings(
	context.Context,
	string,
	contract.SyncObservationBindingsRequest,
) error {
	return nil
}

func (*provisionBindingBackend) SyncModelProvisionBinding(
	context.Context,
	string,
	contract.ModelProvisionBinding,
) error {
	return nil
}

func (*provisionBindingBackend) ApplyModelProvisionEvent(
	context.Context,
	contract.ModelProvisionEvent,
) (*contract.ModelProvisionEventResponse, error) {
	return &contract.ModelProvisionEventResponse{}, nil
}

func newTestAnlfCoordinator(
	app NwdafApp,
	backend coordinator.BackendRuntimeClient,
	sender coordinator.ObservationSender,
) *coordinator.Coordinator {
	delivery := coordinator.NewObservationDelivery(app.CancelContext(), sender, nil)
	return coordinator.New(app, backend, delivery)
}
