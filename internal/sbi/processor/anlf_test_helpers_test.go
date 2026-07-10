package processor

import (
	"github.com/free5gc/nwdaf/internal/anlf/accuracy"
	"github.com/free5gc/nwdaf/internal/anlf/coordinator"
)

func newTestAnlfCoordinator(
	app NwdafApp,
	backend coordinator.BackendRuntimeClient,
	sender coordinator.ObservationSender,
) *coordinator.Coordinator {
	monitor := accuracy.NewMonitor(app)
	delivery := coordinator.NewObservationDelivery(app.CancelContext(), sender, nil)
	return coordinator.New(app, backend, monitor, delivery)
}
