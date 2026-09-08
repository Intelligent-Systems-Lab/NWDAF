package processor

import (
	"net/http"

	"github.com/free5gc/nwdaf/internal/backend"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

// beginMLModelRouteOperationLocked reserves a route transition while
// mlModelMu is held. External I/O must only start after the caller releases
// mlModelMu.
func (p *Processor) beginMLModelRouteOperationLocked(
	route *nwdaf_context.MLModelPeerRoute,
	state nwdaf_context.MLModelRouteLifecycle,
) (uint64, *models.ProblemDetails) {
	return p.beginMLModelRouteOperationFromLocked(
		route,
		state,
		nwdaf_context.MLModelRouteActive,
	)
}

func (p *Processor) beginMLModelRouteOperationFromLocked(
	route *nwdaf_context.MLModelPeerRoute,
	state nwdaf_context.MLModelRouteLifecycle,
	allowedStates ...nwdaf_context.MLModelRouteLifecycle,
) (uint64, *models.ProblemDetails) {
	if route == nil {
		return 0, mlModelUnavailableProblem()
	}
	allowed := false
	for _, candidate := range allowedStates {
		if route.LifecycleState == candidate {
			allowed = true
			break
		}
	}
	if !allowed {
		return 0, mlModelUnavailableProblem()
	}
	route.OperationRevision = p.nextMLModelOperationRevisionLocked()
	route.LifecycleState = state
	return route.OperationRevision, nil
}

func (p *Processor) nextMLModelOperationRevisionLocked() uint64 {
	p.mlModelOperationRevision++
	if p.mlModelOperationRevision == 0 {
		p.mlModelOperationRevision++
	}
	return p.mlModelOperationRevision
}

func mlModelRouteOperationCurrent(
	route nwdaf_context.MLModelPeerRoute,
	state nwdaf_context.MLModelRouteLifecycle,
	revision uint64,
) bool {
	return route.LifecycleState == state &&
		revision != 0 &&
		route.OperationRevision == revision
}

func restoreActiveMLModelRoute(route *nwdaf_context.MLModelPeerRoute) {
	if route == nil {
		return
	}
	route.LifecycleState = nwdaf_context.MLModelRouteActive
}

func mlModelRouteAcceptsCallback(route nwdaf_context.MLModelPeerRoute) bool {
	return route.LifecycleState == nwdaf_context.MLModelRouteActive ||
		route.LifecycleState == nwdaf_context.MLModelRouteReplacing
}

func noContentMLModelResponse() *backend.StandardResponse {
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}
}
