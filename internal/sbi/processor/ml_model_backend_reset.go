package processor

import (
	"context"
	"net/http"

	"github.com/free5gc/nwdaf/internal/backend"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
)

type mlModelResetCleanup struct {
	description string
	run         func(context.Context) (*backend.StandardResponse, error)
}

// ResetMLModelBackendGeneration discards every model relationship that depends on a
// failed backend process. Consumer-owned standard relationships get one
// best-effort DELETE; no process-loss retry worker or replay state is created.
func (p *Processor) ResetMLModelBackendGeneration(
	requestContext context.Context,
	kind backend.Kind,
	generation string,
) {
	if p == nil || generation == "" || p.nwdaf == nil || p.nwdaf.Context() == nil {
		return
	}
	p.mlModelMu.Lock()
	ctx := p.nwdaf.Context()
	cleanups := make([]mlModelResetCleanup, 0)

	for _, route := range ctx.GetAllMLModelProvisionSubscriptionRoutes() {
		if !modelRouteMatchesGeneration(
			kind,
			generation,
			provisionPrimaryBackend(route.PeerRoute),
			route.PeerRoute,
		) {
			continue
		}
		cleanup := p.provisionResetCleanup(kind, route)
		if cleanup.run != nil {
			cleanups = append(cleanups, cleanup)
		}
		ctx.DeleteMLModelProvisionSubscriptionRoute(route.SubscriptionID)
		ctx.TombstoneMLModelResource(nwdaf_context.MLModelDeletionRecord{
			ResourceID:        route.SubscriptionID,
			ProcessGeneration: generation,
			CleanupAttempted:  cleanup.run != nil,
		}, nwdaf_context.MLModelResourceProvisionSubscription)
	}

	for _, route := range ctx.GetAllMLModelMonitorRegistrationRoutes() {
		if !modelRouteMatchesGeneration(
			kind,
			generation,
			provisionPrimaryBackend(route.PeerRoute),
			route.PeerRoute,
		) {
			continue
		}
		cleanup := p.registrationResetCleanup(kind, route)
		if cleanup.run != nil {
			cleanups = append(cleanups, cleanup)
		}
		ctx.DeleteMLModelMonitorRegistrationRoute(route.RegistrationID)
		ctx.TombstoneMLModelResource(nwdaf_context.MLModelDeletionRecord{
			ResourceID:        route.RegistrationID,
			ProcessGeneration: generation,
			CleanupAttempted:  cleanup.run != nil,
		}, nwdaf_context.MLModelResourceMonitorRegistration)
	}

	for _, route := range ctx.GetAllMLModelMonitorSubscriptionRoutes() {
		if !modelRouteMatchesGeneration(
			kind,
			generation,
			monitorPrimaryBackend(route.PeerRoute),
			route.PeerRoute,
		) {
			continue
		}
		cleanup := p.monitorResetCleanup(kind, route)
		if cleanup.run != nil {
			cleanups = append(cleanups, cleanup)
		}
		ctx.DeleteMLModelMonitorSubscriptionRoute(route.SubscriptionID)
		ctx.TombstoneMLModelResource(nwdaf_context.MLModelDeletionRecord{
			ResourceID:        route.SubscriptionID,
			ProcessGeneration: generation,
			CleanupAttempted:  cleanup.run != nil,
		}, nwdaf_context.MLModelResourceMonitorSubscription)
	}

	if kind == backend.KindMTLF {
		for _, route := range ctx.GetAllMLModelTrainingSubscriptionRoutes() {
			if !modelRouteMatchesGeneration(
				kind,
				generation,
				backend.KindMTLF,
				route.PeerRoute,
			) {
				continue
			}
			cleanup := p.trainingResetCleanup(route)
			if cleanup.run != nil {
				cleanups = append(cleanups, cleanup)
			}
			ctx.DeleteMLModelTrainingSubscriptionRoute(route.SubscriptionID)
			ctx.TombstoneMLModelResource(nwdaf_context.MLModelDeletionRecord{
				ResourceID:        route.SubscriptionID,
				ProcessGeneration: generation,
				CleanupAttempted:  cleanup.run != nil,
			}, nwdaf_context.MLModelResourceTrainingSubscription)
		}
	}
	p.mlModelMu.Unlock()

	for _, cleanup := range cleanups {
		response, err := cleanup.run(requestContext)
		if cleanupResponseAccepted(response, err) {
			continue
		}
		logger.ProcLog.Warnf(
			"Backend-loss cleanup failed: operation=%s status=%d err=%v",
			cleanup.description,
			cleanupStatus(response),
			err,
		)
	}
}

func modelRouteMatchesGeneration(
	kind backend.Kind,
	generation string,
	primary backend.Kind,
	route nwdaf_context.MLModelPeerRoute,
) bool {
	return primary == kind && route.ProcessGeneration == generation ||
		route.RelatedBackend == kind && route.RelatedGeneration == generation
}

func provisionPrimaryBackend(route nwdaf_context.MLModelPeerRoute) backend.Kind {
	if route.Direction == nwdaf_context.MLModelRouteDirectionOutbound {
		return backend.KindAnLF
	}
	return backend.KindMTLF
}

func monitorPrimaryBackend(route nwdaf_context.MLModelPeerRoute) backend.Kind {
	if route.Direction == nwdaf_context.MLModelRouteDirectionOutbound {
		return backend.KindMTLF
	}
	return backend.KindAnLF
}

func (p *Processor) provisionResetCleanup(
	kind backend.Kind,
	route nwdaf_context.MLModelProvisionSubscriptionRoute,
) mlModelResetCleanup {
	if kind != backend.KindAnLF {
		return mlModelResetCleanup{}
	}
	if route.PeerRoute.SelectedTarget != nil && route.PeerRoute.PeerLocation != "" &&
		p.mlModelPeerConsumer != nil {
		return mlModelResetCleanup{
			description: "delete peer model provision " + route.SubscriptionID,
			run: func(ctx context.Context) (*backend.StandardResponse, error) {
				return p.mlModelPeerConsumer.DeletePeerMLModelProvision(
					ctx,
					route.PeerRoute.PeerLocation,
				)
			},
		}
	}
	if route.PeerRoute.Direction == nwdaf_context.MLModelRouteDirectionInbound &&
		route.PeerRoute.BackendResourceID != "" && p.mtlfMLModelBackend != nil {
		return mlModelResetCleanup{
			description: "delete local model provision " + route.SubscriptionID,
			run: func(ctx context.Context) (*backend.StandardResponse, error) {
				return p.mtlfMLModelBackend.DeleteMLModelProvisionSubscription(
					ctx,
					route.PeerRoute.BackendResourceID,
				)
			},
		}
	}
	return mlModelResetCleanup{}
}

func (p *Processor) registrationResetCleanup(
	kind backend.Kind,
	route nwdaf_context.MLModelMonitorRegistrationRoute,
) mlModelResetCleanup {
	if kind != backend.KindAnLF {
		return mlModelResetCleanup{}
	}
	if route.PeerRoute.SelectedTarget != nil && route.PeerRoute.PeerLocation != "" &&
		p.mlModelPeerConsumer != nil {
		return mlModelResetCleanup{
			description: "deregister peer model monitor " + route.RegistrationID,
			run: func(ctx context.Context) (*backend.StandardResponse, error) {
				return p.mlModelPeerConsumer.DeletePeerMLModelMonitorRegistration(
					ctx,
					route.PeerRoute.PeerLocation,
				)
			},
		}
	}
	if route.PeerRoute.Direction == nwdaf_context.MLModelRouteDirectionInbound &&
		route.PeerRoute.BackendResourceID != "" && p.mtlfMLModelBackend != nil {
		return mlModelResetCleanup{
			description: "deregister local model monitor " + route.RegistrationID,
			run: func(ctx context.Context) (*backend.StandardResponse, error) {
				return p.mtlfMLModelBackend.DeleteMLModelMonitorRegistration(
					ctx,
					route.PeerRoute.BackendResourceID,
				)
			},
		}
	}
	return mlModelResetCleanup{}
}

func (p *Processor) monitorResetCleanup(
	kind backend.Kind,
	route nwdaf_context.MLModelMonitorSubscriptionRoute,
) mlModelResetCleanup {
	if kind != backend.KindMTLF {
		return mlModelResetCleanup{}
	}
	if route.PeerRoute.SelectedTarget != nil && route.PeerRoute.PeerLocation != "" &&
		p.mlModelPeerConsumer != nil {
		return mlModelResetCleanup{
			description: "delete peer model monitor subscription " + route.SubscriptionID,
			run: func(ctx context.Context) (*backend.StandardResponse, error) {
				return p.mlModelPeerConsumer.DeletePeerMLModelMonitorSubscription(
					ctx,
					route.PeerRoute.PeerLocation,
				)
			},
		}
	}
	if route.PeerRoute.Direction == nwdaf_context.MLModelRouteDirectionInbound &&
		route.PeerRoute.BackendResourceID != "" && p.anlfMLModelBackend != nil {
		return mlModelResetCleanup{
			description: "delete local model monitor subscription " + route.SubscriptionID,
			run: func(ctx context.Context) (*backend.StandardResponse, error) {
				return p.anlfMLModelBackend.DeleteMLModelMonitorSubscription(
					ctx,
					route.PeerRoute.BackendResourceID,
				)
			},
		}
	}
	return mlModelResetCleanup{}
}

func (p *Processor) trainingResetCleanup(
	route nwdaf_context.MLModelTrainingSubscriptionRoute,
) mlModelResetCleanup {
	if route.PeerRoute.SelectedTarget == nil || route.PeerRoute.PeerLocation == "" ||
		p.mlModelPeerConsumer == nil {
		return mlModelResetCleanup{}
	}
	return mlModelResetCleanup{
		description: "delete peer model training subscription " + route.SubscriptionID,
		run: func(ctx context.Context) (*backend.StandardResponse, error) {
			return p.mlModelPeerConsumer.DeletePeerMLModelTraining(
				ctx,
				route.PeerRoute.PeerLocation,
			)
		},
	}
}

func cleanupResponseAccepted(response *backend.StandardResponse, err error) bool {
	if peerMissing(err) {
		return true
	}
	return err == nil && response != nil &&
		(response.StatusCode == http.StatusNoContent || response.StatusCode == http.StatusNotFound)
}

func cleanupStatus(response *backend.StandardResponse) int {
	if response == nil {
		return 0
	}
	return response.StatusCode
}
