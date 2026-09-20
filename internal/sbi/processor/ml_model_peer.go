package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/free5gc/nwdaf/internal/backend"
	wire "github.com/free5gc/nwdaf/internal/compat/mlmodel"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

func (p *Processor) handleCreateRemoteMLModelProvision(
	requestContext context.Context,
	body []byte,
	target backend.SelectedTarget,
) (*backend.StandardResponse, *models.ProblemDetails) {
	parsed, err := wire.ParseMLModelProvisionSubscription(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	if p.mlModelPeerConsumer == nil {
		return nil, mlModelUnavailableProblem()
	}
	localRouteID := uuid.New().String()
	peerBody, err := wire.ReplaceStringField(
		body,
		"notifUri",
		p.publicMLModelCallbackURI("ml-model-provision", localRouteID),
	)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	nwdafContext := p.nwdaf.Context()
	p.mlModelMu.Lock()
	revision := p.nextMLModelOperationRevisionLocked()
	if nwdafContext == nil || !nwdafContext.AddMLModelProvisionSubscriptionRoute(
		nwdaf_context.MLModelProvisionSubscriptionRoute{
			SubscriptionID: localRouteID,
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:         nwdaf_context.MLModelRouteDirectionOutbound,
				SelectedTarget:    copySelectedTarget(target),
				LifecycleState:    nwdaf_context.MLModelRouteCreating,
				OperationRevision: revision,
				ProcessGeneration: p.backendGeneration(p.anlfAvailability),
			},
			Initiator:                  nwdaf_context.MLModelRoutePartyAnLFBackend,
			Destination:                nwdaf_context.MLModelRoutePartyAnLFBackend,
			DestinationNotificationURI: parsed.NotificationURI,
			NotificationCorrelationID:  parsed.NotificationID,
		},
	) {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("could not reserve remote ML Model Provision route")
	}
	p.mlModelMu.Unlock()
	response, peerErr := p.mlModelPeerConsumer.CreatePeerMLModelProvision(
		requestContext,
		target,
		peerBody,
	)
	if peerErr != nil {
		p.finishFailedPeerProvisionCreate(requestContext, localRouteID, revision, response)
		return nil, p.mlModelPeerProblem(peerErr)
	}
	peerLocation, err := resolvedPeerLocation(response)
	if err != nil {
		p.finishFailedPeerProvisionCreate(requestContext, localRouteID, revision, response)
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	backendView, err := wire.ReplaceStringField(response.Body, "notifUri", parsed.NotificationURI)
	if err != nil {
		p.finishFailedPeerProvisionCreate(requestContext, localRouteID, revision, response)
		return nil, mlModelBadGatewayProblem("peer returned an invalid provision representation")
	}
	p.mlModelMu.Lock()
	route, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(localRouteID)
	if !found || !mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		p.mlModelMu.Unlock()
		p.finishFailedPeerProvisionCreate(requestContext, localRouteID, revision, response)
		return nil, mlModelUnavailableProblem()
	}
	route.PeerRoute.PeerLocation = peerLocation
	restoreActiveMLModelRoute(&route.PeerRoute)
	route.AcceptedRepresentation = backendView
	route.BackendRepresentation = backendView
	if !nwdafContext.UpdateMLModelProvisionSubscriptionRoute(route) {
		p.mlModelMu.Unlock()
		p.finishFailedPeerProvisionCreate(requestContext, localRouteID, revision, response)
		return nil, mlModelInternalProblem("could not record remote ML Model Provision route")
	}
	p.mlModelMu.Unlock()
	return &backend.StandardResponse{
		StatusCode: http.StatusCreated,
		Location: p.privateMLModelResourceLocation(
			nwdaf_context.MLModelRoutePartyAnLFBackend,
			"ml-model-provision/subscriptions",
			localRouteID,
		),
		ContentType: "application/json",
		Body:        backendView,
	}, nil
}

func (p *Processor) handleCreateRemoteMLModelMonitorRegistration(
	requestContext context.Context,
	body []byte,
	target backend.SelectedTarget,
) (*backend.StandardResponse, *models.ProblemDetails) {
	if _, err := wire.ParseMLModelMonitorRegistration(body); err != nil {
		return nil, malformedMLModelProblem(err)
	}
	if p.mlModelPeerConsumer == nil {
		return nil, mlModelUnavailableProblem()
	}
	localRouteID := uuid.New().String()
	nwdafContext := p.nwdaf.Context()
	p.mlModelMu.Lock()
	revision := p.nextMLModelOperationRevisionLocked()
	if nwdafContext == nil || !nwdafContext.AddMLModelMonitorRegistrationRoute(
		nwdaf_context.MLModelMonitorRegistrationRoute{
			RegistrationID: localRouteID,
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:         nwdaf_context.MLModelRouteDirectionOutbound,
				SelectedTarget:    copySelectedTarget(target),
				LifecycleState:    nwdaf_context.MLModelRouteCreating,
				OperationRevision: revision,
				ProcessGeneration: p.backendGeneration(p.anlfAvailability),
			},
			Initiator: nwdaf_context.MLModelRoutePartyAnLFBackend,
		},
	) {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("could not reserve remote ML Model Monitor registration")
	}
	p.mlModelMu.Unlock()
	response, peerErr := p.mlModelPeerConsumer.CreatePeerMLModelMonitorRegistration(
		requestContext,
		target,
		body,
	)
	if peerErr != nil {
		p.finishFailedPeerRegistrationCreate(requestContext, localRouteID, revision, response)
		return nil, p.mlModelPeerProblem(peerErr)
	}
	peerLocation, err := resolvedPeerLocation(response)
	if err != nil {
		p.finishFailedPeerRegistrationCreate(requestContext, localRouteID, revision, response)
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	p.mlModelMu.Lock()
	route, found := nwdafContext.GetMLModelMonitorRegistrationRoute(localRouteID)
	if !found || !mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		p.mlModelMu.Unlock()
		p.finishFailedPeerRegistrationCreate(requestContext, localRouteID, revision, response)
		return nil, mlModelUnavailableProblem()
	}
	route.PeerRoute.PeerLocation = peerLocation
	restoreActiveMLModelRoute(&route.PeerRoute)
	route.AcceptedRepresentation = append(json.RawMessage(nil), response.Body...)
	route.BackendRepresentation = append(json.RawMessage(nil), response.Body...)
	if !nwdafContext.UpdateMLModelMonitorRegistrationRoute(route) {
		p.mlModelMu.Unlock()
		p.finishFailedPeerRegistrationCreate(requestContext, localRouteID, revision, response)
		return nil, mlModelInternalProblem("could not record remote ML Model Monitor registration")
	}
	p.mlModelMu.Unlock()
	return &backend.StandardResponse{
		StatusCode: http.StatusCreated,
		Location: p.privateMLModelResourceLocation(
			nwdaf_context.MLModelRoutePartyAnLFBackend,
			"ml-model-monitor/registrations",
			localRouteID,
		),
		ContentType: "application/json",
		Body:        append(json.RawMessage(nil), response.Body...),
	}, nil
}

func (p *Processor) handleCreateRemoteMLModelMonitorSubscription(
	requestContext context.Context,
	body []byte,
	ownerRegistrationID string,
	target backend.SelectedTarget,
) (*backend.StandardResponse, *models.ProblemDetails) {
	parsed, err := wire.ParseMLModelMonitorSubscription(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	if p.mlModelPeerConsumer == nil {
		return nil, mlModelUnavailableProblem()
	}
	nwdafContext := p.nwdaf.Context()
	localRouteID := uuid.New().String()
	peerBody, err := wire.ReplaceStringField(
		body,
		"notificationUri",
		p.publicMLModelCallbackURI("ml-model-monitor", localRouteID),
	)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	p.mlModelMu.Lock()
	normalizedOwnerID, normalizeErr := normalizeMonitorOwnerRegistrationID(
		nwdafContext,
		ownerRegistrationID,
	)
	if normalizeErr != nil {
		p.mlModelMu.Unlock()
		return nil, malformedMLModelProblem(normalizeErr)
	}
	revision := p.nextMLModelOperationRevisionLocked()
	if nwdafContext == nil || !nwdafContext.AddMLModelMonitorSubscriptionRoute(
		nwdaf_context.MLModelMonitorSubscriptionRoute{
			SubscriptionID: localRouteID,
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:         nwdaf_context.MLModelRouteDirectionOutbound,
				SelectedTarget:    copySelectedTarget(target),
				LifecycleState:    nwdaf_context.MLModelRouteCreating,
				OperationRevision: revision,
				ProcessGeneration: p.backendGeneration(p.mtlfAvailability),
			},
			OwnerRegistrationID:        normalizedOwnerID,
			Destination:                nwdaf_context.MLModelRoutePartyMTLFBackend,
			DestinationNotificationURI: parsed.NotificationURI,
			NotificationCorrelationID:  parsed.NotificationID,
		},
	) {
		p.mlModelMu.Unlock()
		return nil, mlModelInternalProblem("could not reserve remote ML Model Monitor subscription")
	}
	p.mlModelMu.Unlock()
	response, peerErr := p.mlModelPeerConsumer.CreatePeerMLModelMonitorSubscription(
		requestContext,
		target,
		peerBody,
	)
	if peerErr != nil {
		p.finishFailedPeerMonitorCreate(requestContext, localRouteID, revision, response)
		return nil, p.mlModelPeerProblem(peerErr)
	}
	peerLocation, err := resolvedPeerLocation(response)
	if err != nil {
		p.finishFailedPeerMonitorCreate(requestContext, localRouteID, revision, response)
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	backendView, err := wire.ReplaceStringField(
		response.Body,
		"notificationUri",
		parsed.NotificationURI,
	)
	if err != nil {
		p.finishFailedPeerMonitorCreate(requestContext, localRouteID, revision, response)
		return nil, mlModelBadGatewayProblem("peer returned an invalid monitor representation")
	}
	p.mlModelMu.Lock()
	route, found := nwdafContext.GetMLModelMonitorSubscriptionRoute(localRouteID)
	if !found || !mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		p.mlModelMu.Unlock()
		p.finishFailedPeerMonitorCreate(requestContext, localRouteID, revision, response)
		return nil, mlModelUnavailableProblem()
	}
	route.PeerRoute.PeerLocation = peerLocation
	restoreActiveMLModelRoute(&route.PeerRoute)
	route.AcceptedRepresentation = backendView
	route.BackendRepresentation = backendView
	if !nwdafContext.UpdateMLModelMonitorSubscriptionRoute(route) {
		p.mlModelMu.Unlock()
		p.finishFailedPeerMonitorCreate(requestContext, localRouteID, revision, response)
		return nil, mlModelInternalProblem("could not record remote ML Model Monitor subscription")
	}
	p.mlModelMu.Unlock()
	return &backend.StandardResponse{
		StatusCode: http.StatusCreated,
		Location: p.privateMLModelResourceLocation(
			nwdaf_context.MLModelRoutePartyMTLFBackend,
			"ml-model-monitor/subscriptions",
			localRouteID,
		),
		ContentType: "application/json",
		Body:        backendView,
	}, nil
}

func (p *Processor) publicMLModelCallbackURI(kind, localRouteID string) string {
	config := p.config()
	if config == nil {
		return ""
	}
	return fmt.Sprintf(
		"%s/nnwdaf-callback/v1/%s/%s",
		strings.TrimRight(config.GetSbiUri(), "/"),
		kind,
		localRouteID,
	)
}

func (p *Processor) privateMLModelResourceLocation(
	party nwdaf_context.MLModelRouteParty,
	resourcePath,
	localRouteID string,
) string {
	switch party {
	case nwdaf_context.MLModelRoutePartyAnLFBackend:
		return p.anlfCallbackURI("/internal/v1/" + resourcePath + "/" + localRouteID)
	case nwdaf_context.MLModelRoutePartyMTLFBackend:
		return p.mtlfCallbackURI("/internal/v1/" + resourcePath + "/" + localRouteID)
	default:
		return ""
	}
}

func resolvedPeerLocation(response *backend.StandardResponse) (string, error) {
	if response == nil || response.StatusCode != http.StatusCreated {
		return "", errors.New("peer returned an invalid create response")
	}
	return backend.ResolvePeerLocation(response.EffectiveURI, response.Location)
}

func peerResourceIDHint(location string) string {
	parsed, err := url.Parse(location)
	if err != nil {
		return ""
	}
	value := path.Base(strings.TrimRight(parsed.Path, "/"))
	if value == "." || value == "/" {
		return ""
	}
	return value
}

func copySelectedTarget(target backend.SelectedTarget) *backend.SelectedTarget {
	copied := target
	return &copied
}

func (p *Processor) backendGeneration(availability backendAvailability) string {
	if availability == nil {
		return ""
	}
	return availability.Snapshot().ProcessInstanceID
}

func (p *Processor) mlModelPeerProblem(err error) *models.ProblemDetails {
	var standardError *backend.StandardError
	if errors.As(err, &standardError) {
		return standardError.StandardProblemDetails()
	}
	var transportError *backend.TransportError
	if errors.As(err, &transportError) {
		return mlModelUnavailableProblem()
	}
	var contractError *backend.ContractError
	if errors.As(err, &contractError) {
		return mlModelBadGatewayProblem(contractError.Detail)
	}
	return mlModelBadGatewayProblem("peer ML model request failed")
}

func peerMissing(err error) bool {
	var standardError *backend.StandardError
	return errors.As(err, &standardError) && standardError.StatusCode == http.StatusNotFound
}

func normalizeMonitorOwnerRegistrationID(
	nwdafContext *nwdaf_context.NWDAFContext,
	ownerRegistrationID string,
) (string, error) {
	ownerRegistrationID = strings.TrimSpace(ownerRegistrationID)
	if nwdafContext == nil {
		return "", errors.New("NWDAF context is unavailable")
	}
	if ownerRegistrationID == "" {
		return "", errors.New("monitor owner registration ID is required")
	}
	if route, found := nwdafContext.GetMLModelMonitorRegistrationRoute(ownerRegistrationID); found {
		if !isActiveLocalMonitorRegistration(route) {
			return "", errors.New("monitor owner registration is not an active local registration")
		}
		return route.RegistrationID, nil
	}
	matches := make([]nwdaf_context.MLModelMonitorRegistrationRoute, 0, 1)
	for _, route := range nwdafContext.GetAllMLModelMonitorRegistrationRoutes() {
		if route.PeerRoute.BackendResourceID == ownerRegistrationID &&
			isActiveLocalMonitorRegistration(route) {
			matches = append(matches, route)
		}
	}
	if len(matches) != 1 {
		return "", errors.New("monitor owner registration does not resolve to one active local registration")
	}
	return matches[0].RegistrationID, nil
}

func isActiveLocalMonitorRegistration(
	route nwdaf_context.MLModelMonitorRegistrationRoute,
) bool {
	return route.PeerRoute.Direction == nwdaf_context.MLModelRouteDirectionInbound &&
		route.PeerRoute.SelectedTarget == nil &&
		route.PeerRoute.LifecycleState == nwdaf_context.MLModelRouteActive &&
		route.PeerRoute.BackendResourceID != ""
}

func (p *Processor) finishFailedPeerProvisionCreate(
	requestContext context.Context,
	routeID string,
	revision uint64,
	response *backend.StandardResponse,
) {
	nwdafContext := p.nwdaf.Context()
	location, err := resolvedPeerLocation(response)
	if err != nil {
		p.mlModelMu.Lock()
		route, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(routeID)
		if found && mlModelRouteOperationCurrent(
			route.PeerRoute,
			nwdaf_context.MLModelRouteCreating,
			revision,
		) {
			nwdafContext.DeleteMLModelProvisionSubscriptionRoute(routeID)
		}
		p.mlModelMu.Unlock()
		return
	}
	cleanupResponse, cleanupErr := p.mlModelPeerConsumer.DeletePeerMLModelProvision(
		requestContext,
		location,
	)
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()
	route, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(routeID)
	if !found || !mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		return
	}
	if cleanupResponseAccepted(cleanupResponse, cleanupErr) {
		nwdafContext.DeleteMLModelProvisionSubscriptionRoute(routeID)
		return
	}
	route.PeerRoute.PeerLocation = location
	p.markPeerRoutePendingCleanupLocked(&route.PeerRoute)
	route.AcceptedRepresentation = nil
	route.BackendRepresentation = nil
	route.DestinationNotificationURI = ""
	route.NotificationCorrelationID = ""
	nwdafContext.UpdateMLModelProvisionSubscriptionRoute(route)
	logPeerCleanupFailure("compensate peer provision create", cleanupResponse, cleanupErr)
}

func (p *Processor) finishFailedPeerRegistrationCreate(
	requestContext context.Context,
	routeID string,
	revision uint64,
	response *backend.StandardResponse,
) {
	nwdafContext := p.nwdaf.Context()
	location, err := resolvedPeerLocation(response)
	if err != nil {
		p.mlModelMu.Lock()
		route, found := nwdafContext.GetMLModelMonitorRegistrationRoute(routeID)
		if found && mlModelRouteOperationCurrent(
			route.PeerRoute,
			nwdaf_context.MLModelRouteCreating,
			revision,
		) {
			nwdafContext.DeleteMLModelMonitorRegistrationRoute(routeID)
		}
		p.mlModelMu.Unlock()
		return
	}
	cleanupResponse, cleanupErr := p.mlModelPeerConsumer.DeletePeerMLModelMonitorRegistration(
		requestContext,
		location,
	)
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()
	route, found := nwdafContext.GetMLModelMonitorRegistrationRoute(routeID)
	if !found || !mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		return
	}
	if cleanupResponseAccepted(cleanupResponse, cleanupErr) {
		nwdafContext.DeleteMLModelMonitorRegistrationRoute(routeID)
		return
	}
	route.PeerRoute.PeerLocation = location
	p.markPeerRoutePendingCleanupLocked(&route.PeerRoute)
	route.AcceptedRepresentation = nil
	route.BackendRepresentation = nil
	nwdafContext.UpdateMLModelMonitorRegistrationRoute(route)
	logPeerCleanupFailure("compensate peer monitor registration create", cleanupResponse, cleanupErr)
}

func (p *Processor) finishFailedPeerMonitorCreate(
	requestContext context.Context,
	routeID string,
	revision uint64,
	response *backend.StandardResponse,
) {
	nwdafContext := p.nwdaf.Context()
	location, err := resolvedPeerLocation(response)
	if err != nil {
		p.mlModelMu.Lock()
		route, found := nwdafContext.GetMLModelMonitorSubscriptionRoute(routeID)
		if found && mlModelRouteOperationCurrent(
			route.PeerRoute,
			nwdaf_context.MLModelRouteCreating,
			revision,
		) {
			nwdafContext.DeleteMLModelMonitorSubscriptionRoute(routeID)
		}
		p.mlModelMu.Unlock()
		return
	}
	cleanupResponse, cleanupErr := p.mlModelPeerConsumer.DeletePeerMLModelMonitorSubscription(
		requestContext,
		location,
	)
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()
	route, found := nwdafContext.GetMLModelMonitorSubscriptionRoute(routeID)
	if !found || !mlModelRouteOperationCurrent(
		route.PeerRoute,
		nwdaf_context.MLModelRouteCreating,
		revision,
	) {
		return
	}
	if cleanupResponseAccepted(cleanupResponse, cleanupErr) {
		nwdafContext.DeleteMLModelMonitorSubscriptionRoute(routeID)
		return
	}
	route.PeerRoute.PeerLocation = location
	p.markPeerRoutePendingCleanupLocked(&route.PeerRoute)
	route.OwnerRegistrationID = ""
	route.AcceptedRepresentation = nil
	route.BackendRepresentation = nil
	route.DestinationNotificationURI = ""
	route.NotificationCorrelationID = ""
	nwdafContext.UpdateMLModelMonitorSubscriptionRoute(route)
	logPeerCleanupFailure("compensate peer monitor subscription create", cleanupResponse, cleanupErr)
}

func (p *Processor) markPeerRoutePendingCleanupLocked(route *nwdaf_context.MLModelPeerRoute) {
	route.OperationRevision = p.nextMLModelOperationRevisionLocked()
	route.LifecycleState = nwdaf_context.MLModelRoutePendingCleanup
	route.ProcessGeneration = ""
	route.CleanupAttempts = 1
	route.NextCleanupAt = time.Now().Add(peerCleanupRetryDelay(route.CleanupAttempts))
}

func peerCleanupRetryDelay(attempt int) time.Duration {
	if attempt <= 1 {
		return time.Second
	}
	if attempt >= 6 {
		return 30 * time.Second
	}
	return time.Duration(1<<(attempt-1)) * time.Second
}

// RunMLModelPeerCleanup retries compensation for peer resources that were
// created successfully but could not be removed after a later local failure.
func (p *Processor) RunMLModelPeerCleanup(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.ReconcilePendingMLModelPeerCleanup(ctx, time.Now())
		}
	}
}

func (p *Processor) ReconcilePendingMLModelPeerCleanup(
	requestContext context.Context,
	now time.Time,
) {
	if p.nwdaf == nil || p.nwdaf.Context() == nil {
		return
	}
	nwdafContext := p.nwdaf.Context()
	if p.mlModelPeerConsumer != nil {
		for _, route := range nwdafContext.GetAllMLModelProvisionSubscriptionRoutes() {
			p.reconcilePendingProvisionCleanup(requestContext, route.SubscriptionID, now)
		}
		for _, route := range nwdafContext.GetAllMLModelMonitorRegistrationRoutes() {
			p.reconcilePendingRegistrationCleanup(requestContext, route.RegistrationID, now)
		}
		for _, route := range nwdafContext.GetAllMLModelMonitorSubscriptionRoutes() {
			p.reconcilePendingMonitorCleanup(requestContext, route.SubscriptionID, now)
		}
	}
	for _, route := range nwdafContext.GetAllMLModelTrainingSubscriptionRoutes() {
		p.reconcilePendingTrainingCleanup(requestContext, route, now)
	}
}

func (p *Processor) reconcilePendingProvisionCleanup(
	ctx context.Context,
	id string,
	now time.Time,
) {
	nwdafContext := p.nwdaf.Context()
	p.mlModelMu.Lock()
	route, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(id)
	if !found || !peerCleanupDue(route.PeerRoute, now) {
		p.mlModelMu.Unlock()
		return
	}
	revision := p.claimPendingCleanupLocked(&route.PeerRoute)
	nwdafContext.UpdateMLModelProvisionSubscriptionRoute(route)
	p.mlModelMu.Unlock()

	response, err := p.mlModelPeerConsumer.DeletePeerMLModelProvision(
		ctx,
		route.PeerRoute.PeerLocation,
	)
	p.mlModelMu.Lock()
	current, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(id)
	if found && mlModelRouteOperationCurrent(
		current.PeerRoute,
		nwdaf_context.MLModelRouteDeleting,
		revision,
	) {
		if cleanupResponseAccepted(response, err) {
			nwdafContext.DeleteMLModelProvisionSubscriptionRoute(id)
		} else {
			current.PeerRoute.LifecycleState = nwdaf_context.MLModelRoutePendingCleanup
			scheduleNextPeerCleanup(&current.PeerRoute, now)
			nwdafContext.UpdateMLModelProvisionSubscriptionRoute(current)
			logPeerCleanupFailure("retry peer provision cleanup", response, err)
		}
	}
	p.mlModelMu.Unlock()
}

func (p *Processor) reconcilePendingRegistrationCleanup(
	ctx context.Context,
	id string,
	now time.Time,
) {
	nwdafContext := p.nwdaf.Context()
	p.mlModelMu.Lock()
	route, found := nwdafContext.GetMLModelMonitorRegistrationRoute(id)
	if !found || !peerCleanupDue(route.PeerRoute, now) {
		p.mlModelMu.Unlock()
		return
	}
	revision := p.claimPendingCleanupLocked(&route.PeerRoute)
	nwdafContext.UpdateMLModelMonitorRegistrationRoute(route)
	p.mlModelMu.Unlock()

	response, err := p.mlModelPeerConsumer.DeletePeerMLModelMonitorRegistration(
		ctx,
		route.PeerRoute.PeerLocation,
	)
	p.mlModelMu.Lock()
	current, found := nwdafContext.GetMLModelMonitorRegistrationRoute(id)
	if found && mlModelRouteOperationCurrent(
		current.PeerRoute,
		nwdaf_context.MLModelRouteDeleting,
		revision,
	) {
		if cleanupResponseAccepted(response, err) {
			nwdafContext.DeleteMLModelMonitorRegistrationRoute(id)
		} else {
			current.PeerRoute.LifecycleState = nwdaf_context.MLModelRoutePendingCleanup
			scheduleNextPeerCleanup(&current.PeerRoute, now)
			nwdafContext.UpdateMLModelMonitorRegistrationRoute(current)
			logPeerCleanupFailure("retry peer monitor registration cleanup", response, err)
		}
	}
	p.mlModelMu.Unlock()
}

func (p *Processor) reconcilePendingMonitorCleanup(
	ctx context.Context,
	id string,
	now time.Time,
) {
	nwdafContext := p.nwdaf.Context()
	p.mlModelMu.Lock()
	route, found := nwdafContext.GetMLModelMonitorSubscriptionRoute(id)
	if !found || !peerCleanupDue(route.PeerRoute, now) {
		p.mlModelMu.Unlock()
		return
	}
	revision := p.claimPendingCleanupLocked(&route.PeerRoute)
	nwdafContext.UpdateMLModelMonitorSubscriptionRoute(route)
	p.mlModelMu.Unlock()

	response, err := p.mlModelPeerConsumer.DeletePeerMLModelMonitorSubscription(
		ctx,
		route.PeerRoute.PeerLocation,
	)
	p.mlModelMu.Lock()
	current, found := nwdafContext.GetMLModelMonitorSubscriptionRoute(id)
	if found && mlModelRouteOperationCurrent(
		current.PeerRoute,
		nwdaf_context.MLModelRouteDeleting,
		revision,
	) {
		if cleanupResponseAccepted(response, err) {
			nwdafContext.DeleteMLModelMonitorSubscriptionRoute(id)
		} else {
			current.PeerRoute.LifecycleState = nwdaf_context.MLModelRoutePendingCleanup
			scheduleNextPeerCleanup(&current.PeerRoute, now)
			nwdafContext.UpdateMLModelMonitorSubscriptionRoute(current)
			logPeerCleanupFailure("retry peer monitor subscription cleanup", response, err)
		}
	}
	p.mlModelMu.Unlock()
}

func (p *Processor) reconcilePendingTrainingCleanup(
	ctx context.Context,
	entry nwdaf_context.MLModelTrainingSubscriptionRoute,
	now time.Time,
) {
	nwdafContext := p.nwdaf.Context()
	p.mlModelMu.Lock()
	var route nwdaf_context.MLModelTrainingSubscriptionRoute
	var found bool
	if entry.SubscriptionID == "" {
		route, found = nwdafContext.GetPendingMLModelTrainingRoute(entry.CallbackRouteID)
	} else {
		route, found = nwdafContext.GetMLModelTrainingSubscriptionRoute(entry.ResourceKey())
	}
	inboundTermination := found && inboundTrainingTerminationCleanupDue(route, now)
	peerCleanup := found && peerCleanupDue(route.PeerRoute, now)
	if !found || (!inboundTermination && !peerCleanup) ||
		(peerCleanup && p.mlModelPeerConsumer == nil) {
		p.mlModelMu.Unlock()
		return
	}
	revision := p.claimPendingCleanupLocked(&route.PeerRoute)
	nwdafContext.UpdateMLModelTrainingSubscriptionRoute(route)
	p.mlModelMu.Unlock()

	var response *backend.StandardResponse
	var err error
	if inboundTermination {
		currentGeneration := p.backendGeneration(p.mtlfAvailability)
		if route.PeerRoute.ProcessGeneration != "" && currentGeneration != "" &&
			route.PeerRoute.ProcessGeneration != currentGeneration {
			response = &backend.StandardResponse{StatusCode: http.StatusNotFound}
		} else {
			lease, admitted := acquireBackend(p.mtlfMLModelBackend, p.mtlfAvailability)
			if admitted {
				if lease != nil {
					defer lease.Release()
				}
				response, err = p.mtlfMLModelBackend.DeleteMLModelTrainingSubscription(
					ctx,
					route.SubscriptionID,
				)
			} else {
				err = errors.New("MTLF backend is unavailable")
			}
		}
	} else {
		response, err = p.mlModelPeerConsumer.DeletePeerMLModelTraining(
			ctx,
			route.PeerRoute.PeerLocation,
		)
	}
	p.mlModelMu.Lock()
	var current nwdaf_context.MLModelTrainingSubscriptionRoute
	if entry.SubscriptionID == "" {
		current, found = nwdafContext.GetPendingMLModelTrainingRoute(entry.CallbackRouteID)
	} else {
		current, found = nwdafContext.GetMLModelTrainingSubscriptionRoute(entry.ResourceKey())
	}
	if found && mlModelRouteOperationCurrent(
		current.PeerRoute,
		nwdaf_context.MLModelRouteDeleting,
		revision,
	) {
		if cleanupResponseAccepted(response, err) {
			if entry.SubscriptionID == "" {
				nwdafContext.DeletePendingMLModelTrainingRoute(entry.CallbackRouteID)
			} else {
				nwdafContext.DeleteMLModelTrainingSubscriptionRoute(entry.ResourceKey())
			}
			if inboundTermination {
				nwdafContext.TombstoneMLModelResource(
					nwdaf_context.MLModelDeletionRecord{
						ResourceID:        route.SubscriptionID,
						ProcessGeneration: route.PeerRoute.ProcessGeneration,
						CleanupAttempted:  true,
					},
					nwdaf_context.MLModelResourceTrainingSubscription,
				)
				logger.ProcLog.Infof(
					"Cleaned terminating inbound ML Model Training resource subscription_id=%s",
					route.SubscriptionID,
				)
			}
		} else {
			if inboundTermination {
				current.PeerRoute.LifecycleState = nwdaf_context.MLModelRouteTerminating
			} else {
				current.PeerRoute.LifecycleState = nwdaf_context.MLModelRoutePendingCleanup
			}
			scheduleNextPeerCleanup(&current.PeerRoute, now)
			nwdafContext.UpdateMLModelTrainingSubscriptionRoute(current)
			operation := "retry peer training cleanup"
			if inboundTermination {
				operation = "cleanup terminating inbound training resource"
			}
			logPeerCleanupFailure(operation, response, err)
		}
	}
	p.mlModelMu.Unlock()
}

func inboundTrainingTerminationCleanupDue(
	route nwdaf_context.MLModelTrainingSubscriptionRoute,
	now time.Time,
) bool {
	return route.PeerRoute.LifecycleState == nwdaf_context.MLModelRouteTerminating &&
		route.PeerRoute.Direction == nwdaf_context.MLModelRouteDirectionInbound &&
		route.SubscriptionID != "" &&
		(route.PeerRoute.NextCleanupAt.IsZero() || !now.Before(route.PeerRoute.NextCleanupAt))
}

func (p *Processor) claimPendingCleanupLocked(route *nwdaf_context.MLModelPeerRoute) uint64 {
	route.OperationRevision = p.nextMLModelOperationRevisionLocked()
	route.LifecycleState = nwdaf_context.MLModelRouteDeleting
	return route.OperationRevision
}

func peerCleanupDue(route nwdaf_context.MLModelPeerRoute, now time.Time) bool {
	return route.LifecycleState == nwdaf_context.MLModelRoutePendingCleanup &&
		route.PeerLocation != "" &&
		(route.NextCleanupAt.IsZero() || !now.Before(route.NextCleanupAt))
}

func scheduleNextPeerCleanup(route *nwdaf_context.MLModelPeerRoute, now time.Time) {
	route.CleanupAttempts++
	route.NextCleanupAt = now.Add(peerCleanupRetryDelay(route.CleanupAttempts))
}

func logPeerCleanupError(operation string, err error) {
	if err != nil {
		logger.ProcLog.Warnf("%s failed: %v", operation, err)
	}
}

func logPeerCleanupFailure(
	operation string,
	response *backend.StandardResponse,
	err error,
) {
	if err != nil {
		logPeerCleanupError(operation, err)
		return
	}
	logger.ProcLog.Warnf(
		"%s returned an invalid response: status=%d",
		operation,
		cleanupStatus(response),
	)
}
