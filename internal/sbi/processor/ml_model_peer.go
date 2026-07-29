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
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()

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
	if nwdafContext == nil || !nwdafContext.AddMLModelProvisionSubscriptionRoute(
		nwdaf_context.MLModelProvisionSubscriptionRoute{
			SubscriptionID: localRouteID,
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:         nwdaf_context.MLModelRouteDirectionOutbound,
				SelectedTarget:    copySelectedTarget(target),
				LifecycleState:    nwdaf_context.MLModelRouteCreating,
				ProcessGeneration: p.backendGeneration(p.anlfAvailability),
			},
			Initiator:                  nwdaf_context.MLModelRoutePartyAnLFBackend,
			Destination:                nwdaf_context.MLModelRoutePartyAnLFBackend,
			DestinationNotificationURI: parsed.NotificationURI,
			NotificationCorrelationID:  parsed.NotificationID,
		},
	) {
		return nil, mlModelInternalProblem("could not reserve remote ML Model Provision route")
	}
	response, peerErr := p.mlModelPeerConsumer.CreatePeerMLModelProvision(
		requestContext,
		target,
		peerBody,
	)
	if peerErr != nil {
		p.finishFailedPeerProvisionCreate(requestContext, localRouteID, response)
		return nil, p.mlModelPeerProblem(peerErr)
	}
	peerLocation, err := resolvedPeerLocation(response)
	if err != nil {
		p.finishFailedPeerProvisionCreate(requestContext, localRouteID, response)
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	backendView, err := wire.ReplaceStringField(response.Body, "notifUri", parsed.NotificationURI)
	if err != nil {
		p.finishFailedPeerProvisionCreate(requestContext, localRouteID, response)
		return nil, mlModelBadGatewayProblem("peer returned an invalid provision representation")
	}
	route := nwdaf_context.MLModelProvisionSubscriptionRoute{
		SubscriptionID: localRouteID,
		PeerRoute: nwdaf_context.MLModelPeerRoute{
			Direction:         nwdaf_context.MLModelRouteDirectionOutbound,
			SelectedTarget:    copySelectedTarget(target),
			PeerLocation:      peerLocation,
			LifecycleState:    nwdaf_context.MLModelRouteActive,
			ProcessGeneration: p.backendGeneration(p.anlfAvailability),
		},
		AcceptedRepresentation:     backendView,
		BackendRepresentation:      backendView,
		Initiator:                  nwdaf_context.MLModelRoutePartyAnLFBackend,
		Destination:                nwdaf_context.MLModelRoutePartyAnLFBackend,
		DestinationNotificationURI: parsed.NotificationURI,
		NotificationCorrelationID:  parsed.NotificationID,
	}
	if !nwdafContext.UpdateMLModelProvisionSubscriptionRoute(route) {
		p.finishFailedPeerProvisionCreate(requestContext, localRouteID, response)
		return nil, mlModelInternalProblem("could not record remote ML Model Provision route")
	}
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
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()

	if _, err := wire.ParseMLModelMonitorRegistration(body); err != nil {
		return nil, malformedMLModelProblem(err)
	}
	if p.mlModelPeerConsumer == nil {
		return nil, mlModelUnavailableProblem()
	}
	localRouteID := uuid.New().String()
	nwdafContext := p.nwdaf.Context()
	if nwdafContext == nil || !nwdafContext.AddMLModelMonitorRegistrationRoute(
		nwdaf_context.MLModelMonitorRegistrationRoute{
			RegistrationID: localRouteID,
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:         nwdaf_context.MLModelRouteDirectionOutbound,
				SelectedTarget:    copySelectedTarget(target),
				LifecycleState:    nwdaf_context.MLModelRouteCreating,
				ProcessGeneration: p.backendGeneration(p.anlfAvailability),
			},
			Initiator: nwdaf_context.MLModelRoutePartyAnLFBackend,
		},
	) {
		return nil, mlModelInternalProblem("could not reserve remote ML Model Monitor registration")
	}
	response, peerErr := p.mlModelPeerConsumer.CreatePeerMLModelMonitorRegistration(
		requestContext,
		target,
		body,
	)
	if peerErr != nil {
		p.finishFailedPeerRegistrationCreate(requestContext, localRouteID, response)
		return nil, p.mlModelPeerProblem(peerErr)
	}
	peerLocation, err := resolvedPeerLocation(response)
	if err != nil {
		p.finishFailedPeerRegistrationCreate(requestContext, localRouteID, response)
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	route := nwdaf_context.MLModelMonitorRegistrationRoute{
		RegistrationID: localRouteID,
		PeerRoute: nwdaf_context.MLModelPeerRoute{
			Direction:         nwdaf_context.MLModelRouteDirectionOutbound,
			SelectedTarget:    copySelectedTarget(target),
			PeerLocation:      peerLocation,
			LifecycleState:    nwdaf_context.MLModelRouteActive,
			ProcessGeneration: p.backendGeneration(p.anlfAvailability),
		},
		AcceptedRepresentation: append(json.RawMessage(nil), response.Body...),
		BackendRepresentation:  append(json.RawMessage(nil), response.Body...),
		Initiator:              nwdaf_context.MLModelRoutePartyAnLFBackend,
	}
	if !nwdafContext.UpdateMLModelMonitorRegistrationRoute(route) {
		p.finishFailedPeerRegistrationCreate(requestContext, localRouteID, response)
		return nil, mlModelInternalProblem("could not record remote ML Model Monitor registration")
	}
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
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()

	parsed, err := wire.ParseMLModelMonitorSubscription(body)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	if p.mlModelPeerConsumer == nil {
		return nil, mlModelUnavailableProblem()
	}
	nwdafContext := p.nwdaf.Context()
	normalizedOwnerID, normalizeErr := normalizeMonitorOwnerRegistrationID(
		nwdafContext,
		ownerRegistrationID,
	)
	if normalizeErr != nil {
		return nil, malformedMLModelProblem(normalizeErr)
	}
	localRouteID := uuid.New().String()
	peerBody, err := wire.ReplaceStringField(
		body,
		"notificationUri",
		p.publicMLModelCallbackURI("ml-model-monitor", localRouteID),
	)
	if err != nil {
		return nil, malformedMLModelProblem(err)
	}
	if nwdafContext == nil || !nwdafContext.AddMLModelMonitorSubscriptionRoute(
		nwdaf_context.MLModelMonitorSubscriptionRoute{
			SubscriptionID: localRouteID,
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:         nwdaf_context.MLModelRouteDirectionOutbound,
				SelectedTarget:    copySelectedTarget(target),
				LifecycleState:    nwdaf_context.MLModelRouteCreating,
				ProcessGeneration: p.backendGeneration(p.mtlfAvailability),
			},
			OwnerRegistrationID:        normalizedOwnerID,
			Destination:                nwdaf_context.MLModelRoutePartyMTLFBackend,
			DestinationNotificationURI: parsed.NotificationURI,
			NotificationCorrelationID:  parsed.NotificationID,
		},
	) {
		return nil, mlModelInternalProblem("could not reserve remote ML Model Monitor subscription")
	}
	response, peerErr := p.mlModelPeerConsumer.CreatePeerMLModelMonitorSubscription(
		requestContext,
		target,
		peerBody,
	)
	if peerErr != nil {
		p.finishFailedPeerMonitorCreate(requestContext, localRouteID, response)
		return nil, p.mlModelPeerProblem(peerErr)
	}
	peerLocation, err := resolvedPeerLocation(response)
	if err != nil {
		p.finishFailedPeerMonitorCreate(requestContext, localRouteID, response)
		return nil, mlModelBadGatewayProblem(err.Error())
	}
	backendView, err := wire.ReplaceStringField(
		response.Body,
		"notificationUri",
		parsed.NotificationURI,
	)
	if err != nil {
		p.finishFailedPeerMonitorCreate(requestContext, localRouteID, response)
		return nil, mlModelBadGatewayProblem("peer returned an invalid monitor representation")
	}
	route := nwdaf_context.MLModelMonitorSubscriptionRoute{
		SubscriptionID: localRouteID,
		PeerRoute: nwdaf_context.MLModelPeerRoute{
			Direction:         nwdaf_context.MLModelRouteDirectionOutbound,
			SelectedTarget:    copySelectedTarget(target),
			PeerLocation:      peerLocation,
			LifecycleState:    nwdaf_context.MLModelRouteActive,
			ProcessGeneration: p.backendGeneration(p.mtlfAvailability),
		},
		OwnerRegistrationID:        normalizedOwnerID,
		AcceptedRepresentation:     backendView,
		BackendRepresentation:      backendView,
		Destination:                nwdaf_context.MLModelRoutePartyMTLFBackend,
		DestinationNotificationURI: parsed.NotificationURI,
		NotificationCorrelationID:  parsed.NotificationID,
	}
	if !nwdafContext.UpdateMLModelMonitorSubscriptionRoute(route) {
		p.finishFailedPeerMonitorCreate(requestContext, localRouteID, response)
		return nil, mlModelInternalProblem("could not record remote ML Model Monitor subscription")
	}
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
	provider, ok := availability.(interface{ Snapshot() backend.Snapshot })
	if !ok {
		return ""
	}
	return provider.Snapshot().ProcessInstanceID
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
	response *backend.StandardResponse,
) {
	nwdafContext := p.nwdaf.Context()
	location, err := resolvedPeerLocation(response)
	if err != nil {
		nwdafContext.DeleteMLModelProvisionSubscriptionRoute(routeID)
		return
	}
	_, cleanupErr := p.mlModelPeerConsumer.DeletePeerMLModelProvision(requestContext, location)
	if cleanupErr == nil || peerMissing(cleanupErr) {
		nwdafContext.DeleteMLModelProvisionSubscriptionRoute(routeID)
		return
	}
	route, found := nwdafContext.GetMLModelProvisionSubscriptionRoute(routeID)
	if !found {
		return
	}
	route.PeerRoute.PeerLocation = location
	markPeerRoutePendingCleanup(&route.PeerRoute)
	route.AcceptedRepresentation = nil
	route.BackendRepresentation = nil
	route.DestinationNotificationURI = ""
	route.NotificationCorrelationID = ""
	nwdafContext.UpdateMLModelProvisionSubscriptionRoute(route)
	logPeerCleanupError("compensate peer provision create", cleanupErr)
}

func (p *Processor) finishFailedPeerRegistrationCreate(
	requestContext context.Context,
	routeID string,
	response *backend.StandardResponse,
) {
	nwdafContext := p.nwdaf.Context()
	location, err := resolvedPeerLocation(response)
	if err != nil {
		nwdafContext.DeleteMLModelMonitorRegistrationRoute(routeID)
		return
	}
	_, cleanupErr := p.mlModelPeerConsumer.DeletePeerMLModelMonitorRegistration(
		requestContext,
		location,
	)
	if cleanupErr == nil || peerMissing(cleanupErr) {
		nwdafContext.DeleteMLModelMonitorRegistrationRoute(routeID)
		return
	}
	route, found := nwdafContext.GetMLModelMonitorRegistrationRoute(routeID)
	if !found {
		return
	}
	route.PeerRoute.PeerLocation = location
	markPeerRoutePendingCleanup(&route.PeerRoute)
	route.AcceptedRepresentation = nil
	route.BackendRepresentation = nil
	nwdafContext.UpdateMLModelMonitorRegistrationRoute(route)
	logPeerCleanupError("compensate peer monitor registration create", cleanupErr)
}

func (p *Processor) finishFailedPeerMonitorCreate(
	requestContext context.Context,
	routeID string,
	response *backend.StandardResponse,
) {
	nwdafContext := p.nwdaf.Context()
	location, err := resolvedPeerLocation(response)
	if err != nil {
		nwdafContext.DeleteMLModelMonitorSubscriptionRoute(routeID)
		return
	}
	_, cleanupErr := p.mlModelPeerConsumer.DeletePeerMLModelMonitorSubscription(
		requestContext,
		location,
	)
	if cleanupErr == nil || peerMissing(cleanupErr) {
		nwdafContext.DeleteMLModelMonitorSubscriptionRoute(routeID)
		return
	}
	route, found := nwdafContext.GetMLModelMonitorSubscriptionRoute(routeID)
	if !found {
		return
	}
	route.PeerRoute.PeerLocation = location
	markPeerRoutePendingCleanup(&route.PeerRoute)
	route.OwnerRegistrationID = ""
	route.AcceptedRepresentation = nil
	route.BackendRepresentation = nil
	route.DestinationNotificationURI = ""
	route.NotificationCorrelationID = ""
	nwdafContext.UpdateMLModelMonitorSubscriptionRoute(route)
	logPeerCleanupError("compensate peer monitor subscription create", cleanupErr)
}

func markPeerRoutePendingCleanup(route *nwdaf_context.MLModelPeerRoute) {
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
	p.mlModelMu.Lock()
	defer p.mlModelMu.Unlock()
	if p.mlModelPeerConsumer == nil || p.nwdaf == nil || p.nwdaf.Context() == nil {
		return
	}
	nwdafContext := p.nwdaf.Context()
	for _, route := range nwdafContext.GetAllMLModelProvisionSubscriptionRoutes() {
		if !peerCleanupDue(route.PeerRoute, now) {
			continue
		}
		_, err := p.mlModelPeerConsumer.DeletePeerMLModelProvision(
			requestContext,
			route.PeerRoute.PeerLocation,
		)
		if err == nil || peerMissing(err) {
			nwdafContext.DeleteMLModelProvisionSubscriptionRoute(route.SubscriptionID)
			continue
		}
		scheduleNextPeerCleanup(&route.PeerRoute, now)
		nwdafContext.UpdateMLModelProvisionSubscriptionRoute(route)
	}
	for _, route := range nwdafContext.GetAllMLModelMonitorRegistrationRoutes() {
		if !peerCleanupDue(route.PeerRoute, now) {
			continue
		}
		_, err := p.mlModelPeerConsumer.DeletePeerMLModelMonitorRegistration(
			requestContext,
			route.PeerRoute.PeerLocation,
		)
		if err == nil || peerMissing(err) {
			nwdafContext.DeleteMLModelMonitorRegistrationRoute(route.RegistrationID)
			continue
		}
		scheduleNextPeerCleanup(&route.PeerRoute, now)
		nwdafContext.UpdateMLModelMonitorRegistrationRoute(route)
	}
	for _, route := range nwdafContext.GetAllMLModelMonitorSubscriptionRoutes() {
		if !peerCleanupDue(route.PeerRoute, now) {
			continue
		}
		_, err := p.mlModelPeerConsumer.DeletePeerMLModelMonitorSubscription(
			requestContext,
			route.PeerRoute.PeerLocation,
		)
		if err == nil || peerMissing(err) {
			nwdafContext.DeleteMLModelMonitorSubscriptionRoute(route.SubscriptionID)
			continue
		}
		scheduleNextPeerCleanup(&route.PeerRoute, now)
		nwdafContext.UpdateMLModelMonitorSubscriptionRoute(route)
	}
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
