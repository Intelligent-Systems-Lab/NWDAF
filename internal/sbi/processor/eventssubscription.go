package processor

import (
	"fmt"
	"net/http"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

// HandleCreateSubscription processes new subscription requests
func (p *Processor) HandleCreateSubscription(
	req *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, string, *models.ProblemDetails) {
	// Phase 1: Hard validation (structure, event type, target period, etc.)
	if problemDetails := p.validateSubscriptionRequest(req); problemDetails != nil {
		return nil, "", problemDetails
	}

	// Phase 1.5: Apply defaults and validate notification method
	// Per TS 29.520: notificationMethod defaults to THRESHOLD when omitted
	if problemDetails := p.applyAndValidateDefaults(req); problemDetails != nil {
		return nil, "", problemDetails
	}

	// Phase 2: Collect soft failures (failEventReports)
	failEventReports := p.collectFailEventReports(req.EventSubscriptions)

	// Check if all events failed - if so, reject with 400
	if len(failEventReports) == len(req.EventSubscriptions) {
		return nil, "", &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "ALL_EVENTS_UNSUPPORTED",
			Detail: "All requested analytics events are not supported",
		}
	}

	// Generate subscription ID
	subscriptionId := nwdaf_context.NewSubscriptionId()

	// Create subscription object
	subscription := &nwdaf_context.Subscription{
		ID:              subscriptionId,
		NotificationURI: req.NotificationURI,
		NotifCorrId:     req.NotifCorrId,
		EventSubs:       req.EventSubscriptions,
		EvtReq:          req.EvtReq,
		IsActive:        true,
	}
	if problemDetails := p.prepareInitialAnalyticsRuntime(subscription); problemDetails != nil {
		return nil, "", problemDetails
	}

	// Populate notification control fields from EvtReq
	if req.EvtReq != nil {
		subscription.NotifMethod = string(req.EvtReq.NotifMethod)
		subscription.RepPeriod = req.EvtReq.RepPeriod
		subscription.MaxReportNbr = req.EvtReq.MaxReportNbr
		if req.EvtReq.MonDur != nil {
			monDur := *req.EvtReq.MonDur
			subscription.MonDur = &monDur
		}
	}

	// Store subscription
	ctx := nwdaf_context.GetSelf()
	ctx.AddSubscription(subscription)

	// Complete source reconciliation and binding sync before accepting the subscription.
	if collectionErr := p.TriggerDataCollection(req.EventSubscriptions, subscriptionId); collectionErr != nil {
		subscription.SetActive(false)
		p.cleanupMlModelState(subscriptionId)
		p.cleanupDataCollection(subscriptionId)
		ctx.DeleteSubscription(subscriptionId)
		return nil, "", analyticsRuntimeUnavailableProblem()
	}

	// Prepare response
	response := buildSubscriptionResponse(req, failEventReports)
	logger.ProcLog.Infof("CreateSubscription: created sub=%s failedEvents=%d",
		subscriptionId, len(failEventReports))

	return response, subscriptionId, nil
}

// HandleUpdateSubscription processes subscription update requests
func (p *Processor) HandleUpdateSubscription(
	subscriptionId string,
	req *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, *models.ProblemDetails) {
	ctx := nwdaf_context.GetSelf()

	// Check if subscription exists
	existing := ctx.GetSubscription(subscriptionId)
	if existing == nil {
		return nil, &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "SUBSCRIPTION_NOT_FOUND",
			Detail: fmt.Sprintf("Subscription %s not found", subscriptionId),
		}
	}

	// Phase 1: Hard validation (same as Create)
	if problemDetails := p.validateSubscriptionRequest(req); problemDetails != nil {
		return nil, problemDetails
	}

	// Phase 1.5: Apply defaults and validate notification method
	if problemDetails := p.applyAndValidateDefaults(req); problemDetails != nil {
		return nil, problemDetails
	}

	// Phase 2: Collect soft failures (failEventReports)
	failEventReports := p.collectFailEventReports(req.EventSubscriptions)

	// Check if all events failed
	if len(failEventReports) == len(req.EventSubscriptions) {
		return nil, &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "ALL_EVENTS_UNSUPPORTED",
			Detail: "All requested analytics events are not supported",
		}
	}

	// Update subscription
	subscription := buildSubscription(subscriptionId, req)
	subscription.CreatedAt = existing.CreatedAt
	existing.SetActive(false)
	runtimeResponse, err := p.anlf.ApplyInitialSubscriptionRuntime(subscription)
	if err != nil {
		existing.SetActive(true)
		return nil, analyticsRuntimeUnavailableProblem()
	}
	subscription.SetRuntime(
		runtimeResponse.RuntimeRevision,
		nwdaf_context.CollectionRequirements{
			SamplingIntervalSeconds: runtimeResponse.CollectionRequirements.SamplingIntervalSeconds,
			RequiredMeasurements:    runtimeResponse.CollectionRequirements.RequiredMeasurements,
		},
		nil,
	)

	ctx.UpdateSubscription(subscription)

	if collectionErr := p.TriggerDataCollection(req.EventSubscriptions, subscriptionId); collectionErr != nil {
		if rollbackErr := p.rollbackSubscriptionUpdate(existing); rollbackErr != nil {
			logger.ProcLog.Errorf(
				"Subscription update rollback failed: sub=%s err=%v",
				subscriptionId,
				rollbackErr,
			)
		}
		return nil, analyticsRuntimeUnavailableProblem()
	}

	// Prepare response
	response := buildSubscriptionResponse(req, failEventReports)
	logger.ProcLog.Infof("UpdateSubscription: updated sub=%s failedEvents=%d",
		subscriptionId, len(failEventReports))

	return response, nil
}

func (p *Processor) rollbackSubscriptionUpdate(
	previous *nwdaf_context.Subscription,
) error {
	if previous == nil {
		return fmt.Errorf("previous subscription is required")
	}
	response, err := p.anlf.ApplyInitialSubscriptionRuntime(previous)
	if err != nil {
		return err
	}
	previous.SetRuntime(
		response.RuntimeRevision,
		nwdaf_context.CollectionRequirements{
			SamplingIntervalSeconds: response.CollectionRequirements.SamplingIntervalSeconds,
			RequiredMeasurements:    response.CollectionRequirements.RequiredMeasurements,
		},
		nil,
	)
	previous.SetActive(true)
	nwdaf_context.GetSelf().UpdateSubscription(previous)
	return p.TriggerDataCollection(previous.EventSubs, previous.ID)
}

// HandleDeleteSubscription processes subscription deletion requests
func (p *Processor) HandleDeleteSubscription(subscriptionId string) *models.ProblemDetails {
	ctx := nwdaf_context.GetSelf()

	// Get subscription to stop scheduler before deletion
	subscription := ctx.GetSubscription(subscriptionId)
	if subscription == nil {
		return &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "SUBSCRIPTION_NOT_FOUND",
			Detail: fmt.Sprintf("Subscription %s not found", subscriptionId),
		}
	}

	subscription.SetActive(false)

	p.cleanupMlModelState(subscriptionId)

	// Cleanup SMF subscriptions only after the backend reporting runtime stops.
	p.cleanupDataCollection(subscriptionId)

	ctx.DeleteSubscription(subscriptionId)
	logger.ProcLog.Infof("DeleteSubscription: deleted sub=%s", subscriptionId)
	return nil
}

func buildSubscription(
	subscriptionId string,
	req *models.NnwdafEventsSubscription,
) *nwdaf_context.Subscription {
	subscription := &nwdaf_context.Subscription{
		ID:              subscriptionId,
		NotificationURI: req.NotificationURI,
		NotifCorrId:     req.NotifCorrId,
		EventSubs:       req.EventSubscriptions,
		EvtReq:          req.EvtReq,
		IsActive:        true,
	}

	if req.EvtReq != nil {
		subscription.NotifMethod = string(req.EvtReq.NotifMethod)
		subscription.RepPeriod = req.EvtReq.RepPeriod
		subscription.MaxReportNbr = req.EvtReq.MaxReportNbr
		if req.EvtReq.MonDur != nil {
			monDur := *req.EvtReq.MonDur
			subscription.MonDur = &monDur
		}
	}

	return subscription
}

func buildSubscriptionResponse(
	req *models.NnwdafEventsSubscription,
	failEventReports []models.FailureEventInfo,
) *models.NnwdafEventsSubscription {
	response := &models.NnwdafEventsSubscription{
		EventSubscriptions: req.EventSubscriptions,
		NotificationURI:    req.NotificationURI,
		NotifCorrId:        req.NotifCorrId,
		EvtReq:             req.EvtReq,
	}

	if len(failEventReports) > 0 {
		response.FailEventReports = failEventReports
	}

	return response
}

func (p *Processor) cleanupMlModelState(subscriptionId string) {
	if err := p.anlf.ReleaseSubscriptionRuntime(subscriptionId); err != nil {
		logger.ProcLog.Warnf("ReleaseAnlfRuntime failed: sub=%s err=%v", subscriptionId, err)
	}
}

func (p *Processor) prepareInitialAnalyticsRuntime(
	subscription *nwdaf_context.Subscription,
) *models.ProblemDetails {
	var event models.NwdafEvent
	for i := range subscription.EventSubs {
		eventSubscription := &subscription.EventSubs[i]
		if eventSubscription.Event == models.NwdafEvent_UE_COMMUNICATION {
			event = eventSubscription.Event
			break
		}
	}
	if event == "" {
		return nil
	}
	cfg := p.config()
	mtlfEndpoint := ""
	if cfg != nil && cfg.Configuration != nil && cfg.Configuration.ExternalMtlf != nil &&
		len(cfg.Configuration.ExternalMtlf.Endpoints) > 0 {
		mtlfEndpoint = cfg.Configuration.ExternalMtlf.Endpoints[0]
	}
	nwdaf_context.GetSelf().SetMlModelInfo(
		subscription.ID,
		nwdaf_context.NewMlModelInfo(event, mtlfEndpoint),
	)
	response, err := p.anlf.ApplyInitialSubscriptionRuntime(subscription)
	if err != nil {
		if releaseErr := p.anlf.ReleaseSubscriptionRuntime(subscription.ID); releaseErr != nil {
			logger.ProcLog.Warnf(
				"Release failed backend runtime after initial apply error: sub=%s err=%v",
				subscription.ID,
				releaseErr,
			)
		}
		return analyticsRuntimeUnavailableProblem()
	}
	subscription.SetRuntime(
		response.RuntimeRevision,
		nwdaf_context.CollectionRequirements{
			SamplingIntervalSeconds: response.CollectionRequirements.SamplingIntervalSeconds,
			RequiredMeasurements:    response.CollectionRequirements.RequiredMeasurements,
		},
		nil,
	)
	return nil
}

func analyticsRuntimeUnavailableProblem() *models.ProblemDetails {
	return &models.ProblemDetails{
		Status: http.StatusServiceUnavailable,
		Title:  http.StatusText(http.StatusServiceUnavailable),
		Detail: "UE_COMMUNICATION analytics runtime is unavailable",
	}
}

// validateSubscriptionRequest is the unified validation entry point for Create/Update
// Validates hard failures only - soft failures are handled separately by collectFailEventReports
func (p *Processor) validateSubscriptionRequest(req *models.NnwdafEventsSubscription) *models.ProblemDetails {
	// 1. Basic structure validation
	if err := p.validateBasicStructure(req); err != nil {
		return err
	}

	// 2. Validate each event subscription
	for i := range req.EventSubscriptions {
		if err := p.validateEventSubscription(i, &req.EventSubscriptions[i]); err != nil {
			return err
		}
	}

	// 3. Validate evtReq (ReportingInformation)
	return p.validateEvtReq(req.EvtReq)
}

// applyAndValidateDefaults applies default values and validates notification method
// Per TS 29.520: notificationMethod defaults to THRESHOLD when omitted
// Priority: evtReq fields > EventSubscription fields
func (p *Processor) applyAndValidateDefaults(req *models.NnwdafEventsSubscription) *models.ProblemDetails {
	// Determine effective NotifMethod (evtReq takes priority)
	effectiveNotifMethod := ""
	if req.EvtReq != nil && req.EvtReq.NotifMethod != "" {
		effectiveNotifMethod = string(req.EvtReq.NotifMethod)
	}

	// If no NotifMethod specified, defaults to THRESHOLD (per TS 29.520)
	if effectiveNotifMethod == "" {
		// TODO: Implement THRESHOLD notification method
		return &models.ProblemDetails{
			Status: http.StatusNotImplemented,
			Cause:  "THRESHOLD_NOT_IMPLEMENTED",
			Detail: "notificationMethod defaults to THRESHOLD which is not yet implemented. " +
				"Please specify evtReq.notifMethod as PERIODIC.",
		}
	}

	// Validate NotifMethod
	switch effectiveNotifMethod {
	case string(models.NwdafEventsSubscriptionNotificationMethod_PERIODIC):
		// PERIODIC requires repPeriod
		effectiveRepPeriod := int32(0)
		if req.EvtReq != nil && req.EvtReq.RepPeriod > 0 {
			effectiveRepPeriod = req.EvtReq.RepPeriod
		}
		if effectiveRepPeriod <= 0 {
			return &models.ProblemDetails{
				Status: http.StatusBadRequest,
				Cause:  "INVALID_REQUEST",
				Detail: "PERIODIC notificationMethod requires evtReq.repPeriod > 0",
			}
		}

	case string(models.NwdafEventsSubscriptionNotificationMethod_THRESHOLD):
		// TODO: Implement THRESHOLD notification method
		return &models.ProblemDetails{
			Status: http.StatusNotImplemented,
			Cause:  "THRESHOLD_NOT_IMPLEMENTED",
			Detail: "THRESHOLD notificationMethod is not yet implemented",
		}

	default:
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "UNSUPPORTED_NOTIF_METHOD",
			Detail: fmt.Sprintf("Unsupported notificationMethod: %s", effectiveNotifMethod),
		}
	}

	// MaxReportNbr: 0 means unlimited (no validation needed)
	// MonDur: nil means no expiry (no validation needed)

	return nil
}

// validateBasicStructure validates required fields in the subscription request
func (p *Processor) validateBasicStructure(req *models.NnwdafEventsSubscription) *models.ProblemDetails {
	if req == nil {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "Request body is empty",
		}
	}

	if len(req.EventSubscriptions) == 0 {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "eventSubscriptions is required",
		}
	}

	if req.NotificationURI == "" {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "notificationURI is required",
		}
	}

	return nil
}

// validateEventSubscription validates a single event subscription including target period
func (p *Processor) validateEventSubscription(
	index int,
	eventSub *models.NwdafEventsSubscriptionEventSubscription,
) *models.ProblemDetails {
	if eventSub.Event == "" {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: fmt.Sprintf("event is required in eventSubscriptions[%d]", index),
		}
	}

	// Note: Event type support is checked in collectFailEventReports (soft failure)
	// Here we only validate if it's a supported event that has additional requirements

	// For ABNORMAL_BEHAVIOUR, validate all requirements including ExceptionId/exptAnaType
	if eventSub.Event == models.NwdafEvent_ABNORMAL_BEHAVIOUR {
		// Basic requirements (tgtUe, mutual exclusion, etc.)
		if err := p.validateAbnormalBehaviourBasic(eventSub); err != nil {
			return err
		}

		// ExceptionId support check → 400 rejection
		if len(eventSub.ExcepRequs) > 0 {
			if err := p.validateSupportedExceptionIds(eventSub.ExcepRequs); err != nil {
				return err
			}
		}

		// exptAnaType support check → 400 rejection
		if eventSub.ExptAnaType != "" {
			if err := p.validateExptAnaType(eventSub.ExptAnaType); err != nil {
				return err
			}
		}
	}

	// For UE_COMMUNICATION, validate tgtUe requirements
	if eventSub.Event == models.NwdafEvent_UE_COMMUNICATION {
		if err := p.validateUeCommunication(eventSub); err != nil {
			return err
		}
	}

	// Validate analytics target period
	if err := p.validateEventTargetPeriod(index, eventSub); err != nil {
		return err
	}

	return nil
}

// validateEventTargetPeriod validates startTs/endTs for a single event subscription
func (p *Processor) validateEventTargetPeriod(
	index int,
	eventSub *models.NwdafEventsSubscriptionEventSubscription,
) *models.ProblemDetails {
	if eventSub.ExtraReportReq == nil {
		return nil
	}

	startTs := eventSub.ExtraReportReq.StartTs
	endTs := eventSub.ExtraReportReq.EndTs

	// Both must be present to validate
	if startTs == nil || endTs == nil {
		return nil
	}

	now := time.Now()

	// startTs in past + endTs in future → reject
	if startTs.Before(now) && endTs.After(now) {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "BOTH_STAT_PRED_NOT_ALLOWED",
			Detail: fmt.Sprintf("eventSubscriptions[%d]: analytics target period with startTs in past "+
				"and endTs in future is not allowed", index),
		}
	}

	// startTs > endTs → invalid
	if startTs.After(*endTs) {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: fmt.Sprintf("eventSubscriptions[%d]: startTs must be before endTs", index),
		}
	}

	return nil
}

// collectFailEventReports collects FailureEventInfo for unsupported event types
// Note: This is event-level (NwdafEvent), NOT ExceptionId-level per TS 29.520 §5.1.6.2.2
func (p *Processor) collectFailEventReports(
	eventSubs []models.NwdafEventsSubscriptionEventSubscription,
) []models.FailureEventInfo {
	var failReports []models.FailureEventInfo

	for i := range eventSubs {
		eventSub := &eventSubs[i]
		// Check if event type is supported (soft failure)
		if !p.isEventSupported(eventSub.Event) {
			failReports = append(failReports, models.FailureEventInfo{
				Event:       eventSub.Event,
				FailureCode: models.NwdafFailureCode_OTHER,
			})
		}
	}

	return failReports
}

// isEventSupported checks if an event type is supported (returns bool instead of ProblemDetails)
func (p *Processor) isEventSupported(event models.NwdafEvent) bool {
	for _, supported := range supportedEvents {
		if event == supported {
			return true
		}
	}
	return false
}

var supportedEvents = []models.NwdafEvent{
	models.NwdafEvent_UE_COMMUNICATION,
}

// validateSupportedEvent checks if the event type is supported (kept for backward compatibility)
func (p *Processor) validateSupportedEvent(event models.NwdafEvent) *models.ProblemDetails {
	if p.isEventSupported(event) {
		return nil
	}
	return &models.ProblemDetails{
		Status: http.StatusBadRequest,
		Cause:  "UNSUPPORTED_EVENT",
		Detail: fmt.Sprintf("Event type %s is not supported. Supported events: %v", event, supportedEvents),
	}
}

// Mobility-related exception IDs
var mobilityExceptionIds = []models.ExceptionId{
	models.ExceptionId_UNEXPECTED_UE_LOCATION,
	models.ExceptionId_PING_PONG_ACROSS_CELLS,
	models.ExceptionId_UNEXPECTED_WAKEUP,
	models.ExceptionId_UNEXPECTED_RADIO_LINK_FAILURES,
}

// Communication-related exception IDs
var communExceptionIds = []models.ExceptionId{
	models.ExceptionId_UNEXPECTED_LONG_LIVE_FLOW,
	models.ExceptionId_UNEXPECTED_LARGE_RATE_FLOW,
	models.ExceptionId_SUSPICION_OF_DDOS_ATTACK,
	models.ExceptionId_WRONG_DESTINATION_ADDRESS,
	models.ExceptionId_TOO_FREQUENT_SERVICE_ACCESS,
}

// validateAbnormalBehaviourBasic validates basic ABNORMAL_BEHAVIOUR requirements
// Does NOT check ExceptionId/exptAnaType support - those are handled as soft failures
func (p *Processor) validateAbnormalBehaviourBasic(
	eventSub *models.NwdafEventsSubscriptionEventSubscription,
) *models.ProblemDetails {
	// Must have target UE information
	if eventSub.TgtUe == nil {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "tgtUe is required for ABNORMAL_BEHAVIOUR",
		}
	}

	// tgtUe must have supis, intGroupIds, or anyUe=true
	hasTarget := len(eventSub.TgtUe.Supis) > 0 ||
		len(eventSub.TgtUe.IntGroupIds) > 0 ||
		eventSub.TgtUe.AnyUe

	if !hasTarget {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "tgtUe must contain supis, intGroupIds, or anyUe=true",
		}
	}

	// Check excepRequs and exptAnaType
	hasExcepRequs := len(eventSub.ExcepRequs) > 0
	hasExptAnaType := eventSub.ExptAnaType != ""

	// Must have either excepRequs or exptAnaType (but not both)
	if !hasExcepRequs && !hasExptAnaType {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "excepRequs or exptAnaType is required for ABNORMAL_BEHAVIOUR",
		}
	}

	// excepRequs and exptAnaType are mutually exclusive
	if hasExcepRequs && hasExptAnaType {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "excepRequs and exptAnaType cannot be provided together",
		}
	}

	// If anyUe=true, validate additional requirements
	if eventSub.TgtUe.AnyUe {
		if err := p.validateAnyUeRequirements(eventSub); err != nil {
			return err
		}
	}

	return nil
}

// validateUeCommunication validates UE_COMMUNICATION specific requirements
// Per TS 29.520 §4.2: tgtUe with supis or intGroupIds is REQUIRED
func (p *Processor) validateUeCommunication(
	eventSub *models.NwdafEventsSubscriptionEventSubscription,
) *models.ProblemDetails {
	// Rule: tgtUe is required
	if eventSub.TgtUe == nil {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "tgtUe is required for UE_COMMUNICATION",
		}
	}
	// Rule: supis or intGroupIds must be present
	if len(eventSub.TgtUe.Supis) == 0 && len(eventSub.TgtUe.IntGroupIds) == 0 {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "tgtUe must contain supis or intGroupIds for UE_COMMUNICATION",
		}
	}

	return nil
}

// validateAnyUeRequirements validates requirements when anyUe=true
func (p *Processor) validateAnyUeRequirements(
	eventSub *models.NwdafEventsSubscriptionEventSubscription,
) *models.ProblemDetails {
	isMobility := p.isMobilityRelated(eventSub)
	isCommun := p.isCommunRelated(eventSub)

	// For anyUe, cannot request both mobility and communication at the same time
	if isMobility && isCommun {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "when anyUe=true, cannot request both mobility and communication related analytics",
		}
	}

	// Check if has networkArea or snssais
	hasNetworkArea := eventSub.NetworkArea != nil
	hasSnssais := len(eventSub.Snssaia) > 0
	hasAppIds := len(eventSub.AppIds) > 0
	hasDnns := len(eventSub.Dnns) > 0

	// Mobility-related: requires networkArea or snssais
	if isMobility && !hasNetworkArea && !hasSnssais {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "when anyUe=true with mobility-related analytics, networkArea or snssais is required",
		}
	}

	// Communication-related: requires networkArea, appIds, dnns, or snssais
	if isCommun && !hasNetworkArea && !hasAppIds && !hasDnns && !hasSnssais {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "when anyUe=true with communication-related analytics, networkArea, appIds, dnns, or snssais is required",
		}
	}

	return nil
}

// validateEvtReq validates the ReportingInformation (evtReq) field
func (p *Processor) validateEvtReq(evtReq *models.ReportingInformation) *models.ProblemDetails {
	if evtReq == nil {
		return nil // Optional field
	}

	// Validate repPeriod for PERIODIC
	if evtReq.NotifMethod == models.SmfEventExposureNotificationMethod_PERIODIC {
		if evtReq.RepPeriod <= 0 {
			return &models.ProblemDetails{
				Status: http.StatusBadRequest,
				Cause:  "INVALID_REQUEST",
				Detail: "repPeriod is required and must be > 0 for PERIODIC notifMethod",
			}
		}
	}

	// Validate maxReportNbr if present
	if evtReq.MaxReportNbr < 0 {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_REQUEST",
			Detail: "maxReportNbr must be >= 0",
		}
	}

	return nil
}

// Supported exception IDs (currently only SUSPICION_OF_DDOS_ATTACK)
var supportedExceptionIds = []models.ExceptionId{
	models.ExceptionId_SUSPICION_OF_DDOS_ATTACK,
}

// validateSupportedExceptionIds checks if the exception IDs are supported
func (p *Processor) validateSupportedExceptionIds(excepRequs []models.Exception) *models.ProblemDetails {
	for _, excep := range excepRequs {
		supported := false
		for _, supportedId := range supportedExceptionIds {
			if excep.ExcepId == supportedId {
				supported = true
				break
			}
		}
		if !supported {
			return &models.ProblemDetails{
				Status: http.StatusBadRequest,
				Cause:  "UNSUPPORTED_EXCEPTION",
				Detail: fmt.Sprintf("ExceptionId %s is not supported. Only SUSPICION_OF_DDOS_ATTACK is supported", excep.ExcepId),
			}
		}
	}
	return nil
}

// validateExptAnaType validates that only COMMUN analytics type is supported
func (p *Processor) validateExptAnaType(exptAnaType models.ExpectedAnalyticsType) *models.ProblemDetails {
	if exptAnaType != "" && exptAnaType != models.ExpectedAnalyticsType_COMMUN {
		return &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "UNSUPPORTED_ANALYTICS_TYPE",
			Detail: "Only COMMUN analytics type is supported for DDoS detection",
		}
	}
	return nil
}

// isMobilityRelated checks if the subscription is mobility-related
func (p *Processor) isMobilityRelated(eventSub *models.NwdafEventsSubscriptionEventSubscription) bool {
	// Check exptAnaType
	if eventSub.ExptAnaType == models.ExpectedAnalyticsType_MOBILITY ||
		eventSub.ExptAnaType == models.ExpectedAnalyticsType_MOBILITY_AND_COMMUN {
		return true
	}

	// Check excepRequs
	for _, excep := range eventSub.ExcepRequs {
		for _, mobilityId := range mobilityExceptionIds {
			if excep.ExcepId == mobilityId {
				return true
			}
		}
	}

	return false
}

// isCommunRelated checks if the subscription is communication-related
func (p *Processor) isCommunRelated(eventSub *models.NwdafEventsSubscriptionEventSubscription) bool {
	// Check exptAnaType
	if eventSub.ExptAnaType == models.ExpectedAnalyticsType_COMMUN ||
		eventSub.ExptAnaType == models.ExpectedAnalyticsType_MOBILITY_AND_COMMUN {
		return true
	}

	// Check excepRequs
	for _, excep := range eventSub.ExcepRequs {
		for _, communId := range communExceptionIds {
			if excep.ExcepId == communId {
				return true
			}
		}
	}

	return false
}

// cleanupDataCollection removes SMF subscriptions when reference count reaches 0
// Per free5gc pattern: keep related processor methods in same file
// This method is called when a NWDAF subscription is deleted
func (p *Processor) cleanupDataCollection(subscriptionId string) {
	ctx := nwdaf_context.GetSelf()

	// Get all resources tracked for this NWDAF subscription
	resources := ctx.GetNwdafSubResources(subscriptionId)

	if len(resources) == 0 {
		logger.ProcLog.Debugf("CleanupDataCollection: no resources sub=%s", subscriptionId)
		return
	}

	// Release each SMF subscription
	for _, res := range resources {
		p.releaseDataCollectionResource(subscriptionId, res)
	}

	// Delete cleanup tracking for this subscription
	ctx.DeleteNwdafSubResources(subscriptionId)

	logger.ProcLog.Infof("CleanupDataCollection: completed sub=%s resources=%d",
		subscriptionId, len(resources))
}

func (p *Processor) releaseDataCollectionResource(
	subscriptionID string,
	resource nwdaf_context.NwdafSubResource,
) {
	ctx := nwdaf_context.GetSelf()
	shouldDelete, smfSubscription := ctx.ReleaseSmfSubscription(resource.CorrelationId, subscriptionID)
	if !shouldDelete || smfSubscription == nil {
		return
	}
	ctx.DeleteTrafficBucket(resource.CorrelationId)
	ctx.DeleteAdrfSmfInfo(resource.CorrelationId)
	consumer := p.nwdaf.Consumer()
	if consumer == nil {
		return
	}
	_, smfSubscriptionID, _ := smfSubscription.GetInfo()
	if err := consumer.UnsubscribeFromSmf(
		p.nwdaf.CancelContext(),
		resource.SmfEndpoint,
		smfSubscriptionID,
	); err != nil {
		logger.ProcLog.Errorf(
			"DeleteSmfSubscription failed: corr=%s sub=%s err=%v",
			resource.CorrelationId,
			smfSubscriptionID,
			err,
		)
		return
	}
	logger.ProcLog.Infof(
		"DeleteSmfSubscription: deleted corr=%s sub=%s",
		resource.CorrelationId,
		smfSubscriptionID,
	)
}
