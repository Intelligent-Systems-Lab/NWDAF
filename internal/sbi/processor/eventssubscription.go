package processor

import (
	"fmt"
	"net/http"
	"time"

	"github.com/free5gc/openapi/models"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/notifier"
)

// HandleCreateSubscription processes new subscription requests
func (p *Processor) HandleCreateSubscription(
	req *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, string, *models.ProblemDetails) {
	logger.ProcLog.Infof("Processing CreateSubscription request")

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

	logger.ProcLog.Infof("Subscription created: %s", subscriptionId)

	// Start notification scheduler for PERIODIC notifications
	if subscription.NotifMethod == string(models.NwdafEventsSubscriptionNotificationMethod_PERIODIC) && subscription.RepPeriod > 0 {
		// Create completion callback to handle scheduler termination
		onComplete := func(subId string, reason string) {
			logger.ProcLog.Infof("Subscription %s notification completed: %s", subId, reason)
			if sub := nwdaf_context.GetSelf().GetSubscription(subId); sub != nil {
				sub.IsActive = false
			}
		}

		scheduler := notifier.NewNotificationScheduler(
			subscriptionId,
			req.NotificationURI,
			subscription.RepPeriod,
			req.EventSubscriptions,
			subscription.NotifCorrId,
			subscription.MaxReportNbr,
			subscription.MonDur,
			onComplete,
		)
		scheduler.Start()
		subscription.Scheduler = scheduler
	}

	// Trigger data collection from source NFs using consumer
	// Per 3GPP TS 23.288 §6.2: NWDAF invokes Nnf_EventExposure_Subscribe to collect data
	// Execute asynchronously to avoid blocking the consumer's subscription request
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.ProcLog.Errorf("Panic in TriggerDataCollection for subscription %s: %v", subscriptionId, r)
			}
		}()
		p.TriggerDataCollection(req.EventSubscriptions, subscriptionId)
	}()

	// Prepare response
	response := &models.NnwdafEventsSubscription{
		EventSubscriptions: req.EventSubscriptions,
		NotificationURI:    req.NotificationURI,
		NotifCorrId:        req.NotifCorrId,
		EvtReq:             req.EvtReq,
	}

	// Add failEventReports if any events failed
	if len(failEventReports) > 0 {
		response.FailEventReports = failEventReports
		logger.ProcLog.Infof("Subscription created with %d failed events", len(failEventReports))
	}

	return response, subscriptionId, nil
}

// HandleUpdateSubscription processes subscription update requests
func (p *Processor) HandleUpdateSubscription(
	subscriptionId string,
	req *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, *models.ProblemDetails) {
	logger.ProcLog.Infof("Processing UpdateSubscription: %s", subscriptionId)

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

	// Stop existing scheduler before updating
	if existing.Scheduler != nil {
		existing.Scheduler.Stop()
	}

	// Update subscription
	subscription := &nwdaf_context.Subscription{
		ID:              subscriptionId,
		NotificationURI: req.NotificationURI,
		NotifCorrId:     req.NotifCorrId,
		EventSubs:       req.EventSubscriptions,
		EvtReq:          req.EvtReq,
		CreatedAt:       existing.CreatedAt,
		IsActive:        true,
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

	ctx.UpdateSubscription(subscription)

	// Start new scheduler if PERIODIC notification requested
	if subscription.NotifMethod == string(models.NwdafEventsSubscriptionNotificationMethod_PERIODIC) && subscription.RepPeriod > 0 {
		// Create completion callback to handle scheduler termination
		onComplete := func(subId string, reason string) {
			logger.ProcLog.Infof("Subscription %s notification completed: %s", subId, reason)
			if sub := nwdaf_context.GetSelf().GetSubscription(subId); sub != nil {
				sub.IsActive = false
			}
		}

		scheduler := notifier.NewNotificationScheduler(
			subscriptionId,
			req.NotificationURI,
			subscription.RepPeriod,
			req.EventSubscriptions,
			subscription.NotifCorrId,
			subscription.MaxReportNbr,
			subscription.MonDur,
			onComplete,
		)
		scheduler.Start()
		subscription.Scheduler = scheduler
	}

	logger.ProcLog.Infof("Subscription updated: %s", subscriptionId)

	// Prepare response
	response := &models.NnwdafEventsSubscription{
		EventSubscriptions: req.EventSubscriptions,
		NotificationURI:    req.NotificationURI,
		NotifCorrId:        req.NotifCorrId,
		EvtReq:             req.EvtReq,
	}

	// Add failEventReports if any events failed
	if len(failEventReports) > 0 {
		response.FailEventReports = failEventReports
		logger.ProcLog.Infof("Subscription updated with %d failed events", len(failEventReports))
	}

	return response, nil
}

// HandleDeleteSubscription processes subscription deletion requests
func (p *Processor) HandleDeleteSubscription(subscriptionId string) *models.ProblemDetails {
	logger.ProcLog.Infof("Processing DeleteSubscription: %s", subscriptionId)

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

	// Stop scheduler if running
	if subscription.Scheduler != nil {
		subscription.Scheduler.Stop()
	}

	// Cleanup SMF subscriptions and data collection resources
	p.cleanupDataCollection(subscriptionId)

	ctx.DeleteSubscription(subscriptionId)
	logger.ProcLog.Infof("Subscription deleted: %s", subscriptionId)
	return nil
}

// validateSubscriptionRequest is the unified validation entry point for Create/Update
// Validates hard failures only - soft failures are handled separately by collectFailEventReports
func (p *Processor) validateSubscriptionRequest(req *models.NnwdafEventsSubscription) *models.ProblemDetails {
	// 1. Basic structure validation
	if err := p.validateBasicStructure(req); err != nil {
		return err
	}

	// 2. Validate each event subscription
	for i, eventSub := range req.EventSubscriptions {
		if err := p.validateEventSubscription(i, &eventSub); err != nil {
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
			Detail: "notificationMethod defaults to THRESHOLD which is not yet implemented. Please specify evtReq.notifMethod as PERIODIC.",
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
			Detail: fmt.Sprintf("eventSubscriptions[%d]: analytics target period with startTs in past and endTs in future is not allowed", index),
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

	for _, eventSub := range eventSubs {
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
	consumer := p.nwdaf.Consumer()

	// Get all resources tracked for this NWDAF subscription
	resources := ctx.GetNwdafSubResources(subscriptionId)

	if len(resources) == 0 {
		logger.ProcLog.Debugf("No resources to cleanup for subscription: %s", subscriptionId)
		return
	}

	// Release each SMF resource
	for _, res := range resources {
		shouldDelete, smfResource := ctx.ReleaseSmfResource(
			res.SmfEndpoint,
			res.Supi,
			subscriptionId,
		)

		if shouldDelete && smfResource != nil {
			// Last reference - delete notification routing
			ctx.DeleteCorrelationToSupi(res.CorrelationId)

			// Unsubscribe from SMF
			if consumer != nil {
				_, smfSubId, _ := smfResource.GetInfo()
				err := consumer.UnsubscribeFromSmf(res.SmfEndpoint, smfSubId)
				if err != nil {
					logger.ProcLog.Errorf("Failed to unsubscribe from SMF: %v", err)
				} else {
					logger.ProcLog.Infof("Unsubscribed from SMF: endpoint=%s, subId=%s",
						res.SmfEndpoint, smfSubId)
				}
			}
		}
	}

	// Delete cleanup tracking for this subscription
	ctx.DeleteNwdafSubResources(subscriptionId)

	logger.ProcLog.Infof("Data collection cleanup completed for subscription: %s", subscriptionId)
}
