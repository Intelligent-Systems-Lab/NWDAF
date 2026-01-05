package processor

import (
	"fmt"
	"net/http"

	"github.com/free5gc/openapi/models"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
)

// HandleCreateSubscription processes new subscription requests
func (p *Processor) HandleCreateSubscription(
	req *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, string, *models.ProblemDetails) {
	logger.ProcLog.Infof("Processing CreateSubscription request")

	// Phase 1: Basic validation (hard failures)
	if problemDetails := p.validateBasicSubscription(req); problemDetails != nil {
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

	// Validate request
	if problemDetails := p.validateSubscription(req); problemDetails != nil {
		return nil, problemDetails
	}

	// Update subscription
	subscription := &nwdaf_context.Subscription{
		ID:              subscriptionId,
		NotificationURI: req.NotificationURI,
		NotifCorrId:     req.NotifCorrId,
		EventSubs:       req.EventSubscriptions,
		EvtReq:          req.EvtReq,
		CreatedAt:       existing.CreatedAt,
	}

	ctx.UpdateSubscription(subscription)

	logger.ProcLog.Infof("Subscription updated: %s", subscriptionId)

	return req, nil
}

// HandleDeleteSubscription processes subscription deletion requests
func (p *Processor) HandleDeleteSubscription(subscriptionId string) *models.ProblemDetails {
	logger.ProcLog.Infof("Processing DeleteSubscription: %s", subscriptionId)

	ctx := nwdaf_context.GetSelf()

	if !ctx.DeleteSubscription(subscriptionId) {
		return &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "SUBSCRIPTION_NOT_FOUND",
			Detail: fmt.Sprintf("Subscription %s not found", subscriptionId),
		}
	}

	logger.ProcLog.Infof("Subscription deleted: %s", subscriptionId)
	return nil
}

// validateBasicSubscription validates basic subscription requirements (hard failures)
// This does NOT check ExceptionId/exptAnaType support - those are soft failures
func (p *Processor) validateBasicSubscription(req *models.NnwdafEventsSubscription) *models.ProblemDetails {
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

	// Validate each event subscription (basic checks only)
	for i, eventSub := range req.EventSubscriptions {
		if eventSub.Event == "" {
			return &models.ProblemDetails{
				Status: http.StatusBadRequest,
				Cause:  "INVALID_REQUEST",
				Detail: fmt.Sprintf("event is required in eventSubscriptions[%d]", i),
			}
		}

		// Check if event type is supported
		if err := p.validateSupportedEvent(eventSub.Event); err != nil {
			return err
		}

		// For ABNORMAL_BEHAVIOUR, validate basic requirements (not ExceptionId/exptAnaType)
		if eventSub.Event == models.NwdafEvent_ABNORMAL_BEHAVIOUR {
			if err := p.validateAbnormalBehaviourBasic(&eventSub); err != nil {
				return err
			}
		}
	}

	// Validate evtReq (ReportingInformation)
	if err := p.validateEvtReq(req.EvtReq); err != nil {
		return err
	}

	return nil
}

// collectFailEventReports collects FailureEventInfo for unsupported exceptions/analytics types
func (p *Processor) collectFailEventReports(
	eventSubs []models.NwdafEventsSubscriptionEventSubscription,
) []models.FailureEventInfo {
	var failReports []models.FailureEventInfo

	for _, eventSub := range eventSubs {
		if eventSub.Event == models.NwdafEvent_ABNORMAL_BEHAVIOUR {
			// Check ExceptionId support
			if len(eventSub.ExcepRequs) > 0 {
				if failInfo := p.checkUnsupportedExceptionIds(eventSub.ExcepRequs); failInfo != nil {
					failReports = append(failReports, *failInfo)
					continue
				}
			}

			// Check exptAnaType support
			if eventSub.ExptAnaType != "" {
				if failInfo := p.checkUnsupportedExptAnaType(eventSub.ExptAnaType); failInfo != nil {
					failReports = append(failReports, *failInfo)
					continue
				}
			}
		}
	}

	return failReports
}

// validateSubscription validates the subscription request (kept for backward compatibility)
func (p *Processor) validateSubscription(req *models.NnwdafEventsSubscription) *models.ProblemDetails {
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

	// Validate each event subscription
	for i, eventSub := range req.EventSubscriptions {
		if eventSub.Event == "" {
			return &models.ProblemDetails{
				Status: http.StatusBadRequest,
				Cause:  "INVALID_REQUEST",
				Detail: fmt.Sprintf("event is required in eventSubscriptions[%d]", i),
			}
		}

		// Check if event type is supported
		if err := p.validateSupportedEvent(eventSub.Event); err != nil {
			return err
		}

		// For ABNORMAL_BEHAVIOUR, validate specific requirements
		if eventSub.Event == models.NwdafEvent_ABNORMAL_BEHAVIOUR {
			if err := p.validateAbnormalBehaviour(&eventSub); err != nil {
				return err
			}
		}
	}

	// Validate evtReq (ReportingInformation)
	if err := p.validateEvtReq(req.EvtReq); err != nil {
		return err
	}

	return nil
}

// Supported events list
var supportedEvents = []models.NwdafEvent{
	models.NwdafEvent_ABNORMAL_BEHAVIOUR,
}

// validateSupportedEvent checks if the event type is supported
func (p *Processor) validateSupportedEvent(event models.NwdafEvent) *models.ProblemDetails {
	for _, supported := range supportedEvents {
		if event == supported {
			return nil
		}
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

// validateAbnormalBehaviour validates ABNORMAL_BEHAVIOUR specific requirements
func (p *Processor) validateAbnormalBehaviour(
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

	// Validate that ExceptionId is supported (only SUSPICION_OF_DDOS_ATTACK)
	if hasExcepRequs {
		if err := p.validateSupportedExceptionIds(eventSub.ExcepRequs); err != nil {
			return err
		}
	}

	// Validate that exptAnaType is supported (only COMMUN)
	if hasExptAnaType {
		if err := p.validateExptAnaType(eventSub.ExptAnaType); err != nil {
			return err
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

// checkUnsupportedExceptionIds checks for unsupported exception IDs and returns FailureEventInfo
// This is used for failEventReports (soft failure) instead of hard rejection
func (p *Processor) checkUnsupportedExceptionIds(excepRequs []models.Exception) *models.FailureEventInfo {
	for _, excep := range excepRequs {
		supported := false
		for _, supportedId := range supportedExceptionIds {
			if excep.ExcepId == supportedId {
				supported = true
				break
			}
		}
		if !supported {
			return &models.FailureEventInfo{
				Event:       models.NwdafEvent_ABNORMAL_BEHAVIOUR,
				FailureCode: models.NwdafFailureCode_OTHER,
			}
		}
	}
	return nil
}

// checkUnsupportedExptAnaType checks for unsupported analytics type and returns FailureEventInfo
// This is used for failEventReports (soft failure) instead of hard rejection
func (p *Processor) checkUnsupportedExptAnaType(exptAnaType models.ExpectedAnalyticsType) *models.FailureEventInfo {
	if exptAnaType != "" && exptAnaType != models.ExpectedAnalyticsType_COMMUN {
		return &models.FailureEventInfo{
			Event:       models.NwdafEvent_ABNORMAL_BEHAVIOUR,
			FailureCode: models.NwdafFailureCode_OTHER,
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
