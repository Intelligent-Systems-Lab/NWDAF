// Package notifier provides analytics handlers for different event types
package notifier

import (
	"github.com/free5gc/openapi/models"
)

// AnalyticsHandler builds event notifications for a specific event type
type AnalyticsHandler interface {
	BuildEventNotification(eventSub *models.NwdafEventsSubscriptionEventSubscription) models.NwdafEventsSubscriptionEventNotification
}

// analyticsHandlers maps event types to their handlers
var analyticsHandlers = map[models.NwdafEvent]AnalyticsHandler{
	models.NwdafEvent_UE_COMMUNICATION:   &UeCommunicationHandler{},
	models.NwdafEvent_ABNORMAL_BEHAVIOUR: &AbnormalBehaviourHandler{},
}

// GetHandler returns the handler for the given event type
func GetHandler(event models.NwdafEvent) (AnalyticsHandler, bool) {
	handler, ok := analyticsHandlers[event]
	return handler, ok
}

// UeCommunicationHandler handles UE_COMMUNICATION analytics
type UeCommunicationHandler struct{}

func (h *UeCommunicationHandler) BuildEventNotification(
	eventSub *models.NwdafEventsSubscriptionEventSubscription,
) models.NwdafEventsSubscriptionEventNotification {
	return models.NwdafEventsSubscriptionEventNotification{
		Event:   eventSub.Event,
		UeComms: []models.UeCommunication{generateUeCommunicationAnalytics(eventSub)},
	}
}

// AbnormalBehaviourHandler handles ABNORMAL_BEHAVIOUR analytics
type AbnormalBehaviourHandler struct{}

func (h *AbnormalBehaviourHandler) BuildEventNotification(
	eventSub *models.NwdafEventsSubscriptionEventSubscription,
) models.NwdafEventsSubscriptionEventNotification {
	return models.NwdafEventsSubscriptionEventNotification{
		Event:        eventSub.Event,
		AbnorBehavrs: generateMockAbnormalBehaviours(),
	}
}
