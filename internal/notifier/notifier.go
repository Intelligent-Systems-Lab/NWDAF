// Package notifier provides subscription notification functionality for NWDAF
package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

// NotificationScheduler manages periodic notifications for subscriptions
type NotificationScheduler struct {
	subscriptionId  string
	notificationURI string
	repPeriod       int32 // seconds
	eventSubs       []models.NwdafEventsSubscriptionEventSubscription

	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
}

// NewNotificationScheduler creates a new scheduler for a subscription
func NewNotificationScheduler(
	subscriptionId string,
	notificationURI string,
	repPeriod int32,
	eventSubs []models.NwdafEventsSubscriptionEventSubscription,
) *NotificationScheduler {
	return &NotificationScheduler{
		subscriptionId:  subscriptionId,
		notificationURI: notificationURI,
		repPeriod:       repPeriod,
		eventSubs:       eventSubs,
	}
}

// Start begins the periodic notification loop
func (s *NotificationScheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cancel != nil {
		return // Already running
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	s.wg.Add(1)
	go s.run(ctx)

	logger.NotifierLog.Infof("Notification scheduler started for subscription %s (period: %ds)",
		s.subscriptionId, s.repPeriod)
}

// Stop stops the periodic notification loop
func (s *NotificationScheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}

	s.wg.Wait()
	logger.NotifierLog.Infof("Notification scheduler stopped for subscription %s", s.subscriptionId)
}

// run is the main notification loop
func (s *NotificationScheduler) run(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(time.Duration(s.repPeriod) * time.Second)
	defer ticker.Stop()

	// Send first notification immediately
	s.sendNotification()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sendNotification()
		}
	}
}

// sendNotification sends a notification to the consumer
func (s *NotificationScheduler) sendNotification() {
	notification := s.buildNotification()

	jsonData, err := json.Marshal(notification)
	if err != nil {
		logger.NotifierLog.Errorf("Failed to marshal notification: %v", err)
		return
	}

	resp, err := http.Post(s.notificationURI, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		logger.NotifierLog.Warnf("Failed to send notification to %s: %v", s.notificationURI, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		logger.NotifierLog.Infof("Notification sent successfully to %s", s.notificationURI)
	} else {
		logger.NotifierLog.Warnf("Notification response: %d from %s", resp.StatusCode, s.notificationURI)
	}
}

// buildNotification builds the notification message
func (s *NotificationScheduler) buildNotification() models.NnwdafEventsSubscriptionNotification {
	var eventNotifications []models.NwdafEventsSubscriptionEventNotification

	for _, eventSub := range s.eventSubs {
		if eventSub.Event == models.NwdafEvent_ABNORMAL_BEHAVIOUR {
			eventNotification := models.NwdafEventsSubscriptionEventNotification{
				Event:        eventSub.Event,
				AbnorBehavrs: generateMockAbnormalBehaviours(),
			}
			eventNotifications = append(eventNotifications, eventNotification)
		}
	}

	return models.NnwdafEventsSubscriptionNotification{
		SubscriptionId:     s.subscriptionId,
		EventNotifications: eventNotifications,
	}
}
