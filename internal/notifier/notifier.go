// Package notifier provides subscription notification functionality for NWDAF
package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/free5gc/nwdaf/internal/anlf"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

// NotificationScheduler manages periodic notifications for subscriptions
type NotificationScheduler struct {
	baseCtx         context.Context
	cfg             *factory.Config
	anlfBackend     anlf.AnlfBackendAPI
	httpClient      *http.Client
	subscriptionId  string
	notificationURI string
	repPeriod       int32 // seconds
	eventSubs       []models.NwdafEventsSubscriptionEventSubscription

	// Notification control fields
	notifCorrId  string     // Notification correlation ID
	maxReportNbr int32      // Maximum number of reports (0 = unlimited)
	monDur       *time.Time // Monitoring duration expiry time
	reportCount  int32      // Current report count

	// Completion callback (called when scheduler stops due to limits)
	onComplete func(subscriptionId string, reason string)

	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
}

// NewNotificationScheduler creates a new scheduler for a subscription
func NewNotificationScheduler(
	baseCtx context.Context,
	cfg *factory.Config,
	anlfBackend anlf.AnlfBackendAPI,
	subscriptionId string,
	notificationURI string,
	repPeriod int32,
	eventSubs []models.NwdafEventsSubscriptionEventSubscription,
	notifCorrId string,
	maxReportNbr int32,
	monDur *time.Time,
	onComplete func(subscriptionId string, reason string),
) *NotificationScheduler {
	return &NotificationScheduler{
		baseCtx:     baseCtx,
		cfg:         cfg,
		anlfBackend: anlfBackend,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        20,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		subscriptionId:  subscriptionId,
		notificationURI: notificationURI,
		repPeriod:       repPeriod,
		eventSubs:       eventSubs,
		notifCorrId:     notifCorrId,
		maxReportNbr:    maxReportNbr,
		monDur:          monDur,
		onComplete:      onComplete,
	}
}

// Start begins the periodic notification loop
func (s *NotificationScheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cancel != nil {
		return // Already running
	}

	ctx, cancel := context.WithCancel(s.baseCtx)
	s.cancel = cancel

	s.wg.Add(1)
	go s.run(ctx)

	// Log scheduler start with control parameters
	monDurStr := "nil"
	if s.monDur != nil {
		monDurStr = s.monDur.Format(time.RFC3339)
	}
	logger.NotifierLog.Infof(
		"NotificationScheduler: started sub=%s period=%ds maxReports=%d monDur=%s",
		s.subscriptionId, s.repPeriod, s.maxReportNbr, monDurStr)
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
	logger.NotifierLog.Infof("NotificationScheduler: stopped sub=%s sent=%d",
		s.subscriptionId, s.reportCount)
}

// run is the main notification loop
func (s *NotificationScheduler) run(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(time.Duration(s.repPeriod) * time.Second)
	defer ticker.Stop()

	select {
	case <-ctx.Done():
		return
	default:
	}

	// Send first notification immediately (if allowed)
	if s.shouldContinue() {
		s.sendNotification(ctx)
	} else {
		s.handleCompletion("LIMIT_REACHED_BEFORE_START")
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.shouldContinue() {
				s.handleCompletion("LIMIT_REACHED")
				return
			}
			s.sendNotification(ctx)
		}
	}
}

// shouldContinue checks if notification should continue based on maxReportNbr and monDur
func (s *NotificationScheduler) shouldContinue() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check maxReportNbr limit (0 means unlimited)
	if s.maxReportNbr > 0 && s.reportCount >= s.maxReportNbr {
		logger.NotifierLog.Debugf("Subscription %s: reached maxReportNbr limit (%d/%d)",
			s.subscriptionId, s.reportCount, s.maxReportNbr)
		return false
	}

	// Check monDur expiry
	if s.monDur != nil && time.Now().After(*s.monDur) {
		logger.NotifierLog.Debugf("Subscription %s: monitoring duration expired at %s",
			s.subscriptionId, s.monDur.Format(time.RFC3339))
		return false
	}

	return true
}

// handleCompletion handles scheduler completion and invokes callback
func (s *NotificationScheduler) handleCompletion(reason string) {
	logger.NotifierLog.Infof("NotificationScheduler: completed sub=%s reason=%s sent=%d",
		s.subscriptionId, reason, s.reportCount)

	if s.onComplete != nil {
		// Call callback asynchronously to avoid blocking
		go s.onComplete(s.subscriptionId, reason)
	}
}

// sendNotification sends a notification to the consumer
func (s *NotificationScheduler) sendNotification(parentCtx context.Context) {
	// Increment report count
	s.mu.Lock()
	s.reportCount++
	currentCount := s.reportCount
	s.mu.Unlock()

	notification := s.buildNotification(parentCtx)

	// Convert to output format (without omitempty for 0 values)
	output := s.convertToOutput(notification)

	// Per TS 29.520 §5.1.2.2.4: notification body MUST be array of NnwdafEventsSubscriptionNotification
	notificationList := NotificationListOutput{output}
	jsonData, err := json.Marshal(notificationList)
	if err != nil {
		logger.NotifierLog.Errorf("Failed to marshal notification: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(parentCtx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.notificationURI, bytes.NewBuffer(jsonData))
	if err != nil {
		logger.NotifierLog.Errorf("Failed to create request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		logger.NotifierLog.Warnf("Failed to send notification to %s: %v", s.notificationURI, err)
		return
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.NotifierLog.Debugf("failed to close response body (may be ignored): %v", closeErr)
		}
	}()

	if resp.StatusCode == http.StatusNoContent {
		logger.NotifierLog.Debugf("SendNotification: sent sub=%s report=%d",
			s.subscriptionId, currentCount)
	} else {
		logger.NotifierLog.Warnf("SendNotification: unexpected-status sub=%s report=%d status=%d",
			s.subscriptionId, currentCount, resp.StatusCode)
	}
}

// convertToOutput converts the notification to output format with proper 0-value serialization
// Uses struct embedding: copies original struct and overrides specific fields
func (s *NotificationScheduler) convertToOutput(n models.NnwdafEventsSubscriptionNotification) NotificationOutput {
	output := NotificationOutput{
		NnwdafEventsSubscriptionNotification: n, // Embed original
	}

	for _, eventNotif := range n.EventNotifications {
		eventOutput := EventNotificationOutput{
			NwdafEventsSubscriptionEventNotification: eventNotif, // Embed original
			Event:                                    string(eventNotif.Event),
		}

		// Convert UeCommunication to output format
		for _, ueComm := range eventNotif.UeComms {
			ueOutput := UeCommunicationOutput{
				UeCommunication: ueComm,            // Embed original
				Confidence:      ueComm.Confidence, // Override (no omitempty)
			}

			if ueComm.TrafChar != nil {
				ueOutput.TrafChar = &TrafficCharacterizationOutput{
					TrafficCharacterization: *ueComm.TrafChar,      // Embed original
					UlVol:                   ueComm.TrafChar.UlVol, // Override (no omitempty)
					DlVol:                   ueComm.TrafChar.DlVol, // Override (no omitempty)
				}
			}

			eventOutput.UeComms = append(eventOutput.UeComms, ueOutput)
		}

		output.EventNotifications = append(output.EventNotifications, eventOutput)
	}

	return output
}

// buildNotification builds the notification message
func (s *NotificationScheduler) buildNotification(
	parentCtx context.Context,
) models.NnwdafEventsSubscriptionNotification {
	var eventNotifications []models.NwdafEventsSubscriptionEventNotification

	for i := range s.eventSubs {
		eventSub := &s.eventSubs[i]
		if handler, ok := GetHandler(eventSub.Event); ok {
			eventNotifications = append(eventNotifications, handler.BuildEventNotification(
				parentCtx,
				s.subscriptionId,
				eventSub,
				s.cfg,
				s.anlfBackend,
			))
		}
	}

	notification := models.NnwdafEventsSubscriptionNotification{
		SubscriptionId:     s.subscriptionId,
		EventNotifications: eventNotifications,
	}

	// Include notifCorrId if provided
	if s.notifCorrId != "" {
		notification.NotifCorrId = s.notifCorrId
	}

	return notification
}
