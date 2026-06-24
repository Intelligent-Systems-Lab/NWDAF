package notifier

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/free5gc/openapi/models"
)

func waitUntil(t *testing.T, timeout time.Duration, condition func() bool, failure string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal(failure)
}

// TestShouldContinue_MaxReportNbrLimit tests that shouldContinue returns false when maxReportNbr is reached
func TestShouldContinue_MaxReportNbrLimit(t *testing.T) {
	tests := []struct {
		name         string
		maxReportNbr int32
		reportCount  int32
		expected     bool
	}{
		{
			name:         "MaxReportNbr not reached",
			maxReportNbr: 5,
			reportCount:  3,
			expected:     true,
		},
		{
			name:         "MaxReportNbr reached",
			maxReportNbr: 5,
			reportCount:  5,
			expected:     false,
		},
		{
			name:         "MaxReportNbr exceeded",
			maxReportNbr: 5,
			reportCount:  6,
			expected:     false,
		},
		{
			name:         "MaxReportNbr zero means unlimited",
			maxReportNbr: 0,
			reportCount:  100,
			expected:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheduler := &NotificationScheduler{
				maxReportNbr: tt.maxReportNbr,
				reportCount:  tt.reportCount,
			}
			result := scheduler.shouldContinue()
			if result != tt.expected {
				t.Errorf("shouldContinue() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

// TestShouldContinue_MonDurExpiry tests that shouldContinue returns false when monDur is expired
func TestShouldContinue_MonDurExpiry(t *testing.T) {
	now := time.Now()
	pastTime := now.Add(-1 * time.Hour)
	futureTime := now.Add(1 * time.Hour)

	tests := []struct {
		name     string
		monDur   *time.Time
		expected bool
	}{
		{
			name:     "MonDur not expired",
			monDur:   &futureTime,
			expected: true,
		},
		{
			name:     "MonDur expired",
			monDur:   &pastTime,
			expected: false,
		},
		{
			name:     "MonDur nil means no limit",
			monDur:   nil,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheduler := &NotificationScheduler{
				maxReportNbr: 0, // unlimited
				monDur:       tt.monDur,
			}
			result := scheduler.shouldContinue()
			if result != tt.expected {
				t.Errorf("shouldContinue() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

// TestShouldContinue_CombinedLimits tests that shouldContinue handles both limits together
func TestShouldContinue_CombinedLimits(t *testing.T) {
	now := time.Now()
	futureTime := now.Add(1 * time.Hour)
	pastTime := now.Add(-1 * time.Hour)

	tests := []struct {
		name         string
		maxReportNbr int32
		reportCount  int32
		monDur       *time.Time
		expected     bool
	}{
		{
			name:         "Both limits OK",
			maxReportNbr: 10,
			reportCount:  5,
			monDur:       &futureTime,
			expected:     true,
		},
		{
			name:         "maxReportNbr reached, monDur OK",
			maxReportNbr: 5,
			reportCount:  5,
			monDur:       &futureTime,
			expected:     false,
		},
		{
			name:         "maxReportNbr OK, monDur expired",
			maxReportNbr: 10,
			reportCount:  5,
			monDur:       &pastTime,
			expected:     false,
		},
		{
			name:         "Both limits exceeded",
			maxReportNbr: 5,
			reportCount:  10,
			monDur:       &pastTime,
			expected:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheduler := &NotificationScheduler{
				maxReportNbr: tt.maxReportNbr,
				reportCount:  tt.reportCount,
				monDur:       tt.monDur,
			}
			result := scheduler.shouldContinue()
			if result != tt.expected {
				t.Errorf("shouldContinue() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

// TestBuildNotification_NotifCorrId tests that notifCorrId is included in notification
func TestBuildNotification_NotifCorrId(t *testing.T) {
	tests := []struct {
		name              string
		notifCorrId       string
		expectNotifCorrId bool
	}{
		{
			name:              "NotifCorrId included when set",
			notifCorrId:       "test-correlation-123",
			expectNotifCorrId: true,
		},
		{
			name:              "NotifCorrId not included when empty",
			notifCorrId:       "",
			expectNotifCorrId: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheduler := &NotificationScheduler{
				subscriptionId: "test-sub-id",
				notifCorrId:    tt.notifCorrId,
				eventSubs: []models.NwdafEventsSubscriptionEventSubscription{
					{
						Event: models.NwdafEvent_ABNORMAL_BEHAVIOUR,
					},
				},
			}

			notification := scheduler.buildNotification()

			if tt.expectNotifCorrId {
				if notification.NotifCorrId != tt.notifCorrId {
					t.Errorf("NotifCorrId = %v, expected %v", notification.NotifCorrId, tt.notifCorrId)
				}
			} else {
				if notification.NotifCorrId != "" {
					t.Errorf("NotifCorrId should be empty, got %v", notification.NotifCorrId)
				}
			}
		})
	}
}

// TestBuildNotification_SubscriptionId tests that subscriptionId is always included
func TestBuildNotification_SubscriptionId(t *testing.T) {
	scheduler := &NotificationScheduler{
		subscriptionId: "test-sub-id-123",
		eventSubs: []models.NwdafEventsSubscriptionEventSubscription{
			{Event: models.NwdafEvent_ABNORMAL_BEHAVIOUR},
		},
	}

	notification := scheduler.buildNotification()

	if notification.SubscriptionId != "test-sub-id-123" {
		t.Errorf("SubscriptionId = %v, expected test-sub-id-123", notification.SubscriptionId)
	}
}

// TestHandleCompletion_Callback tests that onComplete callback is invoked
func TestHandleCompletion_Callback(t *testing.T) {
	var callbackCalled bool
	var callbackSubId string
	var callbackReason string
	var wg sync.WaitGroup
	wg.Add(1)

	scheduler := &NotificationScheduler{
		subscriptionId: "callback-test-sub",
		onComplete: func(subId string, reason string) {
			callbackCalled = true
			callbackSubId = subId
			callbackReason = reason
			wg.Done()
		},
	}

	scheduler.handleCompletion("TEST_REASON")

	// Wait for async callback with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Callback completed
	case <-time.After(1 * time.Second):
		t.Fatal("Callback was not called within timeout")
	}

	if !callbackCalled {
		t.Error("onComplete callback was not called")
	}
	if callbackSubId != "callback-test-sub" {
		t.Errorf("callbackSubId = %v, expected callback-test-sub", callbackSubId)
	}
	if callbackReason != "TEST_REASON" {
		t.Errorf("callbackReason = %v, expected TEST_REASON", callbackReason)
	}
}

// TestNewNotificationScheduler tests scheduler initialization
func TestNewNotificationScheduler(t *testing.T) {
	now := time.Now()
	monDur := now.Add(1 * time.Hour)

	eventSubs := []models.NwdafEventsSubscriptionEventSubscription{
		{Event: models.NwdafEvent_ABNORMAL_BEHAVIOUR},
	}

	onComplete := func(subId string, reason string) {}

	scheduler := NewNotificationScheduler(
		context.Background(),
		nil,
		"test-sub-id",
		"http://localhost:9090/callback",
		10,
		eventSubs,
		"test-corr-id",
		5,
		&monDur,
		onComplete,
	)

	if scheduler.subscriptionId != "test-sub-id" {
		t.Errorf("subscriptionId = %v, expected test-sub-id", scheduler.subscriptionId)
	}
	if scheduler.notificationURI != "http://localhost:9090/callback" {
		t.Errorf("notificationURI = %v, expected http://localhost:9090/callback", scheduler.notificationURI)
	}
	if scheduler.repPeriod != 10 {
		t.Errorf("repPeriod = %v, expected 10", scheduler.repPeriod)
	}
	if scheduler.notifCorrId != "test-corr-id" {
		t.Errorf("notifCorrId = %v, expected test-corr-id", scheduler.notifCorrId)
	}
	if scheduler.maxReportNbr != 5 {
		t.Errorf("maxReportNbr = %v, expected 5", scheduler.maxReportNbr)
	}
	if scheduler.monDur == nil || !scheduler.monDur.Equal(monDur) {
		t.Errorf("monDur = %v, expected %v", scheduler.monDur, monDur)
	}
	if scheduler.reportCount != 0 {
		t.Errorf("reportCount should be initialized to 0, got %v", scheduler.reportCount)
	}
}

func TestNotificationScheduler_StopsWhenParentContextCancelled(t *testing.T) {
	var requestCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	baseCtx, cancel := context.WithCancel(context.Background())
	scheduler := NewNotificationScheduler(
		baseCtx,
		nil,
		"test-sub-id",
		server.URL,
		1,
		[]models.NwdafEventsSubscriptionEventSubscription{
			{Event: models.NwdafEvent_ABNORMAL_BEHAVIOUR},
		},
		"",
		0,
		nil,
		nil,
	)

	scheduler.Start()
	defer scheduler.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for requestCount.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if requestCount.Load() == 0 {
		t.Fatal("expected initial notification to be sent")
	}

	cancel()
	waitUntil(t, 1500*time.Millisecond, func() bool {
		return requestCount.Load() >= 1
	}, "expected scheduler to send the initial notification before cancellation settles")

	stabilityDeadline := time.Now().Add(1200 * time.Millisecond)
	for time.Now().Before(stabilityDeadline) {
		if got := requestCount.Load(); got != 1 {
			t.Fatalf("expected scheduler to stop after parent cancellation, got %d requests", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
