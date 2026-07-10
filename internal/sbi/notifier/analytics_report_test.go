package notifier

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

func TestReportDispatcherMapsDeliversAndDeduplicates(t *testing.T) {
	var calls atomic.Int32
	var deliveredBody string
	consumer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("ReadAll() error = %v", err)
		}
		deliveredBody = string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer consumer.Close()

	nwdaf_context.Init()
	subscription := &nwdaf_context.Subscription{
		ID: "sub-1", NotificationURI: consumer.URL, NotifCorrId: "corr-1", IsActive: true,
		EventSubs: []models.NwdafEventsSubscriptionEventSubscription{{Event: models.NwdafEvent_UE_COMMUNICATION}},
	}
	subscription.SetRuntime(3, nwdaf_context.CollectionRequirements{
		SamplingIntervalSeconds: 30,
		RequiredMeasurements:    []string{"UL_VOLUME"},
	}, nil)
	nwdaf_context.GetSelf().AddSubscription(subscription)

	dispatcher := NewReportDispatcher(context.Background(), nil)
	report := &contract.AnalyticsReport{
		ReportID: "report-1", ReportSequence: 1, RuntimeRevision: 3, GeneratedAt: time.Now(),
		EventNotifications: []contract.AnalyticsEventNotification{{
			Event: string(models.NwdafEvent_UE_COMMUNICATION),
			UeCommunications: []contract.AnalyticsUeCommunication{{
				Timestamp: time.Now(), Confidence: 0,
				TrafficCharacterization: contract.AnalyticsTrafficCharacterization{Dnn: "internet"},
			}},
		}},
	}

	if err := dispatcher.DispatchAnalyticsReport("sub-1", report); err != nil {
		t.Fatalf("DispatchAnalyticsReport() error = %v", err)
	}
	if err := dispatcher.DispatchAnalyticsReport("sub-1", report); err != nil {
		t.Fatalf("duplicate DispatchAnalyticsReport() error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("external delivery calls = %d, want 1", calls.Load())
	}
	expectedFields := []string{
		`"subscriptionId":"sub-1"`,
		`"notifCorrId":"corr-1"`,
		`"ulVol":0`,
		`"confidence":0`,
	}
	for _, expected := range expectedFields {
		if !strings.Contains(deliveredBody, expected) {
			t.Fatalf("notification body %s does not contain %s", deliveredBody, expected)
		}
	}

	stale := *report
	stale.ReportID = "report-stale"
	stale.RuntimeRevision = 2
	if err := dispatcher.DispatchAnalyticsReport("sub-1", &stale); !errors.Is(err, ErrStaleAnalyticsReport) {
		t.Fatalf("stale report error = %v", err)
	}
}
