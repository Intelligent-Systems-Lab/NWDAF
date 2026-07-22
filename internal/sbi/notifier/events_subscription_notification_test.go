package notifier

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

func TestDispatchEventsSubscriptionNotificationsForwardsOriginalStandardBody(t *testing.T) {
	var delivered string
	consumer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("ReadAll() error = %v", err)
		}
		delivered = string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer consumer.Close()

	nwdaf_context.Init()
	nwdaf_context.GetSelf().AddAnalyticsSubscriptionRoute(nwdaf_context.AnalyticsSubscriptionRoute{
		SubscriptionID:          "sub-a",
		ExternalNotificationURI: consumer.URL,
		AcceptedSubscription: models.NnwdafEventsSubscription{
			NotifCorrId: "corr-a",
		},
	})
	notifications := []models.NnwdafEventsSubscriptionNotification{{
		SubscriptionId: "sub-a",
		NotifCorrId:    "corr-a",
		EventNotifications: []models.NwdafEventsSubscriptionEventNotification{{
			Event: models.NwdafEvent_UE_COMMUNICATION,
		}},
	}}
	rawBody := []byte(
		`[{"subscriptionId":"sub-a","notifCorrId":"corr-a","eventNotifications":` +
			`[{"event":"UE_COMMUNICATION","ueComms":[{"commDur":0,"confidence":0,` +
			`"trafChar":{"ulVol":0,"dlVol":0}}]}]}]`,
	)

	err := NewReportDispatcher(context.Background()).DispatchEventsSubscriptionNotifications(
		notifications,
		rawBody,
	)
	if err != nil {
		t.Fatalf("DispatchEventsSubscriptionNotifications() error = %v", err)
	}
	if delivered != string(rawBody) {
		t.Fatalf("delivered body = %s, want exact %s", delivered, rawBody)
	}
}

func TestDispatchEventsSubscriptionNotificationsPreservesRedirectLocation(t *testing.T) {
	consumer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "http://consumer.example/redirected")
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusTemporaryRedirect)
		if _, err := w.Write([]byte(`{"status":307}`)); err != nil {
			t.Errorf("write redirect response: %v", err)
		}
	}))
	defer consumer.Close()

	nwdaf_context.Init()
	nwdaf_context.GetSelf().AddAnalyticsSubscriptionRoute(nwdaf_context.AnalyticsSubscriptionRoute{
		SubscriptionID:          "sub-a",
		ExternalNotificationURI: consumer.URL,
		AcceptedSubscription: models.NnwdafEventsSubscription{
			NotifCorrId: "corr-a",
		},
	})
	err := NewReportDispatcher(context.Background()).DispatchEventsSubscriptionNotifications(
		[]models.NnwdafEventsSubscriptionNotification{{
			SubscriptionId: "sub-a",
			NotifCorrId:    "corr-a",
			EventNotifications: []models.NwdafEventsSubscriptionEventNotification{{
				Event: models.NwdafEvent_UE_COMMUNICATION,
			}},
		}},
		nil,
	)
	var deliveryError *CallbackDeliveryError
	if !errors.As(err, &deliveryError) {
		t.Fatalf("error = %T %v", err, err)
	}
	if deliveryError.StatusCode != http.StatusTemporaryRedirect ||
		deliveryError.RedirectLocation() != "http://consumer.example/redirected" {
		t.Fatalf("delivery error = %+v", deliveryError)
	}
}

func TestDispatchEventsSubscriptionNotificationsRejectsCorrelationMismatch(t *testing.T) {
	nwdaf_context.Init()
	nwdaf_context.GetSelf().AddAnalyticsSubscriptionRoute(nwdaf_context.AnalyticsSubscriptionRoute{
		SubscriptionID:          "sub-a",
		ExternalNotificationURI: "http://consumer.example/notify",
		AcceptedSubscription: models.NnwdafEventsSubscription{
			NotifCorrId: "corr-a",
		},
	})

	err := NewReportDispatcher(context.Background()).DispatchEventsSubscriptionNotifications(
		[]models.NnwdafEventsSubscriptionNotification{{
			SubscriptionId: "sub-a",
			NotifCorrId:    "wrong",
			EventNotifications: []models.NwdafEventsSubscriptionEventNotification{{
				Event: models.NwdafEvent_UE_COMMUNICATION,
			}},
		}},
		nil,
	)

	if err != ErrInvalidAnalyticsReport {
		t.Fatalf("error = %v, want %v", err, ErrInvalidAnalyticsReport)
	}
}
