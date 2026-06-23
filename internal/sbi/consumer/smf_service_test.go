package consumer

import (
	"net/http"
	"testing"

	"github.com/h2non/gock"
)

const (
	testSmfEndpoint       = "http://127.0.0.10:8000"
	testSmfSubscriptionID = "sub-123"
)

func TestNsmfService_SubscribeToSmf(t *testing.T) {
	service := NewNsmfService()
	gock.InterceptClient(service.HTTPClient())
	defer gock.Off()
	defer gock.RestoreClient(service.HTTPClient())

	opts := SmfSubscriptionOptions{
		Supi:        "imsi-208930000000003",
		NotifUri:    "http://127.0.0.1:8080/collector/notify",
		NotifId:     "corr-123",
		EventSubs:   BuildUpfEventSubs("http://127.0.0.1:8080/collector/upf-notify", true, true),
		NotifMethod: "PERIODIC",
		RepPeriod:   10,
	}

	gock.New(testSmfEndpoint).
		Post(SmfEventExposurePath).
		MatchHeader("Content-Type", "application/json").
		JSON(ExtendedNsmfEventExposure{
			Supi:        opts.Supi,
			NotifUri:    opts.NotifUri,
			NotifId:     opts.NotifId,
			EventSubs:   opts.EventSubs,
			NotifMethod: opts.NotifMethod,
			RepPeriod:   opts.RepPeriod,
		}).
		Reply(http.StatusCreated).
		SetHeader("Location", SmfEventExposurePath+"/"+testSmfSubscriptionID)

	subscriptionID, err := service.SubscribeToSmf(testSmfEndpoint, opts)
	if err != nil {
		t.Fatalf("SubscribeToSmf returned error: %v", err)
	}
	if subscriptionID != testSmfSubscriptionID {
		t.Fatalf("SubscribeToSmf returned %q, want %q", subscriptionID, testSmfSubscriptionID)
	}
	if !gock.IsDone() {
		t.Fatal("expected SMF subscription request to match gock expectation")
	}
}

func TestNsmfService_SubscribeToSmfReturnsErrorOnFailureStatus(t *testing.T) {
	service := NewNsmfService()
	gock.InterceptClient(service.HTTPClient())
	defer gock.Off()
	defer gock.RestoreClient(service.HTTPClient())

	gock.New(testSmfEndpoint).
		Post(SmfEventExposurePath).
		Reply(http.StatusInternalServerError).
		BodyString("internal error")

	_, err := service.SubscribeToSmf(testSmfEndpoint, SmfSubscriptionOptions{
		Supi:      "imsi-208930000000003",
		NotifUri:  "http://127.0.0.1:8080/collector/notify",
		NotifId:   "corr-123",
		EventSubs: BuildUpfEventSubs("http://127.0.0.1:8080/collector/upf-notify", true, true),
	})
	if err == nil {
		t.Fatal("expected SubscribeToSmf to fail on non-success status")
	}
}

func TestNsmfService_UnsubscribeFromSmf(t *testing.T) {
	service := NewNsmfService()
	gock.InterceptClient(service.HTTPClient())
	defer gock.Off()
	defer gock.RestoreClient(service.HTTPClient())

	gock.New(testSmfEndpoint).
		Delete(SmfEventExposurePath + "/" + testSmfSubscriptionID).
		Reply(http.StatusNoContent)

	if err := service.UnsubscribeFromSmf(testSmfEndpoint, testSmfSubscriptionID); err != nil {
		t.Fatalf("UnsubscribeFromSmf returned error: %v", err)
	}
	if !gock.IsDone() {
		t.Fatal("expected SMF unsubscribe request to match gock expectation")
	}
}
