package consumer

import (
	"net/http"
	"testing"

	"github.com/h2non/gock"

	"github.com/free5gc/openapi/models"
)

const testMtlfEndpoint = "http://127.0.0.20:8000"

func TestNmtlfService_SubscribeToMtlf(t *testing.T) {
	service := NewNmtlfService()
	gock.InterceptClient(service.HTTPClient())
	defer gock.Off()
	defer gock.RestoreClient(service.HTTPClient())

	opts := MtlfSubscriptionOptions{
		NotifUri: "http://127.0.0.1:8080/mlmodel-notify",
		NotifId:  "corr-123",
		Event:    models.NwdafEvent_UE_COMMUNICATION,
		TgtUe: &models.TargetUeInformation{
			Supis: []string{"imsi-208930000000003"},
		},
	}

	gock.New(testMtlfEndpoint).
		Post(MtlfMLModelProvisionPath).
		MatchHeader("Content-Type", "application/json").
		JSON(models.NwdafMlModelProvSubsc{
			MLEventSubscs: []models.MlEventSubscription{
				{
					MLEvent: opts.Event,
					TgtUe:   opts.TgtUe,
				},
			},
			NotifUri:     opts.NotifUri,
			NotifCorreId: opts.NotifId,
		}).
		Reply(http.StatusCreated).
		SetHeader("Content-Type", "application/json").
		SetHeader("Location", MtlfMLModelProvisionPath+"/sub-123").
		JSON(models.NwdafMlModelProvSubsc{})

	subscriptionID, err := service.SubscribeToMtlf(testMtlfEndpoint, opts)
	if err != nil {
		t.Fatalf("SubscribeToMtlf returned error: %v", err)
	}
	if subscriptionID != "sub-123" {
		t.Fatalf("SubscribeToMtlf returned %q, want %q", subscriptionID, "sub-123")
	}
	if !gock.IsDone() {
		t.Fatal("expected MTLF subscription request to match gock expectation")
	}
}

func TestNmtlfService_SubscribeToMtlfReturnsErrorOnFailureStatus(t *testing.T) {
	service := NewNmtlfService()
	gock.InterceptClient(service.HTTPClient())
	defer gock.Off()
	defer gock.RestoreClient(service.HTTPClient())

	gock.New(testMtlfEndpoint).
		Post(MtlfMLModelProvisionPath).
		Reply(http.StatusInternalServerError).
		BodyString("internal error")

	_, err := service.SubscribeToMtlf(testMtlfEndpoint, MtlfSubscriptionOptions{
		NotifUri: "http://127.0.0.1:8080/mlmodel-notify",
		NotifId:  "corr-123",
		Event:    models.NwdafEvent_UE_COMMUNICATION,
	})
	if err == nil {
		t.Fatal("expected SubscribeToMtlf to fail on non-success status")
	}
}

func TestNmtlfService_UnsubscribeFromMtlf(t *testing.T) {
	service := NewNmtlfService()
	gock.InterceptClient(service.HTTPClient())
	defer gock.Off()
	defer gock.RestoreClient(service.HTTPClient())

	gock.New(testMtlfEndpoint).
		Delete(MtlfMLModelProvisionPath + "/sub-123").
		Reply(http.StatusNoContent)

	if err := service.UnsubscribeFromMtlf(testMtlfEndpoint, "sub-123"); err != nil {
		t.Fatalf("UnsubscribeFromMtlf returned error: %v", err)
	}
	if !gock.IsDone() {
		t.Fatal("expected MTLF unsubscribe request to match gock expectation")
	}
}
