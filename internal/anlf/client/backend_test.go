package client

import (
	"context"
	"net/http"
	"testing"

	"github.com/h2non/gock"

	"github.com/free5gc/nwdaf/internal/anlf"
	"github.com/free5gc/openapi/models"
)

const testAnlfBackendEndpoint = "http://127.0.0.30:8000"

func newInterceptedAnlfBackendClient(t *testing.T) *Client {
	t.Helper()

	client := NewClient(testAnlfBackendEndpoint)
	gock.InterceptClient(client.HTTPClient())
	t.Cleanup(gock.Off)

	return client
}

func testApplyRequest() anlf.ApplySubscriptionRuntimeRequest {
	return anlf.ApplySubscriptionRuntimeRequest{
		Subscription: anlf.SubscriptionRuntimeContext{
			SubscriptionID: "sub-123",
			NotifCorrID:    "corr-123",
			EventSubscriptions: []models.NwdafEventsSubscriptionEventSubscription{
				{Event: models.NwdafEvent_UE_COMMUNICATION},
			},
		},
		ProvisionContext: &anlf.ProvisionContext{
			Source:              "MTLF_PROVISION",
			MtlfSubscriptionID:  "mtlf-sub-1",
			NotifSubscriptionID: "mtlf-sub-1",
			MLEventNotification: anlf.MLEventNotification{
				MlEventNotif: models.MlEventNotif{
					Event:        models.NwdafEvent_UE_COMMUNICATION,
					NotifCorreId: "sub-123",
					MLFileAddr:   &models.MlModelAddr{MLModelUrl: "http://example.com/model-a"},
				},
			},
		},
	}
}

func TestClientApplySubscriptionRuntime(t *testing.T) {
	client := newInterceptedAnlfBackendClient(t)
	request := testApplyRequest()

	gock.New(testAnlfBackendEndpoint).
		Put("/subscriptions/sub-123/runtime").
		JSON(request).
		Reply(http.StatusOK).
		JSON(anlf.ApplySubscriptionRuntimeResponse{
			SubscriptionID:       "sub-123",
			RuntimeState:         "READY",
			Result:               anlf.ApplyResultActivated,
			ActiveModelReference: "http://example.com/model-a",
		})

	response, err := client.ApplySubscriptionRuntime(context.Background(), request)
	if err != nil {
		t.Fatalf("ApplySubscriptionRuntime() error = %v", err)
	}
	if response.Result != anlf.ApplyResultActivated {
		t.Fatalf("result = %q, want %q", response.Result, anlf.ApplyResultActivated)
	}
}

func TestClientApplySubscriptionRuntimeReturnsErrorOnFailureStatus(t *testing.T) {
	client := newInterceptedAnlfBackendClient(t)

	gock.New(testAnlfBackendEndpoint).
		Put("/subscriptions/sub-123/runtime").
		Reply(http.StatusBadGateway)

	if _, err := client.ApplySubscriptionRuntime(context.Background(), testApplyRequest()); err == nil {
		t.Fatal("expected runtime apply to fail on non-success status")
	}
}

func TestClientReleaseSubscriptionRuntime(t *testing.T) {
	client := newInterceptedAnlfBackendClient(t)

	gock.New(testAnlfBackendEndpoint).
		Delete("/subscriptions/sub-123/runtime").
		Reply(http.StatusOK).
		JSON(map[string]string{"subscription_id": "sub-123", "status": "released"})

	if err := client.ReleaseSubscriptionRuntime(context.Background(), "sub-123"); err != nil {
		t.Fatalf("ReleaseSubscriptionRuntime() error = %v", err)
	}
}

func TestClientPredictUsesSubscriptionID(t *testing.T) {
	client := newInterceptedAnlfBackendClient(t)
	trafficData := []anlf.TrafficObservation{{
		Ts:          "2025-06-20T10:00:00Z",
		TotalVol:    1000,
		UlVol:       400,
		DlVol:       600,
		TotalNbPkts: 100,
		UlNbPkts:    40,
		DlNbPkts:    60,
		UlThr:       1.2,
		DlThr:       2.3,
		UlPktThr:    0.8,
		DlPktThr:    1.1,
	}}

	gock.New(testAnlfBackendEndpoint).
		Post("/subscriptions/sub-123/predict").
		JSON(anlf.PredictRequest{HistoricalData: trafficData}).
		Reply(http.StatusOK).
		JSON(anlf.PredictResponse{
			PredictedData: []anlf.UeCommunicationPrediction{{
				Ts: "2025-06-20T10:05:00Z",
				TrafChar: anlf.TrafficCharacterization{
					UlVol: 500,
					DlVol: 700,
				},
				Confidence: 80,
			}},
		})

	response, err := client.Predict(context.Background(), "sub-123", trafficData)
	if err != nil {
		t.Fatalf("Predict() error = %v", err)
	}
	if len(response.PredictedData) != 1 {
		t.Fatalf("prediction count = %d, want 1", len(response.PredictedData))
	}
}

func TestClientPredictReturnsDecodeError(t *testing.T) {
	client := newInterceptedAnlfBackendClient(t)

	gock.New(testAnlfBackendEndpoint).
		Post("/subscriptions/sub-123/predict").
		Reply(http.StatusOK).
		BodyString("{not-json")

	if _, err := client.Predict(context.Background(), "sub-123", nil); err == nil {
		t.Fatal("expected Predict to fail on invalid JSON response")
	}
}
