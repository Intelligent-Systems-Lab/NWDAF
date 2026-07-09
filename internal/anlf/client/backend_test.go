package client

import (
	"context"
	"net/http"
	"testing"

	"github.com/h2non/gock"

	"github.com/free5gc/nwdaf/internal/anlf"
)

const testAnlfBackendEndpoint = "http://127.0.0.30:8000"

func newInterceptedAnlfBackendClient(t *testing.T) *Client {
	t.Helper()

	client := NewClient(testAnlfBackendEndpoint)
	gock.InterceptClient(client.HTTPClient())
	t.Cleanup(func() {
		gock.Off()
	})

	return client
}

func TestClient_LoadModel(t *testing.T) {
	client := newInterceptedAnlfBackendClient(t)

	gock.New(testAnlfBackendEndpoint).
		Post("/model/load").
		JSON(anlf.LoadModelRequest{ModelUrl: "http://example.com/model.h5"}).
		Reply(http.StatusCreated).
		JSON(anlf.LoadModelResponse{ModelId: "model-123"})

	modelID, err := client.LoadModel(context.Background(), "http://example.com/model.h5")
	if err != nil {
		t.Fatalf("LoadModel returned error: %v", err)
	}
	if modelID != "model-123" {
		t.Fatalf("LoadModel returned %q, want %q", modelID, "model-123")
	}
}

func TestClient_LoadModelReturnsErrorOnFailureStatus(t *testing.T) {
	client := newInterceptedAnlfBackendClient(t)

	gock.New(testAnlfBackendEndpoint).
		Post("/model/load").
		Reply(http.StatusBadGateway)

	if _, err := client.LoadModel(context.Background(), "http://example.com/model.h5"); err == nil {
		t.Fatal("expected LoadModel to fail on non-success status")
	}
}

func TestClient_UnloadModel(t *testing.T) {
	client := newInterceptedAnlfBackendClient(t)

	gock.New(testAnlfBackendEndpoint).
		Post("/model/unload").
		JSON(anlf.UnloadModelRequest{ModelId: "model-123"}).
		Reply(http.StatusOK)

	if err := client.UnloadModel(context.Background(), "model-123"); err != nil {
		t.Fatalf("UnloadModel returned error: %v", err)
	}
}

func TestClient_Predict(t *testing.T) {
	client := newInterceptedAnlfBackendClient(t)

	trafficData := []anlf.TrafficObservation{
		{
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
		},
	}

	gock.New(testAnlfBackendEndpoint).
		Post("/predict").
		Reply(http.StatusOK).
		JSON(anlf.PredictResponse{
			PredictedData: []anlf.UeCommunicationPrediction{
				{
					Ts: "2025-06-20T10:05:00Z",
					TrafChar: anlf.TrafficCharacterization{
						UlVol: 500,
						DlVol: 700,
					},
					Confidence: 80,
				},
			},
		})

	response, err := client.Predict(context.Background(), "model-123", trafficData)
	if err != nil {
		t.Fatalf("Predict returned error: %v", err)
	}
	if len(response.PredictedData) != 1 {
		t.Fatalf("Predict returned %d predictions, want 1", len(response.PredictedData))
	}
}

func TestClient_PredictReturnsDecodeError(t *testing.T) {
	client := newInterceptedAnlfBackendClient(t)

	gock.New(testAnlfBackendEndpoint).
		Post("/predict").
		Reply(http.StatusOK).
		BodyString("{not-json")

	if _, err := client.Predict(context.Background(), "model-123", nil); err == nil {
		t.Fatal("expected Predict to fail on invalid JSON response")
	}
}
