package consumer

import (
	"net/http"
	"testing"

	"github.com/h2non/gock"
)

const testMlServiceEndpoint = "http://127.0.0.30:8000"

func newInterceptedMlServiceClient(t *testing.T) *MlServiceClient {
	t.Helper()

	client := NewMlServiceClient(testMlServiceEndpoint)
	gock.InterceptClient(client.HTTPClient())
	t.Cleanup(func() {
		gock.Off()
		gock.RestoreClient(client.HTTPClient())
	})

	return client
}

func TestMlServiceClient_InitializeModel(t *testing.T) {
	client := newInterceptedMlServiceClient(t)

	gock.New(testMlServiceEndpoint).
		Post("/model/load").
		MatchHeader("Content-Type", "application/json").
		JSON(LoadModelRequest{ModelUrl: "http://example.com/model.h5"}).
		Reply(http.StatusCreated).
		JSON(LoadModelResponse{ModelId: "test-model-123"})

	modelID, err := client.InitializeModel("http://example.com/model.h5")
	if err != nil {
		t.Fatalf("InitializeModel returned error: %v", err)
	}
	if modelID != "test-model-123" {
		t.Fatalf("InitializeModel returned %q, want %q", modelID, "test-model-123")
	}
	if !gock.IsDone() {
		t.Fatal("expected ML model initialization request to match gock expectation")
	}
}

func TestMlServiceClient_InitializeModelReturnsErrorOnFailureStatus(t *testing.T) {
	client := newInterceptedMlServiceClient(t)

	gock.New(testMlServiceEndpoint).
		Post("/model/load").
		Reply(http.StatusInternalServerError).
		BodyString("internal error")

	if _, err := client.InitializeModel("http://example.com/model.h5"); err == nil {
		t.Fatal("expected InitializeModel to fail on non-success status")
	}
}

func TestMlServiceClient_UnloadModel(t *testing.T) {
	client := newInterceptedMlServiceClient(t)

	gock.New(testMlServiceEndpoint).
		Post("/model/unload").
		MatchHeader("Content-Type", "application/json").
		JSON(UnloadModelRequest{ModelId: "model-123"}).
		Reply(http.StatusOK).
		JSON(UnloadModelResponse{ModelId: "model-123", Status: "unloaded"})

	if err := client.UnloadModel("model-123"); err != nil {
		t.Fatalf("UnloadModel returned error: %v", err)
	}
	if !gock.IsDone() {
		t.Fatal("expected ML model unload request to match gock expectation")
	}
}

func TestMlServiceClient_Predict(t *testing.T) {
	client := newInterceptedMlServiceClient(t)

	trafficData := []TrafficObservation{
		{
			Ts:       "2024-12-31T23:59:00Z",
			UlVol:    500,
			DlVol:    1000,
			TotalVol: 1500,
		},
	}

	gock.New(testMlServiceEndpoint).
		Post("/predict").
		MatchHeader("Content-Type", "application/json").
		JSON(PredictRequest{
			ModelId:        "model-123",
			HistoricalData: trafficData,
		}).
		Reply(http.StatusOK).
		JSON(PredictResponse{
			PredictedData: []UeCommunicationPrediction{
				{
					Ts:         "2025-01-01T00:00:00Z",
					TrafChar:   TrafficCharacterization{UlVol: 1000, DlVol: 2000},
					Confidence: 85,
				},
			},
		})

	resp, err := client.Predict("model-123", trafficData)
	if err != nil {
		t.Fatalf("Predict returned error: %v", err)
	}
	if len(resp.PredictedData) != 1 {
		t.Fatalf("Predict returned %d predictions, want 1", len(resp.PredictedData))
	}
	if !gock.IsDone() {
		t.Fatal("expected ML prediction request to match gock expectation")
	}
}

func TestMlServiceClient_PredictReturnsDecodeError(t *testing.T) {
	client := newInterceptedMlServiceClient(t)

	gock.New(testMlServiceEndpoint).
		Post("/predict").
		Reply(http.StatusOK).
		BodyString("{invalid json")

	if _, err := client.Predict("model-123", nil); err == nil {
		t.Fatal("expected Predict to fail on invalid JSON response")
	}
}
