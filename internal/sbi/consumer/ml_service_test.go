package consumer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMlServiceClient_InitializeModel(t *testing.T) {
	tests := []struct {
		name           string
		serverResponse func(w http.ResponseWriter, r *http.Request)
		modelUrl       string
		wantModelId    string
		wantErr        bool
	}{
		{
			name: "successful model load",
			serverResponse: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Errorf("expected POST, got %s", r.Method)
				}
				if r.URL.Path != "/model/load" {
					t.Errorf("expected /model/load, got %s", r.URL.Path)
				}

				var req LoadModelRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Errorf("failed to decode request: %v", err)
				}
				if req.ModelUrl != "http://example.com/model.h5" {
					t.Errorf("expected model URL http://example.com/model.h5, got %s", req.ModelUrl)
				}

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				//nolint:errcheck
				json.NewEncoder(w).Encode(LoadModelResponse{ModelId: "test-model-123"})
			},
			modelUrl:    "http://example.com/model.h5",
			wantModelId: "test-model-123",
			wantErr:     false,
		},
		{
			name: "server error",
			serverResponse: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				//nolint:errcheck
				w.Write([]byte("internal error"))
			},
			modelUrl:    "http://example.com/model.h5",
			wantModelId: "",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(tt.serverResponse))
			defer server.Close()

			client := NewMlServiceClient(server.URL)
			modelId, err := client.InitializeModel(tt.modelUrl)

			if (err != nil) != tt.wantErr {
				t.Errorf("InitializeModel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if modelId != tt.wantModelId {
				t.Errorf("InitializeModel() = %v, want %v", modelId, tt.wantModelId)
			}
		})
	}
}

func TestMlServiceClient_Predict(t *testing.T) {
	tests := []struct {
		name           string
		serverResponse func(w http.ResponseWriter, r *http.Request)
		modelId        string
		trafficData    []TrafficObservation
		wantCount      int
		wantErr        bool
	}{
		{
			name: "successful prediction",
			serverResponse: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Errorf("expected POST, got %s", r.Method)
				}
				if r.URL.Path != "/predict" {
					t.Errorf("expected /predict, got %s", r.URL.Path)
				}

				var req PredictRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Errorf("failed to decode request: %v", err)
				}
				if req.ModelId != "model-123" {
					t.Errorf("expected model ID model-123, got %s", req.ModelId)
				}

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				//nolint:errcheck
				json.NewEncoder(w).Encode(PredictResponse{
					PredictedData: []UeCommunicationPrediction{
						{
							Ts:         "2025-01-01T00:00:00Z",
							TrafChar:   TrafficCharacterization{UlVol: 1000, DlVol: 2000},
							Confidence: 85,
						},
					},
				})
			},
			modelId: "model-123",
			trafficData: []TrafficObservation{
				{
					Ts:       "2024-12-31T23:59:00Z",
					UlVol:    500,
					DlVol:    1000,
					TotalVol: 1500,
				},
			},
			wantCount: 1,
			wantErr:   false,
		},
		{
			name: "model not found",
			serverResponse: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				//nolint:errcheck
				w.Write([]byte("model not found"))
			},
			modelId:     "invalid-model",
			trafficData: []TrafficObservation{},
			wantCount:   0,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(tt.serverResponse))
			defer server.Close()

			client := NewMlServiceClient(server.URL)
			resp, err := client.Predict(tt.modelId, tt.trafficData)

			if (err != nil) != tt.wantErr {
				t.Errorf("Predict() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(resp.PredictedData) != tt.wantCount {
				t.Errorf("Predict() returned %d predictions, want %d", len(resp.PredictedData), tt.wantCount)
			}
		})
	}
}
