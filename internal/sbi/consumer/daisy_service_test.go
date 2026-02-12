package consumer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestNewDaisyClient tests DaisyClient initialization
func TestNewDaisyClient(t *testing.T) {
	endpoint := "http://127.0.0.1:9887"
	client := NewDaisyClient(endpoint)

	if client == nil {
		t.Fatal("NewDaisyClient() returned nil")
	}
	if client.GetEndpoint() != endpoint {
		t.Errorf("GetEndpoint() = %v, want %v", client.GetEndpoint(), endpoint)
	}
	if client.HTTPClient() == nil {
		t.Error("HTTPClient() returned nil")
	}
	if client.HTTPClient().Timeout != DaisyDefaultTimeout {
		t.Errorf("Timeout = %v, want %v", client.HTTPClient().Timeout, DaisyDefaultTimeout)
	}
}

// TestTriggerTraining_Success tests successful training trigger
func TestTriggerTraining_Success(t *testing.T) {
	var receivedPayload map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify method and path
		if r.Method != http.MethodPost {
			t.Errorf("Method = %v, want POST", r.Method)
		}
		if r.URL.Path != DaisyPublishTaskPath {
			t.Errorf("Path = %v, want %v", r.URL.Path, DaisyPublishTaskPath)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type = %v, want application/json", r.Header.Get("Content-Type"))
		}

		// Decode payload
		if err := json.NewDecoder(r.Body).Decode(&receivedPayload); err != nil {
			t.Fatalf("Failed to decode request body: %v", err)
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewDaisyClient(server.URL)
	task := map[string]any{
		"TID":        "test-training-001",
		"NUM_ROUNDS": float64(2),
	}

	err := client.TriggerTraining(task)
	if err != nil {
		t.Errorf("TriggerTraining() error = %v", err)
	}

	// Verify payload was received correctly
	if receivedPayload["TID"] != "test-training-001" {
		t.Errorf("TID = %v, want test-training-001", receivedPayload["TID"])
	}
	if receivedPayload["NUM_ROUNDS"] != float64(2) {
		t.Errorf("NUM_ROUNDS = %v, want 2", receivedPayload["NUM_ROUNDS"])
	}
}

// TestTriggerTraining_WithAutoTID tests auto-generated TID when not provided
func TestTriggerTraining_WithAutoTID(t *testing.T) {
	var receivedPayload map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&receivedPayload); err != nil {
			t.Fatalf("Failed to decode request body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewDaisyClient(server.URL)
	task := map[string]any{
		"NUM_ROUNDS": float64(3),
	}

	err := client.TriggerTraining(task)
	if err != nil {
		t.Errorf("TriggerTraining() error = %v", err)
	}

	// TID should be auto-generated
	tid, ok := receivedPayload["TID"]
	if !ok {
		t.Fatal("TID should be auto-generated when not provided")
	}
	if tid == "" {
		t.Error("Auto-generated TID should not be empty")
	}
}

// TestTriggerTraining_ServerError tests error handling for server errors
func TestTriggerTraining_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		if _, writeErr := w.Write([]byte(`{"error": "training failed"}`)); writeErr != nil {
			t.Logf("Failed to write response: %v", writeErr)
		}
	}))
	defer server.Close()

	client := NewDaisyClient(server.URL)
	task := map[string]any{"TID": "test-fail"}

	err := client.TriggerTraining(task)
	if err == nil {
		t.Error("TriggerTraining() should return error on server error")
	}
}

// TestTriggerTraining_ConnectionError tests error handling for connection errors
func TestTriggerTraining_ConnectionError(t *testing.T) {
	client := NewDaisyClient("http://127.0.0.1:1") // Invalid port

	task := map[string]any{"TID": "test-conn-error"}

	err := client.TriggerTraining(task)
	if err == nil {
		t.Error("TriggerTraining() should return error on connection failure")
	}
}
