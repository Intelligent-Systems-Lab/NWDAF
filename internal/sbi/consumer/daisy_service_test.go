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
	if client.HTTPClient().Timeout != DaisyAsyncTimeout {
		t.Errorf("Timeout = %v, want %v", client.HTTPClient().Timeout, DaisyAsyncTimeout)
	}
}

// TestTriggerTrainingAsync_Success tests async training request with 202 response
func TestTriggerTrainingAsync_Success(t *testing.T) {
	var receivedPayload map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("Method = %v, want POST", r.Method)
		}
		if r.URL.Path != DaisyPublishTaskPath {
			t.Errorf("Path = %v, want %v", r.URL.Path, DaisyPublishTaskPath)
		}
		if err := json.NewDecoder(r.Body).Decode(&receivedPayload); err != nil {
			t.Fatalf("Failed to decode request body: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	const cbURL = "http://nwdaf:8080/mtlf/training-complete"
	client := NewDaisyClient(server.URL)
	task := map[string]any{"NUM_ROUNDS": float64(2)}

	taskId, err := client.TriggerTrainingAsync(task, cbURL)
	if err != nil {
		t.Fatalf("TriggerTrainingAsync() error = %v", err)
	}
	if taskId == "" {
		t.Error("TriggerTrainingAsync() should return non-empty taskId")
	}
	if receivedPayload[DaisyCallbackURLKey] != cbURL {
		t.Errorf("callback_url = %v, want %v", receivedPayload[DaisyCallbackURLKey], cbURL)
	}
	if _, ok := receivedPayload[DaisyTIDKey]; !ok {
		t.Error("TID should be set in payload")
	}
	if receivedPayload[DaisyTIDKey] != taskId {
		t.Errorf("returned taskId = %v does not match payload TID = %v",
			taskId, receivedPayload[DaisyTIDKey])
	}
}

// TestTriggerTrainingAsync_Rejected tests error when Daisy returns non-202
func TestTriggerTrainingAsync_Rejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		if _, err := w.Write([]byte(`{"error":"busy"}`)); err != nil {
			t.Logf("failed to write response: %v", err)
		}
	}))
	defer server.Close()

	client := NewDaisyClient(server.URL)
	_, err := client.TriggerTrainingAsync(map[string]any{}, "http://callback")
	if err == nil {
		t.Error("TriggerTrainingAsync() should return error when server rejects")
	}
}

// TestTriggerTrainingAsync_NoCallbackURL tests that callback_url is omitted when empty
func TestTriggerTrainingAsync_NoCallbackURL(t *testing.T) {
	var receivedPayload map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&receivedPayload); err != nil {
			t.Fatalf("Failed to decode request body: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := NewDaisyClient(server.URL)
	_, err := client.TriggerTrainingAsync(map[string]any{}, "")
	if err != nil {
		t.Fatalf("TriggerTrainingAsync() error = %v", err)
	}
	if _, ok := receivedPayload[DaisyCallbackURLKey]; ok {
		t.Error("callback_url should not be set when empty string is passed")
	}
}
