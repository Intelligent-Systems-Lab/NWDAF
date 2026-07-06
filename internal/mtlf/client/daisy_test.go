package client

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/h2non/gock"

	"github.com/free5gc/nwdaf/internal/mtlf"
)

const testDaisyEndpoint = "http://127.0.0.40:8000"

func newInterceptedDaisyClient(t *testing.T) *Client {
	t.Helper()

	client := NewClient(testDaisyEndpoint)
	gock.InterceptClient(client.HTTPClient())
	t.Cleanup(func() {
		gock.Off()
	})

	return client
}

func TestNewClient(t *testing.T) {
	client := NewClient(testDaisyEndpoint)

	if client == nil {
		t.Fatal("NewClient() returned nil")
	}
	if client.GetEndpoint() != testDaisyEndpoint {
		t.Errorf("GetEndpoint() = %v, want %v", client.GetEndpoint(), testDaisyEndpoint)
	}
	if client.HTTPClient().Timeout != AsyncTimeout {
		t.Errorf("Timeout = %v, want %v", client.HTTPClient().Timeout, AsyncTimeout)
	}
}

func TestClient_TriggerTrainingAsync(t *testing.T) {
	client := newInterceptedDaisyClient(t)
	callbackURL := "http://nwdaf:8080/mtlf/training-complete"

	gock.New(testDaisyEndpoint).
		Post(mtlf.DaisyPublishTaskPath).
		JSON(map[string]any{
			"epochs":                 10,
			mtlf.DaisyTIDKey:         "task-123",
			mtlf.DaisyCallbackURLKey: callbackURL,
		}).
		Reply(http.StatusAccepted)

	taskID, err := client.TriggerTrainingAsync(context.Background(), map[string]any{"epochs": 10}, callbackURL, "task-123")
	if err != nil {
		t.Fatalf("TriggerTrainingAsync returned error: %v", err)
	}
	if taskID != "task-123" {
		t.Fatalf("TriggerTrainingAsync returned %q, want %q", taskID, "task-123")
	}
	if !gock.IsDone() {
		t.Fatal("expected Daisy training request to match gock expectation")
	}
}

func TestClient_TriggerTrainingAsyncOmitsCallbackWhenEmpty(t *testing.T) {
	client := newInterceptedDaisyClient(t)

	gock.New(testDaisyEndpoint).
		Post(mtlf.DaisyPublishTaskPath).
		JSON(map[string]any{
			mtlf.DaisyTIDKey: "task-456",
		}).
		Reply(http.StatusAccepted)

	taskID, err := client.TriggerTrainingAsync(context.Background(), map[string]any{}, "", "task-456")
	if err != nil {
		t.Fatalf("TriggerTrainingAsync returned error: %v", err)
	}
	if taskID != "task-456" {
		t.Fatalf("TriggerTrainingAsync returned %q, want %q", taskID, "task-456")
	}
	if !gock.IsDone() {
		t.Fatal("expected Daisy training request without callback to match gock expectation")
	}
}

func TestClient_TriggerTrainingAsyncRejectsFailureStatus(t *testing.T) {
	client := newInterceptedDaisyClient(t)

	gock.New(testDaisyEndpoint).
		Post(mtlf.DaisyPublishTaskPath).
		Reply(http.StatusBadGateway)

	if _, err := client.TriggerTrainingAsync(context.Background(), map[string]any{}, "", "task-789"); err == nil {
		t.Fatal("expected TriggerTrainingAsync to fail on non-202 status")
	}
}

func TestClient_UploadData(t *testing.T) {
	client := newInterceptedDaisyClient(t)

	payload := []json.RawMessage{
		json.RawMessage(`{"supi":"imsi-001"}`),
	}

	gock.New(testDaisyEndpoint).
		Post(mtlf.DaisyUploadDataPath).
		JSON(mtlf.DaisyUploadDataRequest{
			TID:            "task-123",
			GroupId:        "group-a",
			UpfEventNotifs: payload,
		}).
		Reply(http.StatusCreated)

	if err := client.UploadData(context.Background(), "task-123", "group-a", payload); err != nil {
		t.Fatalf("UploadData returned error: %v", err)
	}
	if !gock.IsDone() {
		t.Fatal("expected Daisy upload request to match gock expectation")
	}
}

func TestClient_UploadDataReturnsErrorOnFailureStatus(t *testing.T) {
	client := newInterceptedDaisyClient(t)

	gock.New(testDaisyEndpoint).
		Post(mtlf.DaisyUploadDataPath).
		Reply(http.StatusInternalServerError)

	if err := client.UploadData(context.Background(), "task-123", "group-a", nil); err == nil {
		t.Fatal("expected UploadData to fail on non-success status")
	}
}
