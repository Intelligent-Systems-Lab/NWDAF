package consumer

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/h2non/gock"
)

const testDaisyEndpoint = "http://127.0.0.40:8000"

func newInterceptedDaisyClient(t *testing.T) *DaisyClient {
	t.Helper()

	client := NewDaisyClient(testDaisyEndpoint)
	gock.InterceptClient(client.HTTPClient())
	t.Cleanup(func() {
		gock.Off()
		gock.RestoreClient(client.HTTPClient())
	})

	return client
}

func TestNewDaisyClient(t *testing.T) {
	client := NewDaisyClient(testDaisyEndpoint)

	if client == nil {
		t.Fatal("NewDaisyClient() returned nil")
	}
	if client.GetEndpoint() != testDaisyEndpoint {
		t.Errorf("GetEndpoint() = %v, want %v", client.GetEndpoint(), testDaisyEndpoint)
	}
	if client.HTTPClient() == nil {
		t.Error("HTTPClient() returned nil")
	}
	if client.HTTPClient().Timeout != DaisyAsyncTimeout {
		t.Errorf("Timeout = %v, want %v", client.HTTPClient().Timeout, DaisyAsyncTimeout)
	}
}

func TestDaisyClient_TriggerTrainingAsync(t *testing.T) {
	client := newInterceptedDaisyClient(t)

	callbackURL := "http://nwdaf:8080/mtlf/training-complete"
	task := map[string]any{"NUM_ROUNDS": float64(2)}

	gock.New(testDaisyEndpoint).
		Post(DaisyPublishTaskPath).
		MatchHeader("Content-Type", "application/json").
		JSON(map[string]any{
			"NUM_ROUNDS":        float64(2),
			DaisyTIDKey:         "task-123",
			DaisyCallbackURLKey: callbackURL,
		}).
		Reply(http.StatusAccepted)

	taskID, err := client.TriggerTrainingAsync(context.Background(), task, callbackURL, "task-123")
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

func TestDaisyClient_TriggerTrainingAsyncOmitsCallbackWhenEmpty(t *testing.T) {
	client := newInterceptedDaisyClient(t)

	gock.New(testDaisyEndpoint).
		Post(DaisyPublishTaskPath).
		MatchHeader("Content-Type", "application/json").
		JSON(map[string]any{
			DaisyTIDKey: "task-456",
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

func TestDaisyClient_TriggerTrainingAsyncRejectsFailureStatus(t *testing.T) {
	client := newInterceptedDaisyClient(t)

	gock.New(testDaisyEndpoint).
		Post(DaisyPublishTaskPath).
		Reply(http.StatusInternalServerError).
		BodyString(`{"error":"busy"}`)

	if _, err := client.TriggerTrainingAsync(context.Background(), map[string]any{}, "", "task-789"); err == nil {
		t.Fatal("expected TriggerTrainingAsync to fail on non-202 status")
	}
}

func TestDaisyClient_UploadData(t *testing.T) {
	client := newInterceptedDaisyClient(t)

	upfEventNotifs := []json.RawMessage{
		json.RawMessage(`{"event":"usage-1"}`),
		json.RawMessage(`{"event":"usage-2"}`),
	}

	gock.New(testDaisyEndpoint).
		Post(DaisyUploadDataPath).
		MatchHeader("Content-Type", "application/json").
		JSON(DaisyUploadDataRequest{
			TID:            "task-123",
			GroupId:        "group-A",
			UpfEventNotifs: upfEventNotifs,
		}).
		Reply(http.StatusCreated)

	if err := client.UploadData(context.Background(), "task-123", "group-A", upfEventNotifs); err != nil {
		t.Fatalf("UploadData returned error: %v", err)
	}
	if !gock.IsDone() {
		t.Fatal("expected Daisy upload request to match gock expectation")
	}
}

func TestDaisyClient_UploadDataReturnsErrorOnFailureStatus(t *testing.T) {
	client := newInterceptedDaisyClient(t)

	gock.New(testDaisyEndpoint).
		Post(DaisyUploadDataPath).
		Reply(http.StatusBadGateway).
		BodyString("backend unavailable")

	if err := client.UploadData(context.Background(), "task-123", "group-A", nil); err == nil {
		t.Fatal("expected UploadData to fail on non-success status")
	}
}
