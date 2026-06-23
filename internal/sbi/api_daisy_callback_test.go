package sbi

import (
	"encoding/json"
	"net/http"
	"testing"

	"go.uber.org/mock/gomock"
)

func TestHandleDaisyTrainingComplete_InvalidPayload(t *testing.T) {
	server := newHandlerTestServer(nil)
	c, recorder := newJSONRequestContext(http.MethodPost, "/mtlf/training-complete", []byte(`{"task_id":`))

	server.HandleDaisyTrainingComplete(c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}

	var body map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode body: %v", err)
	}
	if body["error"] != "invalid payload" {
		t.Fatalf("error = %q", body["error"])
	}
}

func TestHandleDaisyTrainingComplete_MissingTaskID(t *testing.T) {
	server := newHandlerTestServer(nil)
	c, recorder := newJSONRequestContext(http.MethodPost, "/mtlf/training-complete", []byte(`{"status":"success"}`))

	server.HandleDaisyTrainingComplete(c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestHandleDaisyTrainingComplete_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProcessor := NewMockprocessorAPI(ctrl)
	mockProcessor.EXPECT().
		HandleDaisyCallback("task-1", "http://example.com/model.onnx", "success", "")

	server := newHandlerTestServer(mockProcessor)
	c, recorder := newJSONRequestContext(
		http.MethodPost,
		"/mtlf/training-complete",
		[]byte(`{"task_id":"task-1","model_url":"http://example.com/model.onnx","status":"success"}`),
	)

	server.HandleDaisyTrainingComplete(c)
	c.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
}
