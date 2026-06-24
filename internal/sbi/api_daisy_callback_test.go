package sbi

import (
	"net/http"
	"testing"

	"go.uber.org/mock/gomock"
)

func TestHandleDaisyTrainingComplete_InvalidPayload(t *testing.T) {
	server := newHandlerTestServer(t, nil)
	c, recorder := newJSONRequestContext(http.MethodPost, "/mtlf/training-complete", []byte(`{"task_id":`))

	server.HandleDaisyTrainingComplete(c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}

	problem := decodeProblemDetailsResponse(t, recorder)
	if problem.Title != malformedRequestSyntaxTitle {
		t.Fatalf("title = %q", problem.Title)
	}
	if problem.Cause != "" {
		t.Fatalf("cause = %q, want empty", problem.Cause)
	}
}

func TestHandleDaisyTrainingComplete_MissingTaskID(t *testing.T) {
	server := newHandlerTestServer(t, nil)
	c, recorder := newJSONRequestContext(http.MethodPost, "/mtlf/training-complete", []byte(`{"status":"success"}`))

	server.HandleDaisyTrainingComplete(c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}

	problem := decodeProblemDetailsResponse(t, recorder)
	if problem.Title != "Mandatory IEs are missing" {
		t.Fatalf("title = %q", problem.Title)
	}
	if problem.Cause != "MANDATORY_IE_MISSING" {
		t.Fatalf("cause = %q", problem.Cause)
	}
	if problem.Detail != "task_id is required" {
		t.Fatalf("detail = %q", problem.Detail)
	}
}

func TestHandleDaisyTrainingComplete_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProcessor := NewMockprocessorAPI(ctrl)
	mockProcessor.EXPECT().
		HandleDaisyCallback("task-1", "http://example.com/model.onnx", "success", "")

	server := newHandlerTestServer(t, mockProcessor)
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
