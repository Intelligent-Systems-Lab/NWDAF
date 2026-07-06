package mtlf

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

const malformedRequestSyntaxTitle = "Malformed request syntax"

type fakeTrainingCompleteProcessor struct {
	taskID   string
	modelURL string
	status   string
	errMsg   string
	calls    int
}

func (f *fakeTrainingCompleteProcessor) HandleDaisyTrainingComplete(
	taskID, modelURL, status, errMsg string,
) {
	f.taskID = taskID
	f.modelURL = modelURL
	f.status = status
	f.errMsg = errMsg
	f.calls++
}

func newCallbackRequestContext(method, target string, body []byte) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, target, strings.NewReader(string(body)))
	c.Request.Header.Set("Content-Type", "application/json")
	return c, recorder
}

func decodeCallbackProblemDetails(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode problem details: %v", err)
	}
	return body
}

func TestHandleDaisyTrainingComplete_InvalidPayload(t *testing.T) {
	server := &Server{}
	c, recorder := newCallbackRequestContext(http.MethodPost, "/mtlf/training-complete", []byte(`{"task_id":`))

	server.HandleDaisyTrainingComplete(c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}

	body := decodeCallbackProblemDetails(t, recorder)
	if body["title"] != malformedRequestSyntaxTitle {
		t.Fatalf("title = %v", body["title"])
	}
}

func TestHandleDaisyTrainingComplete_MissingTaskID(t *testing.T) {
	server := &Server{}
	c, recorder := newCallbackRequestContext(http.MethodPost, "/mtlf/training-complete", []byte(`{"status":"success"}`))

	server.HandleDaisyTrainingComplete(c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}

	body := decodeCallbackProblemDetails(t, recorder)
	if body["cause"] != "MANDATORY_IE_MISSING" {
		t.Fatalf("cause = %v", body["cause"])
	}
}

func TestHandleDaisyTrainingComplete_DelegatesToProcessor(t *testing.T) {
	processor := &fakeTrainingCompleteProcessor{}
	server := &Server{processor: processor}

	c, recorder := newCallbackRequestContext(
		http.MethodPost,
		"/mtlf/training-complete",
		[]byte(`{"task_id":"task-1","model_url":"http://example.com/model.onnx","status":"success"}`),
	)

	server.HandleDaisyTrainingComplete(c)
	c.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
	if processor.calls != 1 {
		t.Fatalf("processor call count = %d, want 1", processor.calls)
	}
	if processor.taskID != "task-1" {
		t.Fatalf("taskID = %q, want %q", processor.taskID, "task-1")
	}
}
