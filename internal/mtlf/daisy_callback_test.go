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
	service := &MtlfService{}
	c, recorder := newCallbackRequestContext(http.MethodPost, "/mtlf/training-complete", []byte(`{"task_id":`))

	service.HandleDaisyTrainingComplete(c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}

	body := decodeCallbackProblemDetails(t, recorder)
	if body["title"] != malformedRequestSyntaxTitle {
		t.Fatalf("title = %v", body["title"])
	}
}

func TestHandleDaisyTrainingComplete_MissingTaskID(t *testing.T) {
	service := &MtlfService{}
	c, recorder := newCallbackRequestContext(http.MethodPost, "/mtlf/training-complete", []byte(`{"status":"success"}`))

	service.HandleDaisyTrainingComplete(c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}

	body := decodeCallbackProblemDetails(t, recorder)
	if body["cause"] != "MANDATORY_IE_MISSING" {
		t.Fatalf("cause = %v", body["cause"])
	}
}

func TestHandleDaisyTrainingComplete_Success(t *testing.T) {
	service := &MtlfService{}
	service.inFlight.Store("task-1", &inFlightEntry{oldModelUrl: "old-model"})

	c, recorder := newCallbackRequestContext(
		http.MethodPost,
		"/mtlf/training-complete",
		[]byte(`{"task_id":"task-1","model_url":"http://example.com/model.onnx","status":"success"}`),
	)

	service.HandleDaisyTrainingComplete(c)
	c.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
	if _, ok := service.inFlight.Load("task-1"); ok {
		t.Fatal("inFlight entry should be removed after callback handling")
	}
}
