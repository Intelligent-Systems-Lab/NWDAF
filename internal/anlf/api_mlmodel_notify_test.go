package anlf

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/openapi/models"
)

const malformedRequestSyntaxTitle = "Malformed request syntax"

type fakeMlModelNotifyProcessor struct {
	callCount     int
	notifications []contract.ModelProvisionNotification
}

func (f *fakeMlModelNotifyProcessor) HandleMlModelProvisionNotify(notifications []contract.ModelProvisionNotification) {
	f.callCount++
	f.notifications = notifications
}

func (f *fakeMlModelNotifyProcessor) HandleAnalyticsReport(
	string,
	*contract.AnalyticsReport,
) error {
	return nil
}

func (*fakeMlModelNotifyProcessor) HandleModelAccuracyReport(*contract.ModelAccuracyReport) error {
	return nil
}

func (*fakeMlModelNotifyProcessor) HandleRuntimeCompletion(*contract.RuntimeCompletionEvent) error {
	return nil
}

func newJSONRequestContext(method, target string, body []byte) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, target, strings.NewReader(string(body)))
	c.Request.Header.Set("Content-Type", "application/json")
	return c, recorder
}

func decodeProblemDetailsResponse(t *testing.T, recorder *httptest.ResponseRecorder) models.ProblemDetails {
	t.Helper()
	var problem models.ProblemDetails
	if err := json.Unmarshal(recorder.Body.Bytes(), &problem); err != nil {
		t.Fatalf("failed to decode problem details: %v", err)
	}
	return problem
}

func TestHandleMlModelProvisionNotifyInvalidJSON(t *testing.T) {
	server := &Server{}
	c, recorder := newJSONRequestContext(http.MethodPost, "/mlmodel-notify", []byte(`{"subscriptionId":`))

	server.HandleMlModelProvisionNotify(c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if problem := decodeProblemDetailsResponse(t, recorder); problem.Title != malformedRequestSyntaxTitle {
		t.Fatalf("title = %q", problem.Title)
	}
}

func TestHandleMlModelProvisionNotifyEmptyNotificationList(t *testing.T) {
	server := &Server{}
	c, recorder := newJSONRequestContext(http.MethodPost, "/mlmodel-notify", []byte(`[]`))

	server.HandleMlModelProvisionNotify(c)
	c.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
}

func TestHandleMlModelProvisionNotifyDelegatesToProcessor(t *testing.T) {
	processor := &fakeMlModelNotifyProcessor{}
	server := &Server{processor: processor}
	body := `[{
		"subscriptionId":"mtlf-sub-1",
		"eventNotifs":[{
			"event":"UE_COMMUNICATION",
			"notifCorreId":"sub-123",
			"mLFileAddr":{"mLModelUrl":"http://example.com/model.onnx"}
		}]
	}]`
	c, recorder := newJSONRequestContext(http.MethodPost, "/mlmodel-notify", []byte(body))

	server.HandleMlModelProvisionNotify(c)
	c.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
	if processor.callCount != 1 || len(processor.notifications) != 1 {
		t.Fatalf("processor calls = %d notifications = %d", processor.callCount, len(processor.notifications))
	}
}
