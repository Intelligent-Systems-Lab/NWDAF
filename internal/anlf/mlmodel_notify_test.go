package anlf

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

const malformedRequestSyntaxTitle = "Malformed request syntax"

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

func TestHandleMlModelProvisionNotify_InvalidJSON(t *testing.T) {
	service := NewAnlfService(testNwdafApp{}, nil)
	c, recorder := newJSONRequestContext(http.MethodPost, "/mlmodel-notify", []byte(`{"subscriptionId":`))

	service.HandleMlModelProvisionNotify(c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}

	problem := decodeProblemDetailsResponse(t, recorder)
	if problem.Title != malformedRequestSyntaxTitle {
		t.Fatalf("title = %q", problem.Title)
	}
}

func TestHandleMlModelProvisionNotify_EmptyNotificationList(t *testing.T) {
	service := NewAnlfService(testNwdafApp{}, nil)
	c, recorder := newJSONRequestContext(http.MethodPost, "/mlmodel-notify", []byte(`[]`))

	service.HandleMlModelProvisionNotify(c)
	c.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
}

func TestHandleMlModelProvisionNotify_InitializesModel(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.SetMlModelInfo("sub-123", nwdaf_context.NewMlModelInfo(models.NwdafEvent_UE_COMMUNICATION, "mtlf"))

	done := make(chan struct{})
	client := &fakeInferenceEngineClient{modelID: "model-123"}
	service := NewAnlfService(testNwdafApp{
		ctx: context.Background(),
		cfg: &factory.Config{
			Configuration: &factory.Configuration{
				InferenceEngine: &factory.InferenceEngineConfig{
					Enabled:  true,
					Endpoint: "http://inference-engine.example",
				},
			},
		},
	}, client)

	notificationBody := `[
		{
			"subscriptionId":"mtlf-sub-1",
			"eventNotifs":[
				{
					"event":"UE_COMMUNICATION",
					"notifCorreId":"sub-123",
					"mLFileAddr":{"mLModelUrl":"http://example.com/model.onnx"}
				}
			]
		}
	]`
	c, recorder := newJSONRequestContext(http.MethodPost, "/mlmodel-notify", []byte(notificationBody))

	go func() {
		for {
			if client.initializeCalls > 0 {
				close(done)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	service.HandleMlModelProvisionNotify(c)
	c.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected InitializeMlModel to be invoked")
	}
}
