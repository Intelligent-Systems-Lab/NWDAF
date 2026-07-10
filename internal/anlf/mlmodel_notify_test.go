package anlf

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

const malformedRequestSyntaxTitle = "Malformed request syntax"

type fakeMlModelNotifyProcessor struct {
	callCount     int
	notifications []models.NwdafMlModelProvNotif
}

func (f *fakeMlModelNotifyProcessor) HandleMlModelProvisionNotify(notifications []models.NwdafMlModelProvNotif) {
	f.callCount++
	f.notifications = notifications
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

func setupProvisionSubscription(subscriptionID, mtlfSubscriptionID string) *nwdaf_context.MlModelInfo {
	addTestSubscription(subscriptionID)
	mlInfo := nwdaf_context.NewMlModelInfo(models.NwdafEvent_UE_COMMUNICATION, "http://mtlf.example")
	mlInfo.SetMtlfSubscription(mtlfSubscriptionID)
	nwdaf_context.GetSelf().SetMlModelInfo(subscriptionID, mlInfo)
	return mlInfo
}

func provisionNotification(subscriptionID, mtlfSubscriptionID, modelURL string) models.NwdafMlModelProvNotif {
	return models.NwdafMlModelProvNotif{
		SubscriptionId: mtlfSubscriptionID,
		EventNotifs: []models.MlEventNotif{{
			Event:        models.NwdafEvent_UE_COMMUNICATION,
			NotifCorreId: subscriptionID,
			MLFileAddr:   &models.MlModelAddr{MLModelUrl: modelURL},
		}},
	}
}

func TestPlanModelProvisionActionsBuildsSpecAlignedApplyRequest(t *testing.T) {
	nwdaf_context.Init()
	setupProvisionSubscription("sub-123", "mtlf-sub-1")
	service := NewAnlfService(testNwdafApp{ctx: context.Background()}, nil)
	notif := provisionNotification("sub-123", "mtlf-sub-1", "http://example.com/model.onnx")

	actions := service.PlanModelProvisionActions(&notif)

	if len(actions) != 1 {
		t.Fatalf("action count = %d, want 1", len(actions))
	}
	request := actions[0].Request
	if request.Subscription.SubscriptionID != "sub-123" {
		t.Fatalf("subscription = %q", request.Subscription.SubscriptionID)
	}
	if request.ProvisionContext == nil ||
		request.ProvisionContext.MLEventNotification.MLFileAddr.MLModelUrl != "http://example.com/model.onnx" {
		t.Fatalf("provision context = %+v", request.ProvisionContext)
	}
}

func TestPlanModelProvisionActionsResolvesMtlfSubscriptionCorrelation(t *testing.T) {
	nwdaf_context.Init()
	setupProvisionSubscription("sub-123", "mtlf-sub-1")
	service := NewAnlfService(testNwdafApp{ctx: context.Background()}, nil)
	notif := provisionNotification("", "mtlf-sub-1", "http://example.com/model.onnx")

	actions := service.PlanModelProvisionActions(&notif)

	if len(actions) != 1 || actions[0].Request.Subscription.SubscriptionID != "sub-123" {
		t.Fatalf("actions = %+v", actions)
	}
}

func TestExecuteModelProvisionActionsUsesBackendOwnedLifecycle(t *testing.T) {
	nwdaf_context.Init()
	mlInfo := setupProvisionSubscription("sub-123", "mtlf-sub-1")
	client := &fakeAnlfBackendClient{applyResponse: &ApplySubscriptionRuntimeResponse{
		SubscriptionID:       "sub-123",
		RuntimeState:         "READY",
		Result:               ApplyResultActivated,
		ActiveModelReference: "http://example.com/model.onnx",
	}}
	service := NewAnlfService(testNwdafApp{ctx: context.Background()}, client)
	notif := provisionNotification("sub-123", "mtlf-sub-1", "http://example.com/model.onnx")

	service.ExecuteModelProvisionActions(service.PlanModelProvisionActions(&notif))

	if client.applyCalls != 1 {
		t.Fatalf("apply calls = %d, want 1", client.applyCalls)
	}
	if !mlInfo.IsReady() || mlInfo.GetModelURL() != "http://example.com/model.onnx" {
		t.Fatalf("ML correlation state = %+v", mlInfo)
	}
}

func TestExecuteModelProvisionActionsPreservesOldCorrelationOnReplacementFailure(t *testing.T) {
	nwdaf_context.Init()
	mlInfo := setupProvisionSubscription("sub-123", "mtlf-sub-1")
	mlInfo.SetModelUrl("http://example.com/model-old.onnx")
	mlInfo.SetModelReady()
	oldShared, _ := nwdaf_context.GetSelf().GetOrCreateSharedModel(
		"http://example.com/model-old.onnx",
		models.NwdafEvent_UE_COMMUNICATION,
	)
	oldShared.AddSubscriber("sub-123")

	client := &fakeAnlfBackendClient{applyResponse: &ApplySubscriptionRuntimeResponse{
		SubscriptionID:       "sub-123",
		RuntimeState:         "READY",
		Result:               ApplyResultFailedUsingPrevious,
		FallbackApplied:      true,
		ActiveModelReference: "http://example.com/model-old.onnx",
		Message:              "replacement failed",
	}}
	service := NewAnlfService(testNwdafApp{ctx: context.Background()}, client)
	notif := provisionNotification("sub-123", "mtlf-sub-1", "http://example.com/model-new.onnx")

	service.ExecuteModelProvisionActions(service.PlanModelProvisionActions(&notif))

	if mlInfo.GetModelURL() != "http://example.com/model-old.onnx" {
		t.Fatalf("model reference = %q, want old reference", mlInfo.GetModelURL())
	}
	if nwdaf_context.GetSelf().GetSharedModel("http://example.com/model-old.onnx") == nil {
		t.Fatal("old shared correlation should remain after backend fallback")
	}
}

func TestBuildProvisionNotificationURIUsesAnlfServerConfig(t *testing.T) {
	service := NewAnlfService(testNwdafApp{cfg: &factory.Config{
		Configuration: &factory.Configuration{Anlf: &factory.AnlfConfig{
			Server: &factory.AuxiliaryServerConfig{
				BindingIPv4:  "127.0.0.1",
				RegisterIPv4: "10.0.0.8",
				Port:         9010,
			},
		}},
	}}, nil)

	if got := service.BuildProvisionNotificationURI(); got != "http://10.0.0.8:9010/mlmodel-notify" {
		t.Fatalf("BuildProvisionNotificationURI() = %q", got)
	}
}
