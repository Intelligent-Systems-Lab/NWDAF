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

func TestHandleMlModelProvisionNotify_InvalidJSON(t *testing.T) {
	server := &Server{}
	c, recorder := newJSONRequestContext(http.MethodPost, "/mlmodel-notify", []byte(`{"subscriptionId":`))

	server.HandleMlModelProvisionNotify(c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}

	problem := decodeProblemDetailsResponse(t, recorder)
	if problem.Title != malformedRequestSyntaxTitle {
		t.Fatalf("title = %q", problem.Title)
	}
}

func TestHandleMlModelProvisionNotify_EmptyNotificationList(t *testing.T) {
	server := &Server{}
	c, recorder := newJSONRequestContext(http.MethodPost, "/mlmodel-notify", []byte(`[]`))

	server.HandleMlModelProvisionNotify(c)
	c.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
}

func TestHandleMlModelProvisionNotify_DelegatesToProcessor(t *testing.T) {
	processor := &fakeMlModelNotifyProcessor{}
	server := &Server{processor: processor}
	body := `[
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
	c, recorder := newJSONRequestContext(http.MethodPost, "/mlmodel-notify", []byte(body))

	server.HandleMlModelProvisionNotify(c)
	c.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
	if processor.callCount != 1 {
		t.Fatalf("processor call count = %d, want 1", processor.callCount)
	}
	if len(processor.notifications) != 1 {
		t.Fatalf("notifications length = %d, want 1", len(processor.notifications))
	}
}

func TestPlanModelProvisionActions_ResolvesActivation(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	ctx.SetMlModelInfo("sub-123", nwdaf_context.NewMlModelInfo(models.NwdafEvent_UE_COMMUNICATION, "mtlf"))

	service := NewAnlfService(testNwdafApp{
		ctx: context.Background(),
		cfg: &factory.Config{
			Configuration: &factory.Configuration{
				AnlfBackend: &factory.AnlfBackendConfig{
					Enabled:  true,
					Endpoint: "http://anlf-backend.example",
				},
			},
		},
	}, nil)

	notif := models.NwdafMlModelProvNotif{
		SubscriptionId: "mtlf-sub-1",
		EventNotifs: []models.MlEventNotif{
			{
				Event:        models.NwdafEvent_UE_COMMUNICATION,
				NotifCorreId: "sub-123",
				MLFileAddr: &models.MlModelAddr{
					MLModelUrl: "http://example.com/model.onnx",
				},
			},
		},
	}

	actions := service.PlanModelProvisionActions(&notif)

	if len(actions) != 1 {
		t.Fatalf("action count = %d, want 1", len(actions))
	}
	if actions[0].NwdafSubID != "sub-123" {
		t.Fatalf("action subscription = %q, want %q", actions[0].NwdafSubID, "sub-123")
	}
	if actions[0].ModelURL != "http://example.com/model.onnx" {
		t.Fatalf("action modelURL = %q", actions[0].ModelURL)
	}
	if status := ctx.GetMlModelInfo("sub-123").GetStatus(); status != nwdaf_context.MlModelStatus_PENDING {
		t.Fatalf("mlInfo status = %q, want %q", status, nwdaf_context.MlModelStatus_PENDING)
	}
}

func TestPlanModelProvisionActions_UsesPerEventCorrelation(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	service := NewAnlfService(testNwdafApp{
		ctx: context.Background(),
		cfg: &factory.Config{
			Configuration: &factory.Configuration{
				AnlfBackend: &factory.AnlfBackendConfig{
					Enabled:  true,
					Endpoint: "http://anlf-backend.example",
				},
			},
		},
	}, nil)

	ctx.SetMlModelInfo("sub-a", nwdaf_context.NewMlModelInfo(models.NwdafEvent_UE_COMMUNICATION, "mtlf"))
	ctx.SetMlModelInfo("sub-b", nwdaf_context.NewMlModelInfo(models.NwdafEvent_UE_COMMUNICATION, "mtlf"))

	notif := models.NwdafMlModelProvNotif{
		SubscriptionId: "fallback-sub",
		EventNotifs: []models.MlEventNotif{
			{
				Event:        models.NwdafEvent_UE_COMMUNICATION,
				NotifCorreId: "sub-a",
				MLFileAddr:   &models.MlModelAddr{MLModelUrl: "http://example.com/model-a.onnx"},
			},
			{
				Event:        models.NwdafEvent_UE_COMMUNICATION,
				NotifCorreId: "sub-b",
				MLFileAddr:   &models.MlModelAddr{MLModelUrl: "http://example.com/model-b.onnx"},
			},
		},
	}

	actions := service.PlanModelProvisionActions(&notif)

	if len(actions) != 2 {
		t.Fatalf("action count = %d, want 2", len(actions))
	}
	if actions[0].NwdafSubID != "sub-a" {
		t.Fatalf("first action subscription = %q, want %q", actions[0].NwdafSubID, "sub-a")
	}
	if actions[1].NwdafSubID != "sub-b" {
		t.Fatalf("second action subscription = %q, want %q", actions[1].NwdafSubID, "sub-b")
	}
}

func TestExecuteModelProvisionActions_InitializesModel(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	mlInfo := nwdaf_context.NewMlModelInfo(models.NwdafEvent_UE_COMMUNICATION, "mtlf")
	ctx.SetMlModelInfo("sub-123", mlInfo)

	client := &fakeAnlfBackendClient{modelID: "model-123"}
	service := NewAnlfService(testNwdafApp{
		ctx: context.Background(),
		cfg: &factory.Config{
			Configuration: &factory.Configuration{
				AnlfBackend: &factory.AnlfBackendConfig{
					Enabled:  true,
					Endpoint: "http://anlf-backend.example",
				},
			},
		},
	}, client)

	service.ExecuteModelProvisionActions([]ModelProvisionAction{
		{
			NwdafSubID: "sub-123",
			ModelURL:   "http://example.com/model.onnx",
			Event:      models.NwdafEvent_UE_COMMUNICATION,
		},
	})

	if client.loadCalls != 1 {
		t.Fatalf("LoadModel called %d times, want 1", client.loadCalls)
	}
	if got := mlInfo.GetStatus(); got != nwdaf_context.MlModelStatus_READY {
		t.Fatalf("mlInfo status = %q, want %q", got, nwdaf_context.MlModelStatus_READY)
	}
	if got := mlInfo.ModelUrl; got != "http://example.com/model.onnx" {
		t.Fatalf("mlInfo modelUrl = %q, want %q", got, "http://example.com/model.onnx")
	}
}

func TestExecuteModelProvisionActions_CleansUpOldModelStateOnSwitch(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()

	const (
		subID       = "sub-123"
		oldModelURL = "http://example.com/model-old.onnx"
		newModelURL = "http://example.com/model-new.onnx"
	)

	mlInfo := nwdaf_context.NewMlModelInfo(models.NwdafEvent_UE_COMMUNICATION, "mtlf")
	mlInfo.SetModelUrl(oldModelURL)
	ctx.SetMlModelInfo(subID, mlInfo)

	oldShared, _ := ctx.GetOrCreateSharedModel(oldModelURL, models.NwdafEvent_UE_COMMUNICATION)
	oldShared.AddSubscriber(subID)
	ctx.GetOrCreateModelAccuracyStore(oldModelURL)

	client := &fakeAnlfBackendClient{modelID: "model-456"}
	service := NewAnlfService(testNwdafApp{
		ctx: context.Background(),
		cfg: &factory.Config{
			Configuration: &factory.Configuration{
				AnlfBackend: &factory.AnlfBackendConfig{
					Enabled:  true,
					Endpoint: "http://anlf-backend.example",
				},
				Mtlf: &factory.MtlfConfig{
					Enabled: true,
					AccuracyMonitor: &factory.AccuracyMonitorConfig{
						Enabled: true,
					},
				},
			},
		},
	}, client)

	service.ExecuteModelProvisionActions([]ModelProvisionAction{
		{
			NwdafSubID: subID,
			ModelURL:   newModelURL,
			Event:      models.NwdafEvent_UE_COMMUNICATION,
		},
	})

	if ctx.GetSharedModel(oldModelURL) != nil {
		t.Fatal("old shared model should be removed after switching to a new model")
	}
	if ctx.GetModelAccuracyStore(oldModelURL) != nil {
		t.Fatal("old accuracy store should be removed after switching to a new model")
	}
	if newShared := ctx.GetSharedModel(newModelURL); newShared == nil {
		t.Fatal("new shared model should exist after switching to a new model")
	} else if newShared.SubscriberCount() != 1 {
		t.Fatalf("new shared model subscriber count = %d, want 1", newShared.SubscriberCount())
	}
	if got := mlInfo.ModelUrl; got != newModelURL {
		t.Fatalf("mlInfo modelUrl = %q, want %q", got, newModelURL)
	}
}

func TestBuildProvisionNotificationURIUsesAnlfServerConfig(t *testing.T) {
	service := NewAnlfService(testNwdafApp{
		cfg: &factory.Config{
			Configuration: &factory.Configuration{
				Anlf: &factory.AnlfConfig{
					Server: &factory.AuxiliaryServerConfig{
						BindingIPv4:  "127.0.0.1",
						RegisterIPv4: "10.0.0.8",
						Port:         9010,
					},
				},
			},
		},
	}, nil)

	if got := service.BuildProvisionNotificationURI(); got != "http://10.0.0.8:9010/mlmodel-notify" {
		t.Fatalf("BuildProvisionNotificationURI() = %q, want %q",
			got, "http://10.0.0.8:9010/mlmodel-notify")
	}
}
