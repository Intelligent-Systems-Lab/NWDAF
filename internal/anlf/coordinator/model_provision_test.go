package coordinator

import (
	"context"
	"testing"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

const provisionTestModelURL = "http://example.com/model.onnx"

func setupProvisionSubscription(subscriptionID, mtlfSubscriptionID string) *nwdaf_context.MlModelInfo {
	addTestSubscription(subscriptionID)
	mlInfo := nwdaf_context.NewMlModelInfo(models.NwdafEvent_UE_COMMUNICATION, "http://mtlf.example")
	mlInfo.SetMtlfSubscription(mtlfSubscriptionID)
	nwdaf_context.GetSelf().SetMlModelInfo(subscriptionID, mlInfo)
	return mlInfo
}

func provisionNotification(subscriptionID, mtlfSubscriptionID, modelURL string) contract.ModelProvisionNotification {
	return contract.ModelProvisionNotification{
		SubscriptionID: mtlfSubscriptionID,
		EventNotifications: []contract.MLEventNotification{{
			MlEventNotif: models.MlEventNotif{
				Event:        models.NwdafEvent_UE_COMMUNICATION,
				NotifCorreId: subscriptionID,
				MLFileAddr:   &models.MlModelAddr{MLModelUrl: modelURL},
			},
		}},
	}
}

func TestPlanModelProvisionActionsBuildsSpecAlignedApplyRequest(t *testing.T) {
	nwdaf_context.Init()
	setupProvisionSubscription("sub-123", "mtlf-sub-1")
	service := newTestCoordinator(testNwdafApp{ctx: context.Background()}, nil)
	notif := provisionNotification("sub-123", "mtlf-sub-1", provisionTestModelURL)

	actions := service.PlanModelProvisionActions(&notif)

	if len(actions) != 1 {
		t.Fatalf("action count = %d, want 1", len(actions))
	}
	request := actions[0].Request
	if request.Subscription.SubscriptionID != "sub-123" {
		t.Fatalf("subscription = %q", request.Subscription.SubscriptionID)
	}
	if request.ProvisionContext == nil ||
		request.ProvisionContext.MLEventNotification.MLFileAddr.MLModelUrl != provisionTestModelURL {
		t.Fatalf("provision context = %+v", request.ProvisionContext)
	}
}

func TestPlanModelProvisionActionsResolvesMtlfSubscriptionCorrelation(t *testing.T) {
	nwdaf_context.Init()
	setupProvisionSubscription("sub-123", "mtlf-sub-1")
	service := newTestCoordinator(testNwdafApp{ctx: context.Background()}, nil)
	notif := provisionNotification("", "mtlf-sub-1", provisionTestModelURL)

	actions := service.PlanModelProvisionActions(&notif)

	if len(actions) != 1 || actions[0].Request.Subscription.SubscriptionID != "sub-123" {
		t.Fatalf("actions = %+v", actions)
	}
}

func TestModelProvisionNotificationWithIdentityForwardsCompleteEvent(t *testing.T) {
	nwdaf_context.Init()
	client := &fakeAnlfBackendClient{}
	service := newTestCoordinator(testNwdafApp{ctx: context.Background()}, client)
	modelID := int64(42)
	notif := provisionNotification("sub-123", "mtlf-sub-1", provisionTestModelURL)
	notif.EventNotifications[0].ModelUniqueID = &modelID
	notif.EventNotifications[0].ModelProviderID = "mtlf-a"
	notif.EventNotifications[0].ModelUpdateInd = true

	service.ExecuteModelProvisionActions(service.PlanModelProvisionActions(&notif))

	if client.applyCalls != 0 {
		t.Fatalf("runtime apply calls = %d, want 0", client.applyCalls)
	}
	if len(client.provisionEvents) != 1 {
		t.Fatalf("provision event count = %d, want 1", len(client.provisionEvents))
	}
	event := client.provisionEvents[0]
	if event.ModelIdentity.ProviderID != "mtlf-a" || event.ModelIdentity.ModelUniqueID != 42 {
		t.Fatalf("model identity = %+v", event.ModelIdentity)
	}
	if !event.ModelUpdateInd || event.Artifact.MLModelURL != provisionTestModelURL {
		t.Fatalf("provision event = %+v", event)
	}
}

func TestExecuteModelProvisionActionsUsesBackendOwnedLifecycle(t *testing.T) {
	nwdaf_context.Init()
	mlInfo := setupProvisionSubscription("sub-123", "mtlf-sub-1")
	client := &fakeAnlfBackendClient{applyResponse: &contract.ApplySubscriptionRuntimeResponse{
		SubscriptionID:       "sub-123",
		RuntimeState:         "READY",
		Result:               contract.ApplyResultActivated,
		ActiveModelReference: provisionTestModelURL,
	}}
	service := newTestCoordinator(testNwdafApp{ctx: context.Background()}, client)
	notif := provisionNotification("sub-123", "mtlf-sub-1", provisionTestModelURL)

	service.ExecuteModelProvisionActions(service.PlanModelProvisionActions(&notif))

	if client.applyCalls != 1 {
		t.Fatalf("apply calls = %d, want 1", client.applyCalls)
	}
	if !mlInfo.IsReady() || mlInfo.GetModelURL() != provisionTestModelURL {
		t.Fatalf("ML correlation state = %+v", mlInfo)
	}
}

func TestExecuteModelProvisionActionsPreservesOldCorrelationOnReplacementFailure(t *testing.T) {
	nwdaf_context.Init()
	mlInfo := setupProvisionSubscription("sub-123", "mtlf-sub-1")
	mlInfo.SetModelUrl("http://example.com/model-old.onnx")
	mlInfo.SetModelReady()
	client := &fakeAnlfBackendClient{applyResponse: &contract.ApplySubscriptionRuntimeResponse{
		SubscriptionID:       "sub-123",
		RuntimeState:         "READY",
		Result:               contract.ApplyResultFailedUsingPrevious,
		FallbackApplied:      true,
		ActiveModelReference: "http://example.com/model-old.onnx",
		Message:              "replacement failed",
	}}
	service := newTestCoordinator(testNwdafApp{ctx: context.Background()}, client)
	notif := provisionNotification("sub-123", "mtlf-sub-1", "http://example.com/model-new.onnx")

	service.ExecuteModelProvisionActions(service.PlanModelProvisionActions(&notif))

	if mlInfo.GetModelURL() != "http://example.com/model-old.onnx" {
		t.Fatalf("model reference = %q, want old reference", mlInfo.GetModelURL())
	}
}

func TestBuildProvisionNotificationURIUsesAnlfServerConfig(t *testing.T) {
	service := newTestCoordinator(testNwdafApp{ctx: context.Background(), cfg: &factory.Config{
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
