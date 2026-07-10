package anlf

import (
	"context"
	"net/http"
	"testing"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

type fakeAnlfBackendClient struct {
	applyCalls      int
	applyRequests   []ApplySubscriptionRuntimeRequest
	applyResponse   *ApplySubscriptionRuntimeResponse
	applyErr        error
	releaseCalls    []string
	releaseErr      error
	predictSubID    string
	predictResponse *PredictResponse
	predictErr      error
}

func (f *fakeAnlfBackendClient) ApplySubscriptionRuntime(
	_ context.Context,
	request ApplySubscriptionRuntimeRequest,
) (*ApplySubscriptionRuntimeResponse, error) {
	f.applyCalls++
	f.applyRequests = append(f.applyRequests, request)
	return f.applyResponse, f.applyErr
}

func (f *fakeAnlfBackendClient) ReleaseSubscriptionRuntime(_ context.Context, subscriptionID string) error {
	f.releaseCalls = append(f.releaseCalls, subscriptionID)
	return f.releaseErr
}

func (f *fakeAnlfBackendClient) Predict(
	_ context.Context,
	subscriptionID string,
	_ []TrafficObservation,
) (*PredictResponse, error) {
	f.predictSubID = subscriptionID
	return f.predictResponse, f.predictErr
}

func (f *fakeAnlfBackendClient) HTTPClient() *http.Client { return &http.Client{} }

func addTestSubscription(subscriptionID string) {
	nwdaf_context.GetSelf().AddSubscription(&nwdaf_context.Subscription{
		ID:          subscriptionID,
		NotifCorrId: "corr-" + subscriptionID,
		EventSubs: []models.NwdafEventsSubscriptionEventSubscription{
			{Event: models.NwdafEvent_UE_COMMUNICATION},
		},
	})
}

func TestApplySubscriptionRuntimeUpdatesOnlyCorrelationState(t *testing.T) {
	nwdaf_context.Init()
	addTestSubscription("sub-1")
	mlInfo := nwdaf_context.NewMlModelInfo(models.NwdafEvent_UE_COMMUNICATION, "")
	nwdaf_context.GetSelf().SetMlModelInfo("sub-1", mlInfo)

	client := &fakeAnlfBackendClient{applyResponse: &ApplySubscriptionRuntimeResponse{
		SubscriptionID:       "sub-1",
		RuntimeState:         "READY",
		Result:               ApplyResultActivated,
		ActiveModelReference: "http://example.com/model-a",
	}}
	service := NewAnlfService(testNwdafApp{
		ctx: context.Background(),
		cfg: &factory.Config{Configuration: &factory.Configuration{
			AnlfBackend: &factory.AnlfBackendConfig{Enabled: true, Endpoint: "http://anlf-backend.example"},
		}},
	}, client)

	request, err := service.BuildSubscriptionRuntimeRequest("sub-1", nil)
	if err != nil {
		t.Fatalf("BuildSubscriptionRuntimeRequest() error = %v", err)
	}
	if _, err = service.ApplySubscriptionRuntime(request); err != nil {
		t.Fatalf("ApplySubscriptionRuntime() error = %v", err)
	}

	if client.applyCalls != 1 {
		t.Fatalf("apply calls = %d, want 1", client.applyCalls)
	}
	if !mlInfo.IsReady() {
		t.Fatal("ML correlation should be READY after backend activation")
	}
	if got := mlInfo.GetModelURL(); got != "http://example.com/model-a" {
		t.Fatalf("model reference = %q", got)
	}
}

func TestApplySubscriptionRuntimeKeepsPreviousCorrelationOnFallback(t *testing.T) {
	nwdaf_context.Init()
	addTestSubscription("sub-1")
	mlInfo := nwdaf_context.NewMlModelInfo(models.NwdafEvent_UE_COMMUNICATION, "")
	mlInfo.SetModelUrl("http://example.com/model-old")
	mlInfo.SetModelReady()
	nwdaf_context.GetSelf().SetMlModelInfo("sub-1", mlInfo)

	client := &fakeAnlfBackendClient{applyResponse: &ApplySubscriptionRuntimeResponse{
		SubscriptionID:       "sub-1",
		RuntimeState:         "READY",
		Result:               ApplyResultFailedUsingPrevious,
		FallbackApplied:      true,
		ActiveModelReference: "http://example.com/model-old",
		Message:              "download failed",
	}}
	service := NewAnlfService(testNwdafApp{ctx: context.Background()}, client)
	request, err := service.BuildSubscriptionRuntimeRequest("sub-1", nil)
	if err != nil {
		t.Fatalf("BuildSubscriptionRuntimeRequest() error = %v", err)
	}

	if _, err = service.ApplySubscriptionRuntime(request); err == nil {
		t.Fatal("expected fallback outcome to be reported as an error")
	}
	if got := mlInfo.GetModelURL(); got != "http://example.com/model-old" {
		t.Fatalf("model reference = %q, want previous reference", got)
	}
}

func TestReleaseSubscriptionRuntimeCallsBackendAndRemovesCorrelation(t *testing.T) {
	nwdaf_context.Init()
	addTestSubscription("sub-1")
	mlInfo := nwdaf_context.NewMlModelInfo(models.NwdafEvent_UE_COMMUNICATION, "")
	mlInfo.SetModelUrl("http://example.com/model-a")
	mlInfo.SetModelReady()
	nwdaf_context.GetSelf().SetMlModelInfo("sub-1", mlInfo)
	shared, _ := nwdaf_context.GetSelf().GetOrCreateSharedModel(
		"http://example.com/model-a",
		models.NwdafEvent_UE_COMMUNICATION,
	)
	shared.AddSubscriber("sub-1")

	client := &fakeAnlfBackendClient{}
	service := NewAnlfService(testNwdafApp{ctx: context.Background()}, client)
	if err := service.ReleaseSubscriptionRuntime("sub-1"); err != nil {
		t.Fatalf("ReleaseSubscriptionRuntime() error = %v", err)
	}

	if len(client.releaseCalls) != 1 || client.releaseCalls[0] != "sub-1" {
		t.Fatalf("release calls = %v", client.releaseCalls)
	}
	if nwdaf_context.GetSelf().GetMlModelInfo("sub-1") != nil {
		t.Fatal("ML correlation should be removed after release")
	}
}

func TestApplyRetrainedModelUsesProvisionContractForAffectedSubscriptions(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	const (
		oldReference = "http://example.com/model-old"
		newReference = "http://example.com/model-new"
	)
	for _, subscriptionID := range []string{"sub-1", "sub-2"} {
		addTestSubscription(subscriptionID)
		mlInfo := nwdaf_context.NewMlModelInfo(models.NwdafEvent_UE_COMMUNICATION, "http://mtlf.example")
		mlInfo.SetMtlfSubscription("mtlf-" + subscriptionID)
		mlInfo.SetModelUrl(oldReference)
		mlInfo.SetModelReady()
		ctx.SetMlModelInfo(subscriptionID, mlInfo)
	}
	oldShared, _ := ctx.GetOrCreateSharedModel(oldReference, models.NwdafEvent_UE_COMMUNICATION)
	oldShared.AddSubscriber("sub-1")
	oldShared.AddSubscriber("sub-2")

	client := &fakeAnlfBackendClient{applyResponse: &ApplySubscriptionRuntimeResponse{
		RuntimeState:         "READY",
		Result:               ApplyResultReplaced,
		ActiveModelReference: newReference,
	}}
	service := NewAnlfService(testNwdafApp{ctx: context.Background()}, client)

	if err := service.ApplyRetrainedModel(oldReference, newReference); err != nil {
		t.Fatalf("ApplyRetrainedModel() error = %v", err)
	}
	if client.applyCalls != 2 {
		t.Fatalf("apply calls = %d, want 2", client.applyCalls)
	}
	for _, request := range client.applyRequests {
		provision := request.ProvisionContext
		if provision == nil || !provision.MLEventNotification.ModelUpdateInd {
			t.Fatalf("retrain request did not carry modelUpdateInd: %+v", provision)
		}
		if provision.MLEventNotification.MLFileAddr.MLModelUrl != newReference {
			t.Fatalf("new model reference = %q", provision.MLEventNotification.MLFileAddr.MLModelUrl)
		}
	}
	if ctx.GetSharedModel(oldReference) != nil {
		t.Fatal("old model correlation should be removed after all subscriptions move")
	}
	if shared := ctx.GetSharedModel(newReference); shared == nil || shared.SubscriberCount() != 2 {
		t.Fatalf("new model correlation = %+v", shared)
	}
}
