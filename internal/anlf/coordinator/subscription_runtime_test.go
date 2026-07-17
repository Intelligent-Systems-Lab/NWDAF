package coordinator

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

type fakeAvailabilityGate struct {
	usable    bool
	markCalls int
}

func (g *fakeAvailabilityGate) Usable() bool { return g.usable }

func (g *fakeAvailabilityGate) MarkUnavailable(string) {
	g.usable = false
	g.markCalls++
}

type backendStatusError int

func (e backendStatusError) Error() string       { return http.StatusText(int(e)) }
func (e backendStatusError) HTTPStatusCode() int { return int(e) }

type fakeAnlfBackendClient struct {
	applyCalls      int
	applyRequests   []contract.ApplySubscriptionRuntimeRequest
	applyResponse   *contract.ApplySubscriptionRuntimeResponse
	applyErr        error
	releaseCalls    []string
	releaseErr      error
	syncCalls       int
	sendCalls       int
	sendErrors      []error
	sentBatches     []contract.ObservationBatch
	provisionEvents []contract.ModelProvisionEvent
}

func (f *fakeAnlfBackendClient) SyncModelProvisionBinding(
	context.Context,
	string,
	contract.ModelProvisionBinding,
) error {
	return nil
}

func (f *fakeAnlfBackendClient) ApplyModelProvisionEvent(
	_ context.Context,
	event contract.ModelProvisionEvent,
) (*contract.ModelProvisionEventResponse, error) {
	f.provisionEvents = append(f.provisionEvents, event)
	return &contract.ModelProvisionEventResponse{
		Status: "APPLIED", AffectedRuntimeCount: 1, Generation: 2,
	}, nil
}

func (f *fakeAnlfBackendClient) ApplySubscriptionRuntime(
	_ context.Context,
	request contract.ApplySubscriptionRuntimeRequest,
) (*contract.ApplySubscriptionRuntimeResponse, error) {
	f.applyCalls++
	f.applyRequests = append(f.applyRequests, request)
	if f.applyResponse != nil {
		if f.applyResponse.RuntimeRevision == 0 {
			f.applyResponse.RuntimeRevision = 1
		}
		if f.applyResponse.CollectionRequirements.SamplingIntervalSeconds == 0 {
			f.applyResponse.CollectionRequirements = contract.CollectionRequirements{
				SamplingIntervalSeconds: 30,
				RequiredMeasurements:    []string{"UL_VOLUME"},
			}
		}
	}
	return f.applyResponse, f.applyErr
}

func (f *fakeAnlfBackendClient) ReleaseSubscriptionRuntime(_ context.Context, subscriptionID string) error {
	f.releaseCalls = append(f.releaseCalls, subscriptionID)
	return f.releaseErr
}

func (f *fakeAnlfBackendClient) SyncObservationBindings(
	_ context.Context,
	_ string,
	_ contract.SyncObservationBindingsRequest,
) error {
	f.syncCalls++
	return nil
}

func (f *fakeAnlfBackendClient) SendObservations(
	_ context.Context,
	_ string,
	batch contract.ObservationBatch,
) error {
	f.sendCalls++
	f.sentBatches = append(f.sentBatches, batch)
	if len(f.sendErrors) >= f.sendCalls {
		return f.sendErrors[f.sendCalls-1]
	}
	return nil
}

func (f *fakeAnlfBackendClient) HTTPClient() *http.Client { return &http.Client{} }

type testNwdafApp struct {
	ctx context.Context
	cfg *factory.Config
}

func (testNwdafApp) SetLogEnable(bool)                    {}
func (testNwdafApp) SetLogLevel(string)                   {}
func (testNwdafApp) SetReportCaller(bool)                 {}
func (testNwdafApp) Start()                               {}
func (testNwdafApp) Terminate()                           {}
func (a testNwdafApp) Config() *factory.Config            { return a.cfg }
func (testNwdafApp) Context() *nwdaf_context.NWDAFContext { return nwdaf_context.GetSelf() }
func (a testNwdafApp) CancelContext() context.Context     { return a.ctx }

func newTestCoordinator(app testNwdafApp, client *fakeAnlfBackendClient) *Coordinator {
	var runtimeClient BackendRuntimeClient
	var sender ObservationSender
	if client != nil {
		runtimeClient = client
		sender = client
	}
	delivery := NewObservationDelivery(app.ctx, sender, nil)
	return New(app, runtimeClient, delivery)
}

func TestAvailabilityGatePreventsLiveBackendRequest(t *testing.T) {
	nwdaf_context.Init()
	client := &fakeAnlfBackendClient{}
	gate := &fakeAvailabilityGate{usable: false}
	coordinator := New(
		testNwdafApp{ctx: context.Background()},
		client,
		NewObservationDelivery(context.Background(), client, nil),
		gate,
	)

	_, err := coordinator.ApplySubscriptionRuntime(contract.ApplySubscriptionRuntimeRequest{})
	if !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("ApplySubscriptionRuntime() error = %v", err)
	}
	if client.applyCalls != 0 {
		t.Fatalf("backend apply calls = %d, want 0", client.applyCalls)
	}
}

func TestBackendFailureClassificationUpdatesAvailability(t *testing.T) {
	nwdaf_context.Init()
	tests := []struct {
		name      string
		err       error
		wantMarks int
	}{
		{name: "domain 4xx", err: backendStatusError(http.StatusConflict)},
		{name: "server 5xx", err: backendStatusError(http.StatusServiceUnavailable), wantMarks: 1},
		{name: "transport", err: errors.New("connection reset"), wantMarks: 1},
		{name: "shutdown cancellation", err: context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeAnlfBackendClient{applyErr: test.err}
			gate := &fakeAvailabilityGate{usable: true}
			coordinator := New(
				testNwdafApp{ctx: context.Background()},
				client,
				NewObservationDelivery(context.Background(), client, nil),
				gate,
			)
			_, applyErr := coordinator.ApplySubscriptionRuntime(contract.ApplySubscriptionRuntimeRequest{})
			if !errors.Is(applyErr, test.err) && applyErr.Error() != test.err.Error() {
				t.Fatalf("ApplySubscriptionRuntime() error = %v, want %v", applyErr, test.err)
			}
			if gate.markCalls != test.wantMarks {
				t.Fatalf("MarkUnavailable calls = %d, want %d", gate.markCalls, test.wantMarks)
			}
		})
	}
}

func addTestSubscription(subscriptionID string) {
	nwdaf_context.GetSelf().AddSubscription(&nwdaf_context.Subscription{
		ID:          subscriptionID,
		IsActive:    true,
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

	client := &fakeAnlfBackendClient{applyResponse: &contract.ApplySubscriptionRuntimeResponse{
		SubscriptionID:       "sub-1",
		RuntimeState:         "READY",
		Result:               contract.ApplyResultActivated,
		ActiveModelReference: "http://example.com/model-a",
	}}
	service := newTestCoordinator(testNwdafApp{
		ctx: context.Background(),
		cfg: &factory.Config{Configuration: &factory.Configuration{
			AnlfBackend: &factory.AnlfBackendConfig{Enabled: true, Endpoint: "http://anlf-backend.example"},
		}},
	}, client)

	request, err := service.BuildSubscriptionRuntimeRequest("sub-1", nil)
	if err != nil {
		t.Fatalf("BuildSubscriptionRuntimeRequest() error = %v", err)
	}
	if request.RuntimeCompletionCallbackURI == "" {
		t.Fatal("runtime completion callback URI was not populated")
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

	client := &fakeAnlfBackendClient{applyResponse: &contract.ApplySubscriptionRuntimeResponse{
		SubscriptionID:       "sub-1",
		RuntimeState:         "READY",
		Result:               contract.ApplyResultFailedUsingPrevious,
		FallbackApplied:      true,
		ActiveModelReference: "http://example.com/model-old",
		Message:              "download failed",
	}}
	service := newTestCoordinator(testNwdafApp{ctx: context.Background()}, client)
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
	client := &fakeAnlfBackendClient{}
	service := newTestCoordinator(testNwdafApp{ctx: context.Background()}, client)
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
