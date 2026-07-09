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
	loadCalls    int
	lastCtx      context.Context
	lastModelURL string
	modelID      string
}

func (f *fakeAnlfBackendClient) LoadModel(ctx context.Context, modelURL string) (string, error) {
	f.loadCalls++
	f.lastCtx = ctx
	f.lastModelURL = modelURL
	return f.modelID, nil
}

func (f *fakeAnlfBackendClient) UnloadModel(context.Context, string) error { return nil }

func (f *fakeAnlfBackendClient) Predict(
	context.Context,
	string,
	[]TrafficObservation,
) (*PredictResponse, error) {
	return nil, nil
}

func (f *fakeAnlfBackendClient) HTTPClient() *http.Client { return &http.Client{} }

func TestInitializeMlModelUsesInjectedClient(t *testing.T) {
	nwdaf_context.Init()

	cfg := &factory.Config{
		Configuration: &factory.Configuration{
			AnlfBackend: &factory.AnlfBackendConfig{
				Enabled:  true,
				Endpoint: "http://anlf-backend.example",
			},
		},
	}
	client := &fakeAnlfBackendClient{modelID: "model-123"}
	service := NewAnlfService(testNwdafApp{
		ctx: context.Background(),
		cfg: cfg,
	}, client)

	mlInfo := nwdaf_context.NewMlModelInfo(models.NwdafEvent_UE_COMMUNICATION, "")
	const modelURL = "http://example.com/model.onnx"
	mlInfo.SetModelUrl(modelURL)

	service.InitializeMlModel("sub-1", mlInfo, modelURL)

	if client.loadCalls != 1 {
		t.Fatalf("LoadModel called %d times, want 1", client.loadCalls)
	}
	if client.lastCtx == nil {
		t.Fatal("LoadModel should receive a parent context")
	}
	if client.lastModelURL != modelURL {
		t.Fatalf("LoadModel modelURL = %q, want %q", client.lastModelURL, modelURL)
	}
	if !mlInfo.IsReady() {
		t.Fatal("mlInfo should be READY after successful initialization")
	}
	if got := mlInfo.GetModelId(); got != "model-123" {
		t.Fatalf("mlInfo modelId = %q, want %q", got, "model-123")
	}
}
