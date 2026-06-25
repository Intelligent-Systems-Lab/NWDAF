package anlf

import (
	"context"
	"net/http"
	"testing"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

type fakeMlServiceClient struct {
	initializeCalls int
	lastCtx         context.Context
	lastModelURL    string
	modelID         string
}

func (f *fakeMlServiceClient) InitializeModel(ctx context.Context, modelURL string) (string, error) {
	f.initializeCalls++
	f.lastCtx = ctx
	f.lastModelURL = modelURL
	return f.modelID, nil
}

func (f *fakeMlServiceClient) UnloadModel(context.Context, string) error { return nil }

func (f *fakeMlServiceClient) Predict(
	context.Context,
	string,
	[]consumer.TrafficObservation,
) (*consumer.PredictResponse, error) {
	return nil, nil
}

func (f *fakeMlServiceClient) HTTPClient() *http.Client { return &http.Client{} }

func TestInitializeMlModelUsesInjectedClient(t *testing.T) {
	nwdaf_context.Init()

	cfg := &factory.Config{
		Configuration: &factory.Configuration{
			MlService: &factory.MlServiceConfig{
				Enabled:  true,
				Endpoint: "http://ml-service.example",
			},
		},
	}
	client := &fakeMlServiceClient{modelID: "model-123"}
	service := NewAnlfService(testNwdafApp{
		ctx: context.Background(),
		cfg: cfg,
	}, client)

	mlInfo := nwdaf_context.NewMlModelInfo(models.NwdafEvent_UE_COMMUNICATION, "")
	const modelURL = "http://example.com/model.onnx"
	mlInfo.SetModelUrl(modelURL)

	service.InitializeMlModel("sub-1", mlInfo, modelURL)

	if client.initializeCalls != 1 {
		t.Fatalf("InitializeModel called %d times, want 1", client.initializeCalls)
	}
	if client.lastCtx == nil {
		t.Fatal("InitializeModel should receive a parent context")
	}
	if client.lastModelURL != modelURL {
		t.Fatalf("InitializeModel modelURL = %q, want %q", client.lastModelURL, modelURL)
	}
	if !mlInfo.IsReady() {
		t.Fatal("mlInfo should be READY after successful initialization")
	}
	if got := mlInfo.GetModelId(); got != "model-123" {
		t.Fatalf("mlInfo modelId = %q, want %q", got, "model-123")
	}
}
