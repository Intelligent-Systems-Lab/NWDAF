package mtlf

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

type fakeDaisyClient struct {
	triggerCalls    int
	lastCtx         context.Context
	lastCallbackURL string
	lastModelTask   map[string]any
	taskID          string
}

func (f *fakeDaisyClient) TriggerTrainingAsync(
	ctx context.Context,
	task map[string]any,
	callbackURL string,
	tidOverride string,
) (string, error) {
	f.triggerCalls++
	f.lastCtx = ctx
	f.lastCallbackURL = callbackURL
	f.lastModelTask = task
	if tidOverride != "" {
		return tidOverride, nil
	}
	return f.taskID, nil
}

func (f *fakeDaisyClient) UploadData(context.Context, string, string, []json.RawMessage) error {
	return nil
}

func (f *fakeDaisyClient) HTTPClient() *http.Client { return &http.Client{} }

// TestHandleTrainingComplete_UnknownTaskId verifies that an unknown taskId is a no-op.
func TestHandleTrainingComplete_UnknownTaskId(t *testing.T) {
	m := &MtlfService{}
	// Should not panic
	m.HandleTrainingComplete("no-such-id", "model.npy", "success", "")
}

// TestHandleTrainingComplete_Failure_ClearsRetraining verifies that a failed
// training callback clears the store's retraining flag and removes the entry.
func TestHandleTrainingComplete_Failure_ClearsRetraining(t *testing.T) {
	m := &MtlfService{}
	store := nwdaf_context.NewModelAccuracyStore("test://old-model")
	store.SetRetraining(true)

	const taskId = "task-fail-001"
	m.inFlight.Store(taskId, &inFlightEntry{
		oldModelUrl: "old.npy",
		store:       store,
	})

	m.HandleTrainingComplete(taskId, "", "failure", "training error")

	if store.IsRetraining() {
		t.Error("IsRetraining should be cleared after training failure")
	}
	if _, ok := m.inFlight.Load(taskId); ok {
		t.Error("inFlight entry should be removed after HandleTrainingComplete")
	}
}

// TestHandleTrainingComplete_Failure_NilStore verifies that a nil store is handled safely.
func TestHandleTrainingComplete_Failure_NilStore(t *testing.T) {
	m := &MtlfService{}

	const taskId = "task-fail-002"
	m.inFlight.Store(taskId, &inFlightEntry{
		oldModelUrl: "old.npy",
		store:       nil,
	})

	// Should not panic even with nil store
	m.HandleTrainingComplete(taskId, "", "failure", "some error")

	if _, ok := m.inFlight.Load(taskId); ok {
		t.Error("inFlight entry should be removed after HandleTrainingComplete")
	}
}

// TestHandleTrainingComplete_Success_ClearsInFlight verifies that a successful
// callback removes the in-flight entry. swapModelAfterRetrain will return early
// because no app config is provided in this unit test.
func TestHandleTrainingComplete_Success_ClearsInFlight(t *testing.T) {
	m := &MtlfService{}

	const taskId = "task-ok-001"
	m.inFlight.Store(taskId, &inFlightEntry{
		oldModelUrl: "old.npy",
		store:       nil,
	})

	m.HandleTrainingComplete(taskId, "new.npy", "success", "")

	if _, ok := m.inFlight.Load(taskId); ok {
		t.Error("inFlight entry should be removed on success")
	}
}

// TestHandleTrainingComplete_DuplicateCallback verifies that a second callback
// for the same taskId is silently ignored (already deleted by LoadAndDelete).
func TestHandleTrainingComplete_DuplicateCallback(t *testing.T) {
	m := &MtlfService{}
	store := nwdaf_context.NewModelAccuracyStore("test://dup")
	store.SetRetraining(true)

	const taskId = "task-dup-001"
	m.inFlight.Store(taskId, &inFlightEntry{
		oldModelUrl: "old.npy",
		store:       store,
	})

	m.HandleTrainingComplete(taskId, "", "failure", "err")
	// Second call with same taskId — should be a no-op
	m.HandleTrainingComplete(taskId, "", "failure", "err")
}

func TestSwapModelAfterRetrain_DeletesOldMonitorState(t *testing.T) {
	cfg := &factory.Config{
		Configuration: &factory.Configuration{},
	}

	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()

	oldModelURL := "file:///old-model.pth"
	newModelURL := "file:///new-model.pth"

	oldShared, _ := ctx.GetOrCreateSharedModel(oldModelURL, models.NwdafEvent_UE_COMMUNICATION)
	oldShared.SetModelId("old-model-id")

	m := newTestMtlfService(cfg)
	m.onModelSwapReady = func(newModelUrl, oldModelId string) (string, error) {
		if newModelUrl != newModelURL {
			t.Fatalf("newModelUrl = %s, want %s", newModelUrl, newModelURL)
		}
		if oldModelId != "old-model-id" {
			t.Fatalf("oldModelId = %s, want old-model-id", oldModelId)
		}
		return "new-model-id", nil
	}
	m.onModelSwapped = func(modelUrl string, wg *sync.WaitGroup) {}

	scope := m.stateStore.GetOrCreateScope(oldModelURL, "group:test", 3, 3)
	scope.RecordObservation(ScopeObservation{
		Timestamp:    time.Now(),
		SampleCount:  5,
		TrafficScale: 2048,
		Metrics: map[string]float64{
			"MAE": 100,
		},
	})

	m.swapModelAfterRetrain(oldModelURL, newModelURL)

	if m.stateStore.ModelExists(oldModelURL) {
		t.Fatal("old model state should be deleted after successful swap")
	}
	if ctx.GetSharedModel(oldModelURL) != nil {
		t.Fatal("old shared model should be deleted after successful swap")
	}
	if ctx.GetSharedModel(newModelURL) == nil {
		t.Fatal("new shared model should be created after successful swap")
	}
}

func TestSubmitDaisyTaskUsesInjectedClient(t *testing.T) {
	cfg := &factory.Config{
		Configuration: &factory.Configuration{
			Sbi: &factory.Sbi{
				Scheme:       "http",
				RegisterIPv4: "127.0.0.1",
				Port:         8000,
			},
			Mtlf: &factory.MtlfConfig{
				Enabled:        true,
				Endpoint:       "http://daisy.example",
				StaticModelUrl: "file:///old-model.onnx",
				Task: map[string]any{
					"NUM_ROUNDS": 2,
				},
			},
		},
	}

	client := &fakeDaisyClient{taskID: "task-123"}
	service := NewMtlfService(testNwdafApp{
		ctx: context.Background(),
		cfg: cfg,
	}, client, nil)

	service.submitDaisyTask(cfg.Configuration.Mtlf, "", cfg.Configuration.Mtlf.StaticModelUrl, nil)

	if client.triggerCalls != 1 {
		t.Fatalf("TriggerTrainingAsync called %d times, want 1", client.triggerCalls)
	}
	if client.lastCtx == nil {
		t.Fatal("TriggerTrainingAsync should receive a parent context")
	}
	if client.lastCallbackURL != "http://127.0.0.1:8000/mtlf/training-complete" {
		t.Fatalf("callbackURL = %q, want %q",
			client.lastCallbackURL, "http://127.0.0.1:8000/mtlf/training-complete")
	}
	if _, ok := client.lastModelTask["NUM_ROUNDS"]; !ok {
		t.Fatal("expected task payload to be forwarded to Daisy client")
	}
	if _, ok := service.inFlight.Load("task-123"); !ok {
		t.Fatal("expected inFlight entry to be stored after successful Daisy submission")
	}
}
