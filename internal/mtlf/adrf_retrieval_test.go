package mtlf

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
)

type fakeAdrfClient struct {
	unsubscribeCalls int
	ctxErrs          []error
}

func (f *fakeAdrfClient) StorageRequest(
	context.Context,
	*nwdaf_context.AdrfSmfInfo,
	[]json.RawMessage,
) (string, error) {
	return "", nil
}

func (f *fakeAdrfClient) RetrievalSubscribe(
	context.Context,
	*nwdaf_context.AdrfSmfInfo,
	string,
	string,
	consumer.AdrfTimePeriod,
) (string, error) {
	return "", nil
}

func (f *fakeAdrfClient) RetrievalRequest(context.Context, []string) (*consumer.NadrfDataStoreRecord, error) {
	return nil, nil
}

func (f *fakeAdrfClient) RetrievalUnsubscribe(ctx context.Context, subscriptionID string) error {
	f.unsubscribeCalls++
	if subscriptionID == "" {
		return nil
	}
	f.ctxErrs = append(f.ctxErrs, ctx.Err())
	return nil
}

func (f *fakeAdrfClient) HTTPClient() *http.Client {
	return &http.Client{}
}

func TestCleanupSubscriptionsUsesDedicatedCleanupContext(t *testing.T) {
	job := &retrainJob{
		subscriptionIds: []string{"sub-1", "sub-2"},
	}
	client := &fakeAdrfClient{}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if canceledCtx.Err() == nil {
		t.Fatal("expected test app context to be canceled")
	}

	cleanupSubscriptions(job, client)

	if client.unsubscribeCalls != 2 {
		t.Fatalf("RetrievalUnsubscribe called %d times, want 2", client.unsubscribeCalls)
	}
	for i, err := range client.ctxErrs {
		if err != nil {
			t.Fatalf("cleanup context %d should be active during unsubscribe, got err=%v", i, err)
		}
	}
}

func TestRunFetchLoopStopsOnShutdownWithoutWaitingForWatchdog(t *testing.T) {
	cfg := &factory.Config{
		Configuration: &factory.Configuration{
			Mtlf: &factory.MtlfConfig{
				Enabled: true,
				Task:    map[string]any{},
			},
			Adrf: &factory.AdrfConfig{
				WatchdogTimeout: 120,
			},
		},
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	client := &fakeAdrfClient{}
	daisy := &fakeDaisyClient{taskID: "task-ignored"}
	service := NewMtlfService(testNwdafApp{
		ctx: cancelCtx,
		cfg: cfg,
	}, daisy, client)

	job := &retrainJob{
		tid:             "tid-shutdown",
		oldModelUrl:     "file:///old-model.onnx",
		subscriptionIds: []string{"sub-1"},
		fetchCh:         make(chan []string, 1),
		watchdog:        time.NewTimer(time.Hour),
	}
	defer job.watchdog.Stop()

	done := make(chan struct{})
	go func() {
		service.runFetchLoop(job, cfg.Configuration.Adrf, cfg.Configuration.Mtlf, client)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("runFetchLoop did not stop promptly after shutdown")
	}

	if client.unsubscribeCalls != 1 {
		t.Fatalf("RetrievalUnsubscribe called %d times, want 1", client.unsubscribeCalls)
	}
	if daisy.triggerCalls != 0 {
		t.Fatalf("TriggerTrainingAsync called %d times, want 0 during shutdown", daisy.triggerCalls)
	}
}
