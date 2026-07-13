package mtlf

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	anlfserver "github.com/free5gc/nwdaf/internal/anlf"
	"github.com/free5gc/nwdaf/internal/anlf/contract"
	anlfprocessor "github.com/free5gc/nwdaf/internal/anlf/processor"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
)

func backendAccuracyReport(id string, generation int64) *contract.ModelAccuracyReport {
	return &contract.ModelAccuracyReport{
		ReportID:          id,
		ModelIdentity:     contract.ModelIdentity{ProviderID: "mtlf-a", ModelUniqueID: 42},
		Generation:        generation,
		MonitoringContext: contract.MonitoringContext{AnalyticsEvent: "UE_COMMUNICATION", ScopeID: "scope-a"},
		AccuracyInformation: contract.AccuracyInformation{
			Metrics: map[string]float64{"WAPE": 0.1}, SampleCount: 2,
			WindowStart: time.Now(), WindowEnd: time.Now(),
		},
	}
}

func TestHandleModelAccuracyReportDeduplicatesAndRejectsStaleGeneration(t *testing.T) {
	nwdaf_context.Init()
	service := &MtlfService{stateStore: NewMonitorStateStore()}

	if err := service.HandleModelAccuracyReport(backendAccuracyReport("report-1", 2)); err != nil {
		t.Fatalf("first report: %v", err)
	}
	if err := service.HandleModelAccuracyReport(backendAccuracyReport("report-1", 2)); err != nil {
		t.Fatalf("duplicate report: %v", err)
	}
	err := service.HandleModelAccuracyReport(backendAccuracyReport("report-old", 1))
	if err != contract.ErrStaleModelGeneration {
		t.Fatalf("stale report error = %v", err)
	}
}

func TestPyAnLFAccuracyJSONReachesMtlfWithoutUnitConversion(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve AnLF port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err = listener.Close(); err != nil {
		t.Fatalf("release AnLF port: %v", err)
	}

	cfg := &factory.Config{Configuration: &factory.Configuration{
		Anlf: &factory.AnlfConfig{Server: &factory.AuxiliaryServerConfig{
			BindingIPv4: "127.0.0.1",
			Port:        port,
		}},
		Mtlf: &factory.MtlfConfig{AccuracyPolicy: &factory.AccuracyMonitorConfig{
			PrimaryMetric:    "sMAPE",
			MinBufferSamples: 8,
			DegradationPolicy: &factory.DegradationPolicyConfig{
				MinDecisionTrafficScale: 1,
			},
		}},
	}}
	service := newTestMtlfService(cfg)
	processor := anlfprocessor.NewProcessor(nil)
	processor.SetModelAccuracyWorkflow(service)
	server, err := anlfserver.NewServer(cfg, processor)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	var serverWG sync.WaitGroup
	if err = server.Run(&serverWG); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	defer func() {
		server.Shutdown()
		serverWG.Wait()
	}()

	// This is the canonical payload asserted by PyAnLF's accuracy serialization test.
	payload := []byte(`{
		"report_id":"report-stable","report_sequence":1,"generated_at":"2026-07-14T01:02:03Z",
		"model_identity":{"provider_id":"provider","model_unique_id":1},"generation":1,
		"monitoring_context":{"analytics_event":"UE_COMMUNICATION","target_ue":{},"event_filter":{},"scope_id":"scope"},
		"accuracy_information":{"metrics":{"sMAPE":0.1},"deviation":0.1,"sample_count":1,"inference_count":1,
		"actual_traffic_scale":10.0,"predicted_traffic_scale":11.0,
		"window_start":"2026-07-14T01:02:03Z","window_end":"2026-07-14T01:02:03Z"},
		"retrain_context":{"subscription_ids":["sub"],"observation_source_ids":["source"]}
	}`)
	requestCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodPost,
		"http://127.0.0.1:"+strconv.Itoa(port)+"/model-accuracy-reports",
		bytes.NewReader(payload),
	)
	if err != nil {
		t.Fatalf("create accuracy report request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("POST accuracy report: %v", err)
	}
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Errorf("close accuracy report response: %v", closeErr)
		}
	}()
	if response.StatusCode != http.StatusNoContent {
		body, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			t.Fatalf("read accuracy report response: %v", readErr)
		}
		t.Fatalf("status = %d, body = %s", response.StatusCode, body)
	}

	modelIdentity := contract.ModelIdentity{ProviderID: "provider", ModelUniqueID: 1}
	storedValue, ok := service.accuracyReportContexts.Load(modelIdentity.Key())
	if !ok {
		t.Fatal("MTLF did not retain the PyAnLF report context")
	}
	stored := storedValue.(contract.ModelAccuracyReport)
	if stored.ReportSequence != 1 || stored.Generation != 1 ||
		stored.MonitoringContext.AnalyticsEvent != "UE_COMMUNICATION" ||
		stored.MonitoringContext.ScopeID != "scope" ||
		stored.AccuracyInformation.Deviation != 0.1 ||
		stored.AccuracyInformation.SampleCount != 1 ||
		stored.AccuracyInformation.InferenceCount != 1 ||
		stored.AccuracyInformation.ActualTrafficScale != 10 ||
		stored.AccuracyInformation.PredictedTrafficScale != 11 ||
		stored.AccuracyInformation.Metrics["sMAPE"] != 0.1 {
		t.Fatalf("stored report changed contract values: %+v", stored)
	}
	if !reflect.DeepEqual(stored.RetrainContext.SubscriptionIDs, []string{"sub"}) ||
		!reflect.DeepEqual(stored.RetrainContext.ObservationSourceIDs, []string{"source"}) {
		t.Fatalf("stored retrain context = %+v", stored.RetrainContext)
	}

	scope := service.stateStore.GetOrCreateScope(modelIdentity.Key(), "scope", 20, 5)
	observations := scope.recentObservations.Snapshot()
	if len(observations) != 1 {
		t.Fatalf("MTLF observations = %d, want 1", len(observations))
	}
	observation := observations[0]
	if observation.SampleCount != 1 || observation.TrafficScale != 10 ||
		observation.PredictedTrafficScale != 11 || observation.Metrics["sMAPE"] != 0.1 {
		t.Fatalf("MTLF policy observation changed contract units: %+v", observation)
	}
}
