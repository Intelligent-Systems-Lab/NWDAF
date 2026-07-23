package mtlf

import (
	"context"
	"net"
	"net/http"
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

func TestLegacyAccuracyRouteIsNotRegistered(t *testing.T) {
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
	}}
	processor := anlfprocessor.NewProcessor(nil)
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

	requestCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodPost,
		"http://127.0.0.1:"+strconv.Itoa(port)+"/model-accuracy-reports",
		nil,
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
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("legacy accuracy route status = %d, want 404", response.StatusCode)
	}
}
