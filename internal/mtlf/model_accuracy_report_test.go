package mtlf

import (
	"testing"
	"time"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
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
