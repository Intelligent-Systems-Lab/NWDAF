package factory_test

import (
	"testing"

	"github.com/free5gc/nwdaf/pkg/factory"
)

// =============================================================================
// ModelParams Helper Method Tests
// =============================================================================

func TestModelParams_Defaults(t *testing.T) {
	// Zero-value ModelParams should return defaults from all helpers
	p := &factory.ModelParams{}

	if got := p.SamplingIntervalOrDefault(); got != 10 {
		t.Errorf("SamplingIntervalOrDefault() = %d, want 10", got)
	}
	if got := p.InputWindowOrDefault(); got != 30 {
		t.Errorf("InputWindowOrDefault() = %d, want 30", got)
	}
	if got := p.OutputWindowOrDefault(); got != 5 {
		t.Errorf("OutputWindowOrDefault() = %d, want 5", got)
	}
}

func TestModelParams_ExplicitValues(t *testing.T) {
	p := &factory.ModelParams{
		SamplingInterval: 5,
		InputWindow:      20,
		OutputWindow:     3,
	}

	if got := p.SamplingIntervalOrDefault(); got != 5 {
		t.Errorf("SamplingIntervalOrDefault() = %d, want 5", got)
	}
	if got := p.InputWindowOrDefault(); got != 20 {
		t.Errorf("InputWindowOrDefault() = %d, want 20", got)
	}
	if got := p.OutputWindowOrDefault(); got != 3 {
		t.Errorf("OutputWindowOrDefault() = %d, want 3", got)
	}
}

func TestModelParams_QueryLookback_Defaults(t *testing.T) {
	// Zero-value: should use default si=10, iw=30 → lookback = 300s
	p := &factory.ModelParams{}
	if got := p.QueryLookback(); got != 300 {
		t.Errorf("QueryLookback() = %d, want 300 (10s × 30pts)", got)
	}
}

func TestModelParams_QueryLookback_Custom(t *testing.T) {
	p := &factory.ModelParams{
		SamplingInterval: 5,
		InputWindow:      20,
	}
	// 5s × 20pts = 100s
	if got := p.QueryLookback(); got != 100 {
		t.Errorf("QueryLookback() = %d, want 100 (5s × 20pts)", got)
	}
}

func TestModelParams_QueryLookback_ZeroInterval(t *testing.T) {
	// SamplingInterval = 0 → fallback to 10
	p := &factory.ModelParams{InputWindow: 15}
	// 10s × 15pts = 150s
	if got := p.QueryLookback(); got != 150 {
		t.Errorf("QueryLookback() = %d, want 150 (10s fallback × 15pts)", got)
	}
}

func TestModelParams_QueryLookback_ZeroInputWindow(t *testing.T) {
	// InputWindow = 0 → fallback to 30
	p := &factory.ModelParams{SamplingInterval: 20}
	// 20s × 30pts = 600s
	if got := p.QueryLookback(); got != 600 {
		t.Errorf("QueryLookback() = %d, want 600 (20s × 30 fallback)", got)
	}
}

func TestModelParams_NegativeValues_UseDefaults(t *testing.T) {
	// Negative values should fall through to defaults
	p := &factory.ModelParams{
		SamplingInterval: -1,
		InputWindow:      -5,
		OutputWindow:     -3,
	}
	if got := p.SamplingIntervalOrDefault(); got != 10 {
		t.Errorf("SamplingIntervalOrDefault() = %d, want 10 for negative input", got)
	}
	if got := p.InputWindowOrDefault(); got != 30 {
		t.Errorf("InputWindowOrDefault() = %d, want 30 for negative input", got)
	}
	if got := p.OutputWindowOrDefault(); got != 5 {
		t.Errorf("OutputWindowOrDefault() = %d, want 5 for negative input", got)
	}
}

// =============================================================================
// AnalyticsConfig nil safety
// =============================================================================

func TestAnalyticsConfig_NilUeCommunication(t *testing.T) {
	// AnalyticsConfig with nil UeCommunication should not panic
	cfg := &factory.AnalyticsConfig{
		UeCommunication: nil,
	}
	if cfg.UeCommunication != nil {
		t.Error("UeCommunication should be nil")
	}
}

func TestAccuracyMonitorConfig_PolicyDefaults(t *testing.T) {
	cfg := &factory.AccuracyMonitorConfig{}

	if got := cfg.PrimaryMetricOrDefault(); got != "MAE" {
		t.Errorf("PrimaryMetricOrDefault() = %q, want %q", got, "MAE")
	}
	if got := cfg.RecentBufferSizeOrDefault(); got != 20 {
		t.Errorf("RecentBufferSizeOrDefault() = %d, want 20", got)
	}
	if got := cfg.MinBufferSamplesOrDefault(); got != 8 {
		t.Errorf("MinBufferSamplesOrDefault() = %d, want 8", got)
	}
	if got := cfg.MinStdOrDefault(); got != 0.01 {
		t.Errorf("MinStdOrDefault() = %.2f, want 0.01", got)
	}
	if got := cfg.FixedFloorOrDefault(); got != 1024 {
		t.Errorf("FixedFloorOrDefault() = %.0f, want 1024", got)
	}
	if got := cfg.ZScoreThresholdOrDefault(); got != 3.0 {
		t.Errorf("ZScoreThresholdOrDefault() = %.1f, want 3.0", got)
	}
	if got := cfg.ScopeStateTTLOrDefault(); got != 600 {
		t.Errorf("ScopeStateTTLOrDefault() = %d, want 600", got)
	}
	if got := cfg.DecisionWindowSizeOrDefault(); got != 3 {
		t.Errorf("DecisionWindowSizeOrDefault() = %d, want 3", got)
	}
	if got := cfg.RequiredHitsInWindowOrDefault(); got != 3 {
		t.Errorf("RequiredHitsInWindowOrDefault() = %d, want 3", got)
	}
	if got := cfg.ConsecutiveBreachesOrDefault(); got != 3 {
		t.Errorf("ConsecutiveBreachesOrDefault() = %d, want 3", got)
	}
	if got := cfg.MetricsToRecordOrDefault(); len(got) != 5 {
		t.Errorf("MetricsToRecordOrDefault() length = %d, want 5", len(got))
	}
	if cfg.ChronicPolicy.EnabledOrDefault() {
		t.Error("ChronicPolicy.EnabledOrDefault() = true, want false")
	}
	if got := cfg.ChronicPolicy.MetricOrDefault(); got != "WAPE" {
		t.Errorf("ChronicPolicy.MetricOrDefault() = %q, want %q", got, "WAPE")
	}
	if got := cfg.ChronicPolicy.AggregatorOrDefault(); got != "percentile" {
		t.Errorf("ChronicPolicy.AggregatorOrDefault() = %q, want %q", got, "percentile")
	}
	if got := cfg.ChronicPolicy.PercentileOrDefault(); got != 75 {
		t.Errorf("ChronicPolicy.PercentileOrDefault() = %d, want 75", got)
	}
}

func TestAccuracyMonitorConfig_PolicyExplicitValues(t *testing.T) {
	enabled := true
	cfg := &factory.AccuracyMonitorConfig{
		MetricsToRecord:      []string{"MAE", "WAPE"},
		PrimaryMetric:        "WAPE",
		RecentBufferSize:     12,
		MinBufferSamples:     4,
		MinStd:               0.5,
		FixedFloor:           2048,
		ZScoreThreshold:      2.5,
		DecisionWindowSize:   6,
		RequiredHitsInWindow: 4,
		ScopeStateTTL:        120,
		ConsecutiveBreaches:  5,
		ChronicPolicy: &factory.ChronicPolicyConfig{
			Enabled:         &enabled,
			Metric:          "nrmse",
			Aggregator:      "mean",
			Percentile:      50,
			Threshold:       1.5,
			MinTrafficScale: 4096,
		},
	}

	if got := cfg.PrimaryMetricOrDefault(); got != "WAPE" {
		t.Errorf("PrimaryMetricOrDefault() = %q, want %q", got, "WAPE")
	}
	if got := cfg.RecentBufferSizeOrDefault(); got != 12 {
		t.Errorf("RecentBufferSizeOrDefault() = %d, want 12", got)
	}
	if got := cfg.MinBufferSamplesOrDefault(); got != 4 {
		t.Errorf("MinBufferSamplesOrDefault() = %d, want 4", got)
	}
	if got := cfg.MinStdOrDefault(); got != 0.5 {
		t.Errorf("MinStdOrDefault() = %.1f, want 0.5", got)
	}
	if got := cfg.FixedFloorOrDefault(); got != 2048 {
		t.Errorf("FixedFloorOrDefault() = %.0f, want 2048", got)
	}
	if got := cfg.ZScoreThresholdOrDefault(); got != 2.5 {
		t.Errorf("ZScoreThresholdOrDefault() = %.1f, want 2.5", got)
	}
	if got := cfg.ScopeStateTTLOrDefault(); got != 120 {
		t.Errorf("ScopeStateTTLOrDefault() = %d, want 120", got)
	}
	if got := cfg.DecisionWindowSizeOrDefault(); got != 6 {
		t.Errorf("DecisionWindowSizeOrDefault() = %d, want 6", got)
	}
	if got := cfg.RequiredHitsInWindowOrDefault(); got != 4 {
		t.Errorf("RequiredHitsInWindowOrDefault() = %d, want 4", got)
	}
	if got := cfg.ConsecutiveBreachesOrDefault(); got != 5 {
		t.Errorf("ConsecutiveBreachesOrDefault() = %d, want 5", got)
	}
	gotMetrics := cfg.MetricsToRecordOrDefault()
	if len(gotMetrics) != 2 || gotMetrics[0] != "MAE" || gotMetrics[1] != "WAPE" {
		t.Errorf("MetricsToRecordOrDefault() = %v, want [MAE WAPE]", gotMetrics)
	}
	if !cfg.ChronicPolicy.EnabledOrDefault() {
		t.Error("ChronicPolicy.EnabledOrDefault() = false, want true")
	}
	if got := cfg.ChronicPolicy.MetricOrDefault(); got != "NRMSE" {
		t.Errorf("ChronicPolicy.MetricOrDefault() = %q, want %q", got, "NRMSE")
	}
	if got := cfg.ChronicPolicy.AggregatorOrDefault(); got != "mean" {
		t.Errorf("ChronicPolicy.AggregatorOrDefault() = %q, want %q", got, "mean")
	}
	if got := cfg.ChronicPolicy.PercentileOrDefault(); got != 50 {
		t.Errorf("ChronicPolicy.PercentileOrDefault() = %d, want 50", got)
	}
	if got := cfg.ChronicPolicy.ThresholdOrDefault(); got != 1.5 {
		t.Errorf("ChronicPolicy.ThresholdOrDefault() = %.1f, want 1.5", got)
	}
	if got := cfg.ChronicPolicy.MinTrafficScaleOrDefault(); got != 4096 {
		t.Errorf("ChronicPolicy.MinTrafficScaleOrDefault() = %.0f, want 4096", got)
	}
}
