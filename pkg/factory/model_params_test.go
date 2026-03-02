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
