// Package anlf implements the Analytics Logical Function (AnLF) of NWDAF.
// Per TS 23.288: AnLF handles analytics requests, executes the inference pipeline,
// measures prediction accuracy, and reports accuracy information to MTLF.
package anlf

import (
	"context"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

// NwdafApp defines the app-level dependencies needed by AnLF.
type NwdafApp interface {
	CancelContext() context.Context
}

// AnlfService is the AnLF entry point.
type AnlfService struct {
	nwdaf             NwdafApp
	onDeviationReport func(modelUrl string, deviation float64, store *nwdaf_context.ModelAccuracyStore)
	onAccuracyReports func(modelUrl string, reports []AccuracyReport, store *nwdaf_context.ModelAccuracyStore)
}

// AccuracyReport is the internal AnLF output for one monitor round and one scope.
// MTLF consumes these per-scope metrics for retrain policy evaluation and
// CSV/log observability.
type AccuracyReport struct {
	ModelURL     string
	ScopeKey     string
	NwdafSubID   string
	Metrics      map[string]float64
	SampleCount  int
	InferenceNum int
	WindowStart  time.Time
	WindowEnd    time.Time
}

// NewAnlfService creates a new AnlfService instance.
func NewAnlfService(nwdaf NwdafApp) *AnlfService {
	return &AnlfService{nwdaf: nwdaf}
}

// SetOnDeviationReport registers the callback invoked when AnLF finishes computing
// model-level deviation for a model. This callback is kept for legacy
// compatibility while the report-based MTLF policy path is active.
func (a *AnlfService) SetOnDeviationReport(
	fn func(modelUrl string, deviation float64, store *nwdaf_context.ModelAccuracyStore),
) {
	a.onDeviationReport = fn
}

// SetOnAccuracyReports registers the callback invoked when AnLF finishes
// computing per-scope accuracy metrics for one monitor round.
func (a *AnlfService) SetOnAccuracyReports(
	fn func(modelUrl string, reports []AccuracyReport, store *nwdaf_context.ModelAccuracyStore),
) {
	a.onAccuracyReports = fn
}
