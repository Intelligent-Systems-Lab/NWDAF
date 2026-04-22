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
// It is currently used for observability, while retrain decision still follows
// the legacy deviation callback.
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
// accuracy for a model. Per TS 23.288 §6.2D: AnLF reports Analytics Accuracy
// Information to MTLF, which then decides whether to retrain.
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
