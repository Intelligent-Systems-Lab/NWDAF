// Package anlf implements the Analytics Logical Function (AnLF) of NWDAF.
// Per TS 23.288: AnLF handles analytics requests, executes the inference pipeline,
// measures prediction accuracy, and reports accuracy information to MTLF.
package anlf

import (
	"context"

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
