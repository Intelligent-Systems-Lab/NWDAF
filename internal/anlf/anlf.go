// Package anlf implements the Analytics Logical Function (AnLF) of NWDAF.
// Per TS 23.288: AnLF handles analytics requests, executes the inference pipeline,
// measures prediction accuracy, and reports accuracy information to MTLF.
package anlf

import (
	"context"
	"sync"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/app"
	"github.com/free5gc/nwdaf/pkg/factory"
)

// NwdafApp defines the app-level dependencies needed by AnLF.
type NwdafApp interface {
	app.App
	CancelContext() context.Context
}

// AnlfService is the AnLF entry point.
type AnlfService struct {
	nwdaf             NwdafApp
	anlfBackend       AnlfBackendAPI
	onDeviationReport func(modelUrl string, deviation float64, store *nwdaf_context.ModelAccuracyStore)
	onAccuracyReports func(modelUrl string, reports []AccuracyReport, store *nwdaf_context.ModelAccuracyStore)
	warmupMu          sync.Mutex
	startupWarmupDone bool
	wg                *sync.WaitGroup
}

// AccuracyReport is the internal AnLF output for one monitor round and one scope.
// MTLF consumes these per-scope metrics for retrain policy evaluation and
// CSV/log observability.
type AccuracyReport struct {
	ModelURL              string
	ScopeKey              string
	NwdafSubID            string
	Metrics               map[string]float64
	TrafficScale          float64
	PredictedTrafficScale float64
	SampleCount           int
	InferenceNum          int
	WindowStart           time.Time
	WindowEnd             time.Time
}

// NewAnlfService creates a new AnlfService instance.
func NewAnlfService(nwdaf NwdafApp, anlfBackend AnlfBackendAPI) *AnlfService {
	return &AnlfService{
		nwdaf:       nwdaf,
		anlfBackend: anlfBackend,
	}
}

func (a *AnlfService) config() *factory.Config {
	if a == nil || a.nwdaf == nil {
		return nil
	}
	return a.nwdaf.Config()
}

func ueCommunicationModelParams(cfg *factory.Config) *factory.ModelParams {
	if cfg != nil && cfg.Configuration != nil &&
		cfg.Configuration.Analytics != nil &&
		cfg.Configuration.Analytics.UeCommunication != nil {
		return cfg.Configuration.Analytics.UeCommunication
	}
	return &factory.ModelParams{}
}

func accuracyMonitorConfig(cfg *factory.Config) *factory.AccuracyMonitorConfig {
	if cfg == nil || cfg.Configuration == nil || cfg.Configuration.Mtlf == nil {
		return nil
	}
	return cfg.Configuration.Mtlf.AccuracyMonitor
}

func isAccuracyMonitorEnabled(cfg *factory.Config) bool {
	return cfg != nil && cfg.Configuration != nil &&
		cfg.Configuration.Mtlf != nil && cfg.Configuration.Mtlf.Enabled &&
		cfg.Configuration.Mtlf.AccuracyMonitor != nil && cfg.Configuration.Mtlf.AccuracyMonitor.Enabled
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

func (a *AnlfService) SetWaitGroup(wg *sync.WaitGroup) {
	a.wg = wg
}

func (a *AnlfService) launchOwnedTask(fn func()) {
	if fn == nil {
		return
	}
	if a.wg != nil {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			fn()
		}()
		return
	}
	go fn()
}

func (a *AnlfService) AnlfBackend() AnlfBackendAPI {
	return a.anlfBackend
}

func (a *AnlfService) BuildProvisionNotificationURI() string {
	cfg := a.config()
	if cfg == nil {
		return ""
	}
	return cfg.GetAnlfServerURI() + "/mlmodel-notify"
}

func (a *AnlfService) acquireStartupWarmupDuration(accCfg *factory.AccuracyMonitorConfig) int {
	a.warmupMu.Lock()
	defer a.warmupMu.Unlock()

	if a.startupWarmupDone {
		return 0
	}
	a.startupWarmupDone = true

	warmup := accCfg.WarmupDuration
	if warmup <= 0 {
		warmup = 120
	}
	return warmup
}
