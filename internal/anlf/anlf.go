// Package anlf implements the Analytics Logical Function (AnLF) of NWDAF.
// Per TS 23.288: AnLF handles analytics requests, executes the inference pipeline,
// measures prediction accuracy, and reports accuracy information to MTLF.
package anlf

import (
	"context"
	"sync"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/app"
	"github.com/free5gc/nwdaf/pkg/factory"
)

var anlfLog = logger.AnlfLog

// NwdafApp defines the app-level dependencies needed by AnLF.
type NwdafApp interface {
	app.App
	CancelContext() context.Context
}

// AnlfService is the AnLF entry point.
type AnlfService struct {
	nwdaf               NwdafApp
	anlfBackend         AnlfBackendAPI
	onDeviationReport   func(modelUrl string, deviation float64, store *nwdaf_context.ModelAccuracyStore)
	onAccuracyReports   func(modelUrl string, reports []AccuracyReport, store *nwdaf_context.ModelAccuracyStore)
	warmupMu            sync.Mutex
	startupWarmupDone   bool
	wg                  *sync.WaitGroup
	observationDelivery *ObservationDelivery
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
	service := &AnlfService{
		nwdaf:       nwdaf,
		anlfBackend: anlfBackend,
	}
	var deliveryConfig *factory.ObservationDeliveryConfig
	if cfg := nwdaf.Config(); cfg != nil && cfg.Configuration != nil && cfg.Configuration.AnlfBackend != nil {
		deliveryConfig = cfg.Configuration.AnlfBackend.ObservationDelivery
	}
	service.observationDelivery = NewObservationDelivery(nwdaf.CancelContext(), anlfBackend, deliveryConfig)
	return service
}

func (a *AnlfService) config() *factory.Config {
	if a == nil || a.nwdaf == nil {
		return nil
	}
	return a.nwdaf.Config()
}

func activeSamplingInterval(subscriptionID string) int {
	ctx := nwdaf_context.GetSelf()
	if ctx != nil {
		if subscription := ctx.GetSubscription(subscriptionID); subscription != nil {
			_, requirements, _, _ := subscription.RuntimeSnapshot()
			if requirements.SamplingIntervalSeconds > 0 {
				return requirements.SamplingIntervalSeconds
			}
		}
	}
	return 10
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

func (a *AnlfService) StartObservationDelivery() {
	a.observationDelivery.Start()
}

func (a *AnlfService) StopObservationDelivery() {
	a.observationDelivery.Stop()
}

func (a *AnlfService) EnqueueObservations(sourceID string, observations []SourceObservation) bool {
	return a.observationDelivery.Enqueue(sourceID, observations)
}

func (a *AnlfService) BuildProvisionNotificationURI() string {
	cfg := a.config()
	if cfg == nil {
		return ""
	}
	return cfg.GetAnlfServerURI() + "/mlmodel-notify"
}

func (a *AnlfService) BuildAnalyticsReportCallbackURI(subscriptionID string) string {
	cfg := a.config()
	if cfg == nil {
		return ""
	}
	return cfg.GetAnlfServerURI() + "/subscriptions/" + subscriptionID + "/analytics-reports"
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
