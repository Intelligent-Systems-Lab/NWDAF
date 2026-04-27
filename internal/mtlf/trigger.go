package mtlf

import (
	"fmt"
	"math"
	"time"

	"github.com/free5gc/nwdaf/internal/anlf"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
)

const trafficScaleMetricName = "__traffic_scale__"

func signalState(baselineReady bool, signal bool) string {
	if !baselineReady {
		return "skipped"
	}
	return fmt.Sprintf("%t", signal)
}

// HandleAccuracyReports receives per-scope accuracy information from AnLF and
// decides whether to trigger model retraining.
// Per TS 23.288 §6.2E: MTLF analyzes accuracy degradation reported by AnLF and
// determines whether retraining is necessary.
func (m *MtlfService) HandleAccuracyReports(
	modelUrl string,
	reports []anlf.AccuracyReport,
	store *nwdaf_context.ModelAccuracyStore,
) {
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil ||
		cfg.Configuration.Mtlf == nil ||
		cfg.Configuration.Mtlf.AccuracyMonitor == nil ||
		len(reports) == 0 {
		return
	}
	accCfg := cfg.Configuration.Mtlf.AccuracyMonitor

	// Skip evaluation if a retrain is already in flight for this model.
	if store.IsRetraining() {
		mtlfLog.Debugf("Retraining in progress, skipping accuracy reports: model=%s", modelUrl)
		return
	}

	if m.stateStore == nil {
		m.stateStore = NewMonitorStateStore()
	}

	now := time.Now()
	m.stateStore.GCExpiredScopes(now, time.Duration(accCfg.ScopeStateTTLOrDefault())*time.Second)

	primaryMetric := accCfg.PrimaryMetricOrDefault()
	bufferSize := accCfg.RecentBufferSizeOrDefault()
	minBufferSamples := accCfg.MinBufferSamplesOrDefault()
	minStd := accCfg.MinStdOrDefault()
	fixedFloor := accCfg.FixedFloorOrDefault()
	zThreshold := accCfg.ZScoreThresholdOrDefault()
	windowSize := accCfg.DecisionWindowSizeOrDefault()
	requiredHits := accCfg.RequiredHitsInWindowOrDefault()
	chronicCfg := accCfg.ChronicPolicy

	for _, report := range reports {
		if report.ScopeKey == "" {
			mtlfLog.Warnf("Skipping accuracy report with empty scope: model=%s", modelUrl)
			continue
		}

		current, ok := report.Metrics[primaryMetric]
		if !ok {
			mtlfLog.Warnf("Primary metric missing in accuracy report: model=%s scope=%s metric=%s",
				modelUrl, report.ScopeKey, primaryMetric)
			continue
		}

		scopeState := m.stateStore.GetOrCreateScope(modelUrl, report.ScopeKey, bufferSize, windowSize)
		historyCount := scopeState.SampleCount(primaryMetric)
		mean := scopeState.Mean(primaryMetric)
		std := scopeState.Std(primaryMetric)
		baselineReady := historyCount >= minBufferSamples

		for metric, value := range report.Metrics {
			scopeState.RecordMetric(metric, value, now)
		}
		if report.TrafficScale > 0 {
			scopeState.RecordMetric(trafficScaleMetricName, report.TrafficScale, now)
		}

		degradationEligible := current > fixedFloor
		degradationSignal := false
		zscore := 0.0
		if historyCount > 0 {
			zscore = (current - mean) / math.Max(std, minStd)
			degradationSignal = zscore > zThreshold
		}
		degradationSignalState := signalState(baselineReady, degradationSignal)

		degradationHit := baselineReady && degradationEligible && degradationSignal
		degradationHits := 0
		if baselineReady {
			degradationHits = scopeState.RecordDegradationOutcome(degradationHit)
		} else {
			scopeState.ResetDecisionWindows()
		}

		chronicEnabled := chronicCfg != nil && chronicCfg.EnabledOrDefault()
		chronicEligible := false
		chronicSignal := false
		chronicHit := false
		chronicValue := 0.0
		trafficScale := scopeState.Mean(trafficScaleMetricName)
		chronicHits := 0
		chronicMetricCount := 0
		if chronicEnabled {
			chronicMetricCount = scopeState.SampleCount(chronicCfg.MetricOrDefault())
			chronicEligible = trafficScale >= chronicCfg.MinTrafficScaleOrDefault()
			if chronicMetricCount > 0 {
				switch chronicCfg.AggregatorOrDefault() {
				case "mean":
					chronicValue = scopeState.Mean(chronicCfg.MetricOrDefault())
				default:
					chronicValue = scopeState.Percentile(
						chronicCfg.MetricOrDefault(),
						chronicCfg.PercentileOrDefault(),
					)
				}
			}
			chronicSignal = chronicValue > chronicCfg.ThresholdOrDefault()
		}
		chronicSignalState := signalState(baselineReady, chronicSignal)
		if chronicEnabled && baselineReady {
			chronicHit = chronicEligible && chronicSignal
			chronicHits = scopeState.RecordChronicOutcome(chronicHit)
		}

		hitReason := "none"
		switch {
		case degradationHit && chronicHit:
			hitReason = "both"
		case degradationHit:
			hitReason = "degradation"
		case chronicHit:
			hitReason = "chronic"
		}

		mtlfLog.Infof(
			"Accuracy policy [%s]: scope=%s metric=%s current=%.4f mean=%.4f std=%.4f "+
				"zscore=%.4f degradationEligible=%t degradationSignal=%s baselineReady=%t trafficScale=%.4f "+
				"chronicEligible=%t chronicSignal=%s chronicValue=%.4f degradationHits=%d/%d chronicHits=%d/%d hitReason=%s",
			modelUrl,
			report.ScopeKey,
			primaryMetric,
			current,
			mean,
			std,
			zscore,
			degradationEligible,
			degradationSignalState,
			baselineReady,
			trafficScale,
			chronicEligible,
			chronicSignalState,
			chronicValue,
			degradationHits,
			requiredHits,
			chronicHits,
			requiredHits,
			hitReason,
		)

		if degradationHits >= requiredHits || chronicHits >= requiredHits {
			mtlfLog.Warnf(
				"Retrain trigger [%s]: scope=%s metric=%s current=%.4f degradationHits=%d/%d chronicHits=%d/%d reason=%s",
				modelUrl,
				report.ScopeKey,
				primaryMetric,
				current,
				degradationHits, requiredHits,
				chronicHits, requiredHits,
				hitReason,
			)
			m.stateStore.ResetModelBreaches(modelUrl)
			store.SetRetraining(true)
			m.dispatchRetrain(modelUrl, store)
			return
		}
	}
}

func (m *MtlfService) dispatchRetrain(modelUrl string, store *nwdaf_context.ModelAccuracyStore) {
	if m.onRetrainTriggered != nil {
		m.onRetrainTriggered(modelUrl, store)
		return
	}
	m.startRetrainWorkflow(modelUrl, store)
}
