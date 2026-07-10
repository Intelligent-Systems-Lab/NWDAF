package mtlf

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/free5gc/nwdaf/internal/anlf/accuracy"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

const (
	trafficScaleMetricName          = "__traffic_scale__"
	predictedTrafficScaleMetricName = "__predicted_traffic_scale__"
	lowTrafficOvershootEpsilonBase  = 1.0
)

func signalState(baselineReady bool, signal bool) string {
	if !baselineReady {
		return "skipped"
	}
	return fmt.Sprintf("%t", signal)
}

func composeHitReason(degradationHit, chronicHit, lowTrafficHit bool) string {
	reasons := make([]string, 0, 3)
	if degradationHit {
		reasons = append(reasons, "degradation")
	}
	if chronicHit {
		reasons = append(reasons, "chronic")
	}
	if lowTrafficHit {
		reasons = append(reasons, "low_traffic_overprediction")
	}
	if len(reasons) == 0 {
		return "none"
	}
	return strings.Join(reasons, "+")
}

// HandleAccuracyReports receives per-scope accuracy information from AnLF and
// decides whether to trigger model retraining.
// Per TS 23.288 §6.2E: MTLF analyzes accuracy degradation reported by AnLF and
// determines whether retraining is necessary.
func (m *MtlfService) HandleAccuracyReports(
	modelUrl string,
	reports []accuracy.Report,
	store *nwdaf_context.ModelAccuracyStore,
) {
	cfg := m.config()
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
	minSamples := accCfg.MinSamples
	if minSamples <= 0 {
		minSamples = 5
	}
	minStd := accCfg.MinStdOrDefault()
	fixedFloor := accCfg.FixedFloorOrDefault()
	degradationMinScale := accCfg.DegradationPolicy.MinDecisionTrafficScaleOrDefault()
	zThreshold := accCfg.ZScoreThresholdOrDefault()
	windowSize := accCfg.DecisionWindowSizeOrDefault()
	requiredHits := accCfg.RequiredHitsInWindowOrDefault()
	chronicCfg := accCfg.ChronicPolicy
	lowTrafficCfg := accCfg.LowTrafficPolicy

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
		recentHistoryCount := scopeState.SampleCount(primaryMetric)
		recentBaselineReady := recentHistoryCount >= minBufferSamples
		degradationHistoryCount := scopeState.DegradationSampleCount(primaryMetric)
		mean := scopeState.DegradationMean(primaryMetric)
		std := scopeState.DegradationStd(primaryMetric)
		degradationBaselineReady := degradationHistoryCount >= minBufferSamples

		observation := ScopeObservation{
			Timestamp:             now,
			SampleCount:           report.SampleCount,
			TrafficScale:          report.TrafficScale,
			PredictedTrafficScale: report.PredictedTrafficScale,
			Metrics:               make(map[string]float64, len(report.Metrics)),
		}
		for metric, value := range report.Metrics {
			observation.Metrics[metric] = value
		}
		scopeState.RecordObservation(observation)

		degradationTrafficEligible := report.TrafficScale >= degradationMinScale
		degradationEligible := current > fixedFloor && degradationTrafficEligible
		degradationSignal := false
		zscore := 0.0
		if degradationHistoryCount > 0 {
			zscore = (current - mean) / math.Max(std, minStd)
			degradationSignal = zscore > zThreshold
		}
		degradationSignalState := signalState(degradationBaselineReady, degradationSignal)

		degradationHit := degradationBaselineReady && degradationEligible && degradationSignal
		degradationHits := 0
		if degradationBaselineReady {
			if degradationTrafficEligible {
				degradationHits = scopeState.RecordDegradationOutcome(degradationHit)
			} else {
				degradationHits = scopeState.BreachCount()
			}
		} else {
			scopeState.ResetDegradationWindow()
		}

		chronicEnabled := chronicCfg != nil && chronicCfg.EnabledOrDefault()
		chronicEligible := false
		chronicSignal := false
		chronicHit := false
		chronicValue := 0.0
		recentTrafficScaleMean := scopeState.Mean(trafficScaleMetricName)
		chronicHits := 0
		chronicMetricCount := 0
		if chronicEnabled {
			chronicMetricCount = scopeState.SampleCount(chronicCfg.MetricOrDefault())
			chronicEligible = recentTrafficScaleMean >= chronicCfg.MinDecisionTrafficScaleOrDefault()
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
		chronicSignalState := signalState(recentBaselineReady, chronicSignal)
		if chronicEnabled && recentBaselineReady {
			chronicHit = chronicEligible && chronicSignal
			chronicHits = scopeState.RecordChronicOutcome(chronicHit)
		} else if chronicEnabled {
			scopeState.ResetChronicWindow()
		}

		lowTrafficEnabled := lowTrafficCfg != nil && lowTrafficCfg.EnabledOrDefault()
		lowTrafficEligible := false
		lowTrafficSignal := false
		lowTrafficHit := false
		lowTrafficHits := 0
		lowTrafficOvershootRatio := 0.0
		if lowTrafficEnabled {
			lowTrafficEligible = report.TrafficScale <= lowTrafficCfg.MaxActualTrafficScaleOrDefault()
			overshootBase := math.Max(report.TrafficScale, lowTrafficOvershootEpsilonBase)
			lowTrafficOvershootRatio = report.PredictedTrafficScale / overshootBase
			lowTrafficSignal = report.PredictedTrafficScale >= lowTrafficCfg.MinPredictedTrafficScaleOrDefault() &&
				report.PredictedTrafficScale >= lowTrafficCfg.PredictionOvershootRatioOrDefault()*overshootBase
		}
		lowTrafficSignalState := signalState(recentBaselineReady, lowTrafficSignal)
		if lowTrafficEnabled && recentBaselineReady {
			lowTrafficHit = lowTrafficEligible && lowTrafficSignal
			lowTrafficHits = scopeState.RecordLowTrafficOutcome(lowTrafficHit)
		} else if lowTrafficEnabled {
			scopeState.ResetLowTrafficWindow()
		}

		baselineNotFull := degradationHistoryCount < minBufferSamples
		shouldRecordDegradationReference := report.SampleCount >= minSamples &&
			degradationTrafficEligible &&
			(baselineNotFull || !degradationSignal)
		if shouldRecordDegradationReference {
			scopeState.RecordDegradationReference(observation)
		}

		hitReason := composeHitReason(degradationHit, chronicHit, lowTrafficHit)

		mtlfLog.Debugf(
			"Accuracy policy [%s]: scope=%s metric=%s current=%.4f mean=%.4f std=%.4f "+
				"zscore=%.4f degradationEligible=%t degradationSignal=%s "+
				"degradationBaselineReady=%t recentBaselineReady=%t actualTrafficScale=%.4f recentTrafficScaleMean=%.4f "+
				"predictedTrafficScale=%.4f chronicEligible=%t chronicSignal=%s chronicValue=%.4f "+
				"lowTrafficEligible=%t lowTrafficSignal=%s lowTrafficOvershootRatio=%.4f "+
				"degradationReferenceSamples=%d recentSamples=%d "+
				"degradationHits=%d/%d chronicHits=%d/%d lowTrafficHits=%d/%d hitReason=%s",
			modelUrl,
			report.ScopeKey,
			primaryMetric,
			current,
			mean,
			std,
			zscore,
			degradationEligible,
			degradationSignalState,
			degradationBaselineReady,
			recentBaselineReady,
			report.TrafficScale,
			recentTrafficScaleMean,
			report.PredictedTrafficScale,
			chronicEligible,
			chronicSignalState,
			chronicValue,
			lowTrafficEligible,
			lowTrafficSignalState,
			lowTrafficOvershootRatio,
			degradationHistoryCount,
			recentHistoryCount,
			degradationHits,
			requiredHits,
			chronicHits,
			requiredHits,
			lowTrafficHits,
			requiredHits,
			hitReason,
		)

		if degradationHits >= requiredHits || chronicHits >= requiredHits || lowTrafficHits >= requiredHits {
			mtlfLog.Warnf(
				"RetrainTrigger: scope=%s metric=%s current=%.4f "+
					"degradationHits=%d/%d chronicHits=%d/%d lowTrafficHits=%d/%d reason=%s",
				report.ScopeKey,
				primaryMetric,
				current,
				degradationHits, requiredHits,
				chronicHits, requiredHits,
				lowTrafficHits, requiredHits,
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
