package anlf

import (
	"context"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
)

// StartAccuracyMonitorForModel starts a per-model accuracy monitoring goroutine.
// Idempotent — skips if a monitor is already running for this modelUrl.
// Per TS 23.288 §6.2D: monitoring activated when analytics model becomes active.
func (a *AnlfService) StartAccuracyMonitorForModel(
	modelUrl string, wg *sync.WaitGroup,
) {
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil ||
		cfg.Configuration.Mtlf == nil || !cfg.Configuration.Mtlf.Enabled ||
		cfg.Configuration.Mtlf.AccuracyMonitor == nil ||
		!cfg.Configuration.Mtlf.AccuracyMonitor.Enabled {
		return
	}

	nwdafCtx := nwdaf_context.GetSelf()
	store, _ := nwdafCtx.GetOrCreateModelAccuracyStore(modelUrl)

	accCfg := cfg.Configuration.Mtlf.AccuracyMonitor
	interval := accCfg.CheckInterval
	if interval <= 0 {
		interval = 60
	}

	monCtx, cancel := context.WithCancel(a.nwdaf.CancelContext())
	if !store.TryStartMonitor(cancel) {
		anlfLog.Debugf("Accuracy monitor already running for model: %s", modelUrl)
		return
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		a.runModelAccuracyLoop(monCtx, modelUrl, store, accCfg, a.acquireStartupWarmupDuration(accCfg))
	}()

	anlfLog.Infof("Accuracy monitor started: model=%s, interval=%ds", modelUrl, interval)
}

// StopAccuracyMonitorForModel stops the monitor for a model if no subscribers remain.
// Checks SharedModelInfo subscriber count to decide.
func (a *AnlfService) StopAccuracyMonitorForModel(modelUrl string) {
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil ||
		cfg.Configuration.Mtlf == nil ||
		cfg.Configuration.Mtlf.AccuracyMonitor == nil ||
		!cfg.Configuration.Mtlf.AccuracyMonitor.Enabled {
		return
	}

	nwdafCtx := nwdaf_context.GetSelf()
	shared := nwdafCtx.GetSharedModel(modelUrl)
	if shared != nil && shared.SubscriberCount() > 0 {
		return
	}

	nwdafCtx.DeleteModelAccuracyStore(modelUrl)
	anlfLog.Infof("Accuracy monitor stopped: model=%s", modelUrl)
}

// runModelAccuracyLoop periodically checks accuracy for one model.
func (a *AnlfService) runModelAccuracyLoop(
	ctx context.Context,
	modelUrl string,
	store *nwdaf_context.ModelAccuracyStore,
	accCfg *factory.AccuracyMonitorConfig,
	warmup int,
) {
	if _, err := getAccuracyCSVWriter(a.nwdaf.CancelContext(), accCfg); err != nil {
		anlfLog.Warnf("Accuracy CSV initialization failed: %v", err)
	}
	if warmup > 0 {
		anlfLog.Infof("Accuracy monitor warmup: model=%s, waiting %ds", modelUrl, warmup)
		select {
		case <-time.After(time.Duration(warmup) * time.Second):
			anlfLog.Infof("Accuracy monitor warmup complete: model=%s", modelUrl)
		case <-ctx.Done():
			anlfLog.Infof("Accuracy monitor exiting during warmup: model=%s", modelUrl)
			return
		}

		// Startup warmup discards pre-stabilization predictions and inference
		// counters. Post-swap monitors intentionally skip this path.
		if drained := store.ConsumeMaturePredictions(0); len(drained) > 0 {
			anlfLog.Infof("Accuracy monitor: discarded %d warmup predictions for model=%s",
				len(drained), modelUrl)
		}
		store.GetAndResetInferenceNum() // discard warmup inference count
	} else {
		anlfLog.Infof("Accuracy monitor warmup skipped: model=%s", modelUrl)
	}

	interval := accCfg.CheckInterval
	if interval <= 0 {
		interval = 60
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.checkModelAccuracy(modelUrl, store, accCfg)
		case <-ctx.Done():
			anlfLog.Infof("Accuracy monitor exiting: model=%s", modelUrl)
			return
		}
	}
}

// checkModelAccuracy collects mature predictions, computes sMAPE, and reports
// the deviation to MTLF via the onDeviationReport callback.
// Per TS 23.288 §6.2D: AnLF generates Analytics Accuracy Information from
// prediction vs ground truth comparison.
func (a *AnlfService) checkModelAccuracy(
	modelUrl string,
	store *nwdaf_context.ModelAccuracyStore,
	accCfg *factory.AccuracyMonitorConfig,
) {
	nwdafCtx := nwdaf_context.GetSelf()
	checkTime := time.Now()

	si := time.Duration(getUeCommunicationModelParams().SamplingIntervalOrDefault()) * time.Second
	mature := store.ConsumeMaturePredictions(2 * si)
	if len(mature) == 0 {
		return
	}

	var pairs []matchedPair
	pairRows := make([]pairCSVRecord, 0, len(mature))
	scopedPairs := make(map[string]*scopedPairAccumulator)
	unscopedMatches := 0
	gtCounts := make([]string, 0, len(mature))
	for _, pred := range mature {
		actual := a.lookupGroundTruth(nwdafCtx, pred)
		row := pairCSVRecord{
			CheckTime:   checkTime,
			ModelURL:    modelUrl,
			ScopeKey:    pred.ScopeKey,
			NwdafSubID:  pred.NwdafSubId,
			PredictedAt: pred.PredictedAt,
			TargetTime:  pred.TargetTime,
			PredUl:      pred.PredUlVol,
			PredDl:      pred.PredDlVol,
		}
		if actual != nil {
			pair := matchedPair{
				predUl: pred.PredUlVol, predDl: pred.PredDlVol,
				actualUl: actual.ulVol, actualDl: actual.dlVol,
			}
			row.ActualUl = &actual.ulVol
			row.ActualDl = &actual.dlVol
			pairs = append(pairs, pair)
			if pred.ScopeKey != "" {
				recordScopedPair(scopedPairs, pred, pair)
			} else {
				unscopedMatches++
			}
			gtCounts = append(gtCounts, strconv.Itoa(actual.count))
		} else {
			gtCounts = append(gtCounts, "0")
		}
		pairRows = append(pairRows, row)
	}
	anlfLog.Debugf("Ground truth [%d mature → %d matched]: %s",
		len(mature), len(pairs), strings.Join(gtCounts, ","))

	if len(pairs) == 0 {
		anlfLog.Debugf("No matched pairs for model: %s", modelUrl)
		return
	}

	deviation := computeSMAPE(pairs)
	inferenceNum := store.GetAndResetInferenceNum()
	store.UpdateDeviation(deviation)
	reports := buildAccuracyReports(modelUrl, scopedPairs, inferenceNum)
	a.dumpAccuracyCSV(accCfg, checkTime, reports, pairRows)

	pseudoAccuracy := int(math.Max(0, 100-deviation*50))
	anlfLog.Infof("Accuracy [%s]: deviation=%.4f, accuracy=%d%%, samples=%d, inferences=%d",
		modelUrl, deviation, pseudoAccuracy, len(pairs), inferenceNum)
	for _, report := range reports {
		anlfLog.Infof("Accuracy scope [%s]: scope=%s samples=%d metrics=%s",
			modelUrl, report.ScopeKey, report.SampleCount, formatMetrics(report.Metrics))
	}
	if unscopedMatches > 0 {
		anlfLog.Warnf("Accuracy scope skipped [%s]: %d matched predictions missing scopeKey",
			modelUrl, unscopedMatches)
	}

	minSamples := accCfg.MinSamples
	if minSamples <= 0 {
		minSamples = 5
	}
	if len(pairs) < minSamples {
		anlfLog.Debugf("Not enough samples [%s]: %d < %d", modelUrl, len(pairs), minSamples)
		return
	}

	if len(reports) > 0 && a.onAccuracyReports != nil {
		a.onAccuracyReports(modelUrl, reports, store)
	}

	// Report to MTLF — MTLF decides whether to retrain (TS 23.288 §6.2E)
	if a.onDeviationReport != nil {
		a.onDeviationReport(modelUrl, deviation, store)
	}
}

func (a *AnlfService) dumpAccuracyCSV(
	accCfg *factory.AccuracyMonitorConfig,
	checkTime time.Time,
	reports []AccuracyReport,
	pairRows []pairCSVRecord,
) {
	writer, err := getAccuracyCSVWriter(a.nwdaf.CancelContext(), accCfg)
	if err != nil {
		anlfLog.Warnf("Accuracy CSV unavailable: %v", err)
		return
	}
	if writer == nil {
		return
	}

	writeErr := writer.WritePairs(pairRows)
	if writeErr != nil {
		anlfLog.Warnf("Accuracy pairs CSV write failed: %v", writeErr)
	}
	writeErr = writer.WriteMetrics(checkTime, reports)
	if writeErr != nil {
		anlfLog.Warnf("Accuracy metrics CSV write failed: %v", writeErr)
	}
}

// groundTruth holds actual measurement values for one time window.
type groundTruth struct {
	ulVol, dlVol int64
	count        int // number of DB/in-memory records aggregated
}

// lookupGroundTruth finds actual UPF traffic data matching a prediction.
// For each expected corrId (derived from pred.NwdafSubId), the record closest to
// pred.TargetTime within ±samplingInterval is selected. Values are summed across
// all corrIds to produce the group-level ground truth.
// Primary source: MongoDB. Fallback: in-memory scan.
func (a *AnlfService) lookupGroundTruth(
	ctx *nwdaf_context.NWDAFContext,
	pred nwdaf_context.PredictionRecord,
) *groundTruth {
	cfg := factory.NwdafConfig
	samplingInterval := 10
	if cfg != nil && cfg.Configuration != nil &&
		cfg.Configuration.Analytics != nil &&
		cfg.Configuration.Analytics.UeCommunication != nil {
		samplingInterval = cfg.Configuration.Analytics.UeCommunication.SamplingIntervalOrDefault()
	}
	si := time.Duration(samplingInterval) * time.Second

	corrIds := ctx.GetCorrelationIdsByNwdafSubId(pred.NwdafSubId)
	if len(corrIds) == 0 {
		return nil
	}

	// Query window: TargetTime ± si to absorb jitter.
	from := pred.TargetTime.Add(-si)
	to := pred.TargetTime.Add(si)

	// Primary: MongoDB — query wider window, then pick nearest per corrId.
	if cfg != nil && cfg.Configuration != nil && cfg.Configuration.Mongodb != nil &&
		nwdaf_context.IsMongoAvailable() {
		dbName := cfg.Configuration.Mongodb.Name
		records, err := nwdaf_context.QueryTrafficInTimeRange(dbName, corrIds, from, to)
		if err == nil && len(records) > 0 {
			gt := nearestPerCorrId(corrIds, records, pred.TargetTime)
			if gt != nil {
				return gt
			}
		}
		if err != nil {
			anlfLog.Debugf("MongoDB ground truth query failed, falling back to in-memory: %v", err)
		}
	}

	// Fallback: in-memory scan — per corrId, find nearest data point to TargetTime.
	var ulVol, dlVol int64
	count := 0
	for _, corrId := range corrIds {
		allData := ctx.GetAllTrafficDataForCorrelation(corrId)
		var bestDiff time.Duration = -1
		var bestUl, bestDl int64
		for _, td := range allData {
			td.Lock()
			for _, dp := range td.RawUpfData {
				diff := dp.Timestamp.Sub(pred.TargetTime)
				if diff < 0 {
					diff = -diff
				}
				if diff <= si && (bestDiff < 0 || diff < bestDiff) {
					bestDiff = diff
					bestUl = dp.UlVolume
					bestDl = dp.DlVolume
				}
			}
			td.Unlock()
		}
		if bestDiff >= 0 {
			ulVol += bestUl
			dlVol += bestDl
			count++
		}
	}
	if count == 0 {
		return nil
	}
	return &groundTruth{ulVol: ulVol, dlVol: dlVol, count: count}
}

// nearestPerCorrId selects the record closest to targetTime for each corrId,
// then sums UL/DL volumes across all corrIds.
func nearestPerCorrId(
	corrIds []string,
	records []nwdaf_context.UpfTrafficRecord,
	targetTime time.Time,
) *groundTruth {
	type best struct {
		diff         time.Duration
		ulVol, dlVol int64
	}
	byCorr := make(map[string]*best, len(corrIds))
	for _, id := range corrIds {
		byCorr[id] = nil
	}
	for _, r := range records {
		diff := r.Timestamp.Sub(targetTime)
		if diff < 0 {
			diff = -diff
		}
		b := byCorr[r.Metadata.CorrelationId]
		if b == nil || diff < b.diff {
			byCorr[r.Metadata.CorrelationId] = &best{diff: diff, ulVol: r.UlVolume, dlVol: r.DlVolume}
		}
	}
	var ulVol, dlVol int64
	count := 0
	for _, b := range byCorr {
		if b != nil {
			ulVol += b.ulVol
			dlVol += b.dlVol
			count++
		}
	}
	if count == 0 {
		return nil
	}
	return &groundTruth{ulVol: ulVol, dlVol: dlVol, count: count}
}

// matchedPair holds a prediction-truth pair for sMAPE computation.
type matchedPair struct {
	predUl, predDl     int64
	actualUl, actualDl int64
}

type scopedPairAccumulator struct {
	pairs       []matchedPair
	nwdafSubIDs map[string]struct{}
	windowStart time.Time
	windowEnd   time.Time
}

// computeSMAPE calculates Symmetric Mean Absolute Percentage Error.
// Each UL and DL channel is treated as an independent sample.
// Result is in [0, 2]; when both actual and pred are zero the sample contributes 0.
func computeSMAPE(pairs []matchedPair) float64 {
	if len(pairs) == 0 {
		return 0
	}

	var sumSMAPE float64
	n := float64(len(pairs) * 2)

	for _, p := range pairs {
		ulDenom := math.Abs(float64(p.actualUl)) + math.Abs(float64(p.predUl))
		dlDenom := math.Abs(float64(p.actualDl)) + math.Abs(float64(p.predDl))

		if ulDenom > 0 {
			sumSMAPE += math.Abs(float64(p.predUl-p.actualUl)) / (ulDenom / 2)
		}
		if dlDenom > 0 {
			sumSMAPE += math.Abs(float64(p.predDl-p.actualDl)) / (dlDenom / 2)
		}
	}

	return sumSMAPE / n
}

// computeMAE calculates Mean Absolute Error across UL and DL channels.
func computeMAE(pairs []matchedPair) float64 {
	if len(pairs) == 0 {
		return 0
	}

	var sumAbsErr float64
	for _, p := range pairs {
		sumAbsErr += math.Abs(float64(p.predUl - p.actualUl))
		sumAbsErr += math.Abs(float64(p.predDl - p.actualDl))
	}

	return sumAbsErr / float64(len(pairs)*2)
}

// computeMSE calculates Mean Squared Error across UL and DL channels.
func computeMSE(pairs []matchedPair) float64 {
	if len(pairs) == 0 {
		return 0
	}

	var sumSqErr float64
	for _, p := range pairs {
		ulErr := float64(p.predUl - p.actualUl)
		dlErr := float64(p.predDl - p.actualDl)
		sumSqErr += ulErr * ulErr
		sumSqErr += dlErr * dlErr
	}

	return sumSqErr / float64(len(pairs)*2)
}

// computeWAPE calculates Weighted Absolute Percentage Error across UL and DL channels.
func computeWAPE(pairs []matchedPair) float64 {
	if len(pairs) == 0 {
		return 0
	}

	var sumAbsErr float64
	for _, p := range pairs {
		sumAbsErr += math.Abs(float64(p.predUl - p.actualUl))
		sumAbsErr += math.Abs(float64(p.predDl - p.actualDl))
	}

	sumAbsActual := computeSumAbsActual(pairs)
	if sumAbsActual == 0 {
		return 0
	}

	return sumAbsErr / sumAbsActual
}

func computeSumAbsActual(pairs []matchedPair) float64 {
	var sumAbsActual float64
	for _, p := range pairs {
		sumAbsActual += math.Abs(float64(p.actualUl))
		sumAbsActual += math.Abs(float64(p.actualDl))
	}
	return sumAbsActual
}

func computeMeanAbsActual(pairs []matchedPair) float64 {
	if len(pairs) == 0 {
		return 0
	}
	return computeSumAbsActual(pairs) / float64(len(pairs)*2)
}

// computeNRMSE calculates Normalized Root Mean Squared Error across UL and DL channels.
// RMSE is normalized by the mean absolute actual volume of all channels.
func computeNRMSE(pairs []matchedPair) float64 {
	if len(pairs) == 0 {
		return 0
	}

	meanAbsActual := computeMeanAbsActual(pairs)
	if meanAbsActual == 0 {
		return 0
	}

	return math.Sqrt(computeMSE(pairs)) / meanAbsActual
}

func computeAll(pairs []matchedPair) map[string]float64 {
	return map[string]float64{
		"sMAPE": computeSMAPE(pairs),
		"MAE":   computeMAE(pairs),
		"MSE":   computeMSE(pairs),
		"WAPE":  computeWAPE(pairs),
		"NRMSE": computeNRMSE(pairs),
	}
}

func recordScopedPair(
	scopedPairs map[string]*scopedPairAccumulator,
	pred nwdaf_context.PredictionRecord,
	pair matchedPair,
) {
	acc := scopedPairs[pred.ScopeKey]
	if acc == nil {
		acc = &scopedPairAccumulator{
			nwdafSubIDs: make(map[string]struct{}),
			windowStart: pred.TargetTime,
			windowEnd:   pred.TargetTime,
		}
		scopedPairs[pred.ScopeKey] = acc
	}

	acc.pairs = append(acc.pairs, pair)
	if pred.NwdafSubId != "" {
		acc.nwdafSubIDs[pred.NwdafSubId] = struct{}{}
	}
	if pred.TargetTime.Before(acc.windowStart) {
		acc.windowStart = pred.TargetTime
	}
	if pred.TargetTime.After(acc.windowEnd) {
		acc.windowEnd = pred.TargetTime
	}
}

func buildAccuracyReports(
	modelURL string,
	scopedPairs map[string]*scopedPairAccumulator,
	inferenceNum int,
) []AccuracyReport {
	if len(scopedPairs) == 0 {
		return nil
	}

	scopeKeys := make([]string, 0, len(scopedPairs))
	for scopeKey := range scopedPairs {
		scopeKeys = append(scopeKeys, scopeKey)
	}
	slices.Sort(scopeKeys)

	reports := make([]AccuracyReport, 0, len(scopeKeys))
	for _, scopeKey := range scopeKeys {
		acc := scopedPairs[scopeKey]
		if acc == nil || len(acc.pairs) == 0 {
			continue
		}

		report := AccuracyReport{
			ModelURL:     modelURL,
			ScopeKey:     scopeKey,
			NwdafSubID:   singleNwdafSubID(acc.nwdafSubIDs),
			Metrics:      computeAll(acc.pairs),
			TrafficScale: computeMeanAbsActual(acc.pairs),
			SampleCount:  len(acc.pairs),
			InferenceNum: inferenceNum,
			WindowStart:  acc.windowStart,
			WindowEnd:    acc.windowEnd,
		}
		reports = append(reports, report)
	}

	return reports
}

func singleNwdafSubID(ids map[string]struct{}) string {
	if len(ids) != 1 {
		return ""
	}
	for id := range ids {
		return id
	}
	return ""
}

func formatMetrics(metrics map[string]float64) string {
	if len(metrics) == 0 {
		return ""
	}

	keys := make([]string, 0, len(metrics))
	for key := range metrics {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+strconv.FormatFloat(metrics[key], 'f', 4, 64))
	}

	return strings.Join(parts, ",")
}
