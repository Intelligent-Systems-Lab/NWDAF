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
	cfg := a.config()
	if !isAccuracyMonitorEnabled(cfg) {
		return
	}

	nwdafCtx := nwdaf_context.GetSelf()
	store, _ := nwdafCtx.GetOrCreateModelAccuracyStore(modelUrl)

	accCfg := accuracyMonitorConfig(cfg)
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
	if !isAccuracyMonitorEnabled(a.config()) {
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
		if drained := store.DiscardAllPredictions(); drained > 0 {
			anlfLog.Infof("Accuracy monitor: discarded %d warmup predictions for model=%s",
				drained, modelUrl)
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

// checkModelAccuracy scans pending predictions, computes sMAPE for the matched
// pred/actual pairs, and reports the deviation to MTLF via onDeviationReport.
// the deviation to MTLF via the onDeviationReport callback.
// Per TS 23.288 §6.2D: AnLF generates Analytics Accuracy Information from
// prediction vs ground truth comparison.
func (a *AnlfService) checkModelAccuracy(
	modelUrl string,
	store *nwdaf_context.ModelAccuracyStore,
	accCfg *factory.AccuracyMonitorConfig,
) {
	nwdafCtx := nwdaf_context.GetSelf()

	samplingInterval := ueCommunicationModelParams(a.config()).SamplingIntervalOrDefault()
	pending := store.SnapshotPredictions()
	if len(pending) == 0 {
		return
	}
	maxMissCount := predictionMaxMissCount(samplingInterval, accCfg.CheckInterval)

	var pairs []matchedPair
	scopedPairs := make(map[string]*scopedPairAccumulator)
	unscopedMatches := 0
	gtCounts := make([]string, 0, len(pending))
	matchedIDs := make(map[uint64]struct{}, len(pending))
	missedIDs := make(map[uint64]struct{}, len(pending))
	sourceCounts := map[string]int{
		"mongo":  0,
		"memory": 0,
		"none":   0,
	}
	for _, pred := range pending {
		actual := a.lookupGroundTruth(nwdafCtx, pred)
		if actual != nil {
			matchedIDs[pred.ID] = struct{}{}
			sourceCounts[actual.source]++
			pair := matchedPair{
				predUl: pred.PredUlVol, predDl: pred.PredDlVol,
				actualUl: actual.ulVol, actualDl: actual.dlVol,
			}
			pairs = append(pairs, pair)
			if pred.ScopeKey != "" {
				recordScopedPair(scopedPairs, pred, pair)
			} else {
				unscopedMatches++
			}
			gtCounts = append(gtCounts, strconv.Itoa(actual.count))
		} else {
			missedIDs[pred.ID] = struct{}{}
			sourceCounts["none"]++
			gtCounts = append(gtCounts, "0")
		}
	}
	matchedCount, discardedCount := store.ResolvePredictions(matchedIDs, missedIDs, maxMissCount)
	anlfLog.Debugf(
		"Ground truth [%d pending → %d matched, %d discarded]: "+
			"si=%ds checkInterval=%ds maxMissCount=%d "+
			"source[mongo=%d memory=%d none=%d] matches=%s",
		len(pending), matchedCount, discardedCount, samplingInterval, accCfg.CheckInterval, maxMissCount,
		sourceCounts["mongo"], sourceCounts["memory"], sourceCounts["none"], strings.Join(gtCounts, ","),
	)

	if len(pairs) == 0 {
		anlfLog.Debugf("No matched pairs for model: %s", modelUrl)
		return
	}

	deviation := computeSMAPE(pairs)
	inferenceNum := store.GetAndResetInferenceNum()
	store.UpdateDeviation(deviation)
	reports := buildAccuracyReports(modelUrl, scopedPairs, inferenceNum)

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
	eligibleReports := make([]AccuracyReport, 0, len(reports))
	for _, report := range reports {
		if report.SampleCount < minSamples {
			anlfLog.Debugf(
				"Accuracy scope skipped by minSamples [%s]: scope=%s samples=%d < %d",
				modelUrl, report.ScopeKey, report.SampleCount, minSamples,
			)
			continue
		}
		eligibleReports = append(eligibleReports, report)
	}
	if len(eligibleReports) > 0 && a.onAccuracyReports != nil {
		a.onAccuracyReports(modelUrl, eligibleReports, store)
	}
	if len(pairs) < minSamples {
		anlfLog.Debugf("Not enough samples [%s]: %d < %d", modelUrl, len(pairs), minSamples)
		return
	}

	// Report to MTLF — MTLF decides whether to retrain (TS 23.288 §6.2E)
	if a.onDeviationReport != nil {
		a.onDeviationReport(modelUrl, deviation, store)
	}
}

func predictionMaxMissCount(samplingInterval, checkInterval int) int {
	if samplingInterval <= 0 {
		samplingInterval = 10
	}
	if checkInterval <= 0 {
		checkInterval = 60
	}
	maxMissCount := int(math.Ceil(float64(2*samplingInterval)/float64(checkInterval))) + 1
	if maxMissCount < 1 {
		return 1
	}
	return maxMissCount
}

// groundTruth holds actual measurement values for one time window.
type groundTruth struct {
	ulVol, dlVol int64
	count        int // number of DB/in-memory records aggregated
	source       string
}

// lookupGroundTruth finds actual UPF traffic data matching a prediction's target
// slot. Both predictions and actuals are mapped onto the same slot grid by
// rounding actual timestamps relative to pred.TargetSlotTime; only exact slot-key
// matches are accepted.
func (a *AnlfService) lookupGroundTruth(
	ctx *nwdaf_context.NWDAFContext,
	pred nwdaf_context.PredictionRecord,
) *groundTruth {
	cfg := a.config()
	samplingInterval := ueCommunicationModelParams(cfg).SamplingIntervalOrDefault()
	si := time.Duration(samplingInterval) * time.Second

	corrIds := ctx.GetCorrelationIdsByNwdafSubId(pred.NwdafSubId)
	if len(corrIds) == 0 {
		return nil
	}

	slotTime := pred.TargetSlotTime
	if slotTime.IsZero() {
		slotTime = pred.TargetTime
	}
	from := slotTime.Add(-si)
	to := slotTime.Add(si)

	// Primary: MongoDB — query a slot-sized window around the target slot and
	// aggregate only records that map back to the same slot key.
	if cfg != nil && cfg.Configuration != nil && cfg.Configuration.Mongodb != nil &&
		nwdaf_context.IsMongoAvailable() {
		dbName := cfg.Configuration.Mongodb.Name
		records, err := nwdaf_context.QueryTrafficInTimeRange(dbName, corrIds, from, to)
		if err == nil && len(records) > 0 {
			gt := aggregateRecordsForTargetSlot(corrIds, records, slotTime, samplingInterval)
			if gt != nil {
				anlfLog.Debugf(
					"Ground truth slot match: source=mongo sub=%s targetTime=%s targetSlotTime=%s corrIds=%d contributors=%d",
					pred.NwdafSubId, pred.TargetTime.Format(time.RFC3339), slotTime.Format(time.RFC3339), len(corrIds), gt.count,
				)
				return gt
			}
		}
		if err != nil {
			anlfLog.Debugf("MongoDB ground truth query failed, falling back to in-memory: %v", err)
		}
	}

	// Fallback: in-memory scan — aggregate only the data points that map to the
	// same slot identity as pred.TargetSlotTime.
	var ulVol, dlVol int64
	count := 0
	for _, corrId := range corrIds {
		allData := ctx.GetAllTrafficDataForCorrelation(corrId)
		var corrUl, corrDl int64
		matched := false
		for _, td := range allData {
			td.Lock()
			var slotMatch *nwdaf_context.UpfDataPoint
			for _, dp := range td.RawUpfData {
				if slotKeyForTime(dp.Timestamp, slotTime, samplingInterval) == 0 {
					dpCopy := dp
					slotMatch = &dpCopy
				}
			}
			td.Unlock()
			if slotMatch != nil {
				corrUl += slotMatch.UlVolume
				corrDl += slotMatch.DlVolume
				matched = true
			}
		}
		if matched {
			ulVol += corrUl
			dlVol += corrDl
			count++
		}
	}
	if count == 0 {
		return nil
	}
	anlfLog.Debugf(
		"Ground truth slot match: source=memory sub=%s targetTime=%s targetSlotTime=%s corrIds=%d contributors=%d",
		pred.NwdafSubId, pred.TargetTime.Format(time.RFC3339), slotTime.Format(time.RFC3339), len(corrIds), count,
	)
	return &groundTruth{ulVol: ulVol, dlVol: dlVol, count: count, source: "memory"}
}

func slotKeyForTime(ts, slotOrigin time.Time, samplingInterval int) int {
	if samplingInterval <= 0 {
		samplingInterval = 10
	}
	slotWidth := time.Duration(samplingInterval) * time.Second
	return int(math.Round(float64(ts.Sub(slotOrigin)) / float64(slotWidth)))
}

// aggregateRecordsForTargetSlot groups MongoDB records by corrId and IP session,
// keeps only those that map to the same slot key as slotTime, and sums them.
func aggregateRecordsForTargetSlot(
	corrIds []string,
	records []nwdaf_context.UpfTrafficRecord,
	slotTime time.Time,
	samplingInterval int,
) *groundTruth {
	type slotRecord struct {
		ulVol, dlVol int64
	}
	byCorrIP := make(map[string]slotRecord)
	for _, r := range records {
		if slotKeyForTime(r.Timestamp, slotTime, samplingInterval) != 0 {
			continue
		}
		key := r.Metadata.CorrelationId + "\x00" + r.Metadata.IpAddr
		byCorrIP[key] = slotRecord{ulVol: r.UlVolume, dlVol: r.DlVolume}
	}
	byCorr := make(map[string]slotRecord, len(corrIds))
	for key, record := range byCorrIP {
		corrID, _, _ := strings.Cut(key, "\x00")
		agg := byCorr[corrID]
		agg.ulVol += record.ulVol
		agg.dlVol += record.dlVol
		byCorr[corrID] = agg
	}

	var ulVol, dlVol int64
	count := 0
	for _, corrID := range corrIds {
		if record, ok := byCorr[corrID]; ok {
			ulVol += record.ulVol
			dlVol += record.dlVol
			count++
		}
	}
	if count == 0 {
		return nil
	}
	return &groundTruth{ulVol: ulVol, dlVol: dlVol, count: count, source: "mongo"}
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

func computeSumAbsPred(pairs []matchedPair) float64 {
	var sumAbsPred float64
	for _, p := range pairs {
		sumAbsPred += math.Abs(float64(p.predUl))
		sumAbsPred += math.Abs(float64(p.predDl))
	}
	return sumAbsPred
}

func computeMeanAbsPred(pairs []matchedPair) float64 {
	if len(pairs) == 0 {
		return 0
	}
	return computeSumAbsPred(pairs) / float64(len(pairs)*2)
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
			ModelURL:              modelURL,
			ScopeKey:              scopeKey,
			NwdafSubID:            singleNwdafSubID(acc.nwdafSubIDs),
			Metrics:               computeAll(acc.pairs),
			TrafficScale:          computeMeanAbsActual(acc.pairs),
			PredictedTrafficScale: computeMeanAbsPred(acc.pairs),
			SampleCount:           len(acc.pairs),
			InferenceNum:          inferenceNum,
			WindowStart:           acc.windowStart,
			WindowEnd:             acc.windowEnd,
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
