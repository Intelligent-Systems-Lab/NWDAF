package anlf

import (
	"context"
	"math"
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
		a.runModelAccuracyLoop(monCtx, modelUrl, store, accCfg)
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
) {
	warmup := accCfg.WarmupDuration
	if warmup <= 0 {
		warmup = 120
	}
	anlfLog.Infof("Accuracy monitor warmup: model=%s, waiting %ds", modelUrl, warmup)
	select {
	case <-time.After(time.Duration(warmup) * time.Second):
		anlfLog.Infof("Accuracy monitor warmup complete: model=%s", modelUrl)
	case <-ctx.Done():
		anlfLog.Infof("Accuracy monitor exiting during warmup: model=%s", modelUrl)
		return
	}

	// Discard predictions and inference counter accumulated during warmup —
	// they reflect model behaviour before it stabilized and should not
	// influence accuracy scoring.
	if drained := store.ConsumeMaturePredictions(); len(drained) > 0 {
		anlfLog.Infof("Accuracy monitor: discarded %d warmup predictions for model=%s",
			len(drained), modelUrl)
	}
	store.GetAndResetInferenceNum() // discard warmup inference count

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

	mature := store.ConsumeMaturePredictions()
	if len(mature) == 0 {
		return
	}

	var pairs []matchedPair
	for _, pred := range mature {
		actual := a.lookupGroundTruth(nwdafCtx, pred)
		if actual != nil {
			pairs = append(pairs, matchedPair{
				predUl: pred.PredUlVol, predDl: pred.PredDlVol,
				actualUl: actual.ulVol, actualDl: actual.dlVol,
			})
		}
	}

	if len(pairs) == 0 {
		anlfLog.Debugf("No matched pairs for model: %s", modelUrl)
		return
	}

	deviation := computeSMAPE(pairs)
	inferenceNum := store.GetAndResetInferenceNum()
	store.UpdateDeviation(deviation)

	pseudoAccuracy := int(math.Max(0, 100-deviation*50))
	anlfLog.Infof("Accuracy [%s]: deviation=%.4f, accuracy=%d%%, samples=%d, inferences=%d",
		modelUrl, deviation, pseudoAccuracy, len(pairs), inferenceNum)

	minSamples := accCfg.MinSamples
	if minSamples <= 0 {
		minSamples = 5
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

// groundTruth holds actual measurement values for one time window.
type groundTruth struct {
	ulVol, dlVol int64
}

// lookupGroundTruth finds actual UPF traffic data matching a prediction.
// Primary source: MongoDB (precise time-range query).
// Fallback: in-memory scan (when MongoDB is unavailable or returns nothing).
func (a *AnlfService) lookupGroundTruth(
	ctx *nwdaf_context.NWDAFContext,
	pred nwdaf_context.PredictionRecord,
) *groundTruth {
	cfg := factory.NwdafConfig
	from := pred.TargetTime
	samplingInterval := 10
	if cfg != nil && cfg.Configuration != nil &&
		cfg.Configuration.Analytics != nil &&
		cfg.Configuration.Analytics.UeCommunication != nil {
		samplingInterval = cfg.Configuration.Analytics.UeCommunication.SamplingIntervalOrDefault()
	}
	to := pred.TargetTime.Add(time.Duration(samplingInterval) * time.Second)

	// Primary: MongoDB time-range query
	if cfg != nil && cfg.Configuration != nil && cfg.Configuration.Mongodb != nil &&
		nwdaf_context.IsMongoAvailable() {
		dbName := cfg.Configuration.Mongodb.Name
		corrIds := ctx.GetCorrelationIdsByNwdafSubId(pred.NwdafSubId)
		if len(corrIds) > 0 {
			records, err := nwdaf_context.QueryTrafficInTimeRange(dbName, corrIds, from, to)
			if err == nil && len(records) > 0 {
				var ulVol, dlVol int64
				for _, r := range records {
					ulVol += r.UlVolume
					dlVol += r.DlVolume
				}
				return &groundTruth{ulVol: ulVol, dlVol: dlVol}
			}
			if err != nil {
				anlfLog.Debugf("MongoDB ground truth query failed, falling back to in-memory: %v", err)
			}
		}
	}

	// Fallback: in-memory scan
	dataList := ctx.GetTrafficDataByNwdafSubId(pred.NwdafSubId)
	if len(dataList) == 0 {
		return nil
	}

	var ulVol, dlVol int64
	window := time.Duration(samplingInterval) * time.Second
	for _, td := range dataList {
		td.Lock()
		for _, dp := range td.RawUpfData {
			diff := dp.Timestamp.Sub(pred.TargetTime)
			if diff >= 0 && diff < window {
				ulVol += dp.UlVolume
				dlVol += dp.DlVolume
			}
		}
		td.Unlock()
	}
	if ulVol == 0 && dlVol == 0 {
		return nil
	}
	return &groundTruth{ulVol: ulVol, dlVol: dlVol}
}

// matchedPair holds a prediction-truth pair for sMAPE computation.
type matchedPair struct {
	predUl, predDl     int64
	actualUl, actualDl int64
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
