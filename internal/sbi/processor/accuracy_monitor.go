package processor

import (
	"context"
	"math"
	"sync"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
)

// StartAccuracyMonitorForModel starts a per-model accuracy monitoring goroutine.
// Idempotent — skips if monitor is already running for this modelUrl.
// Per TS 23.288 §5C: monitoring activated when analytics model becomes active.
func (p *Processor) StartAccuracyMonitorForModel(
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
	store, isNew := nwdafCtx.GetOrCreateModelAccuracyStore(modelUrl)

	if !isNew && store.IsMonitorRunning() {
		mtlfLog.Debugf("Accuracy monitor already running for model: %s", modelUrl)
		return
	}

	accCfg := cfg.Configuration.Mtlf.AccuracyMonitor
	interval := accCfg.CheckInterval
	if interval <= 0 {
		interval = 60
	}

	monCtx, cancel := context.WithCancel(p.nwdaf.CancelContext())
	store.SetMonitorRunning(cancel)

	wg.Add(1)
	go func() {
		defer wg.Done()
		p.runModelAccuracyLoop(monCtx, modelUrl, store, accCfg)
	}()

	mtlfLog.Infof("Accuracy monitor started: model=%s, interval=%ds",
		modelUrl, interval)
}

// StopAccuracyMonitorForModel stops the monitor for a model if no subscribers remain.
// Checks SharedModelInfo subscriber count to decide.
func (p *Processor) StopAccuracyMonitorForModel(modelUrl string) {
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil ||
		cfg.Configuration.Mtlf == nil ||
		cfg.Configuration.Mtlf.AccuracyMonitor == nil ||
		!cfg.Configuration.Mtlf.AccuracyMonitor.Enabled {
		return
	}

	// Check registry — if subscribers remain, keep monitor running
	nwdafCtx := nwdaf_context.GetSelf()
	shared := nwdafCtx.GetSharedModel(modelUrl)
	if shared != nil && shared.SubscriberCount() > 0 {
		return
	}

	// Last subscriber gone — stop monitor and clean up
	nwdafCtx.DeleteModelAccuracyStore(modelUrl)
	mtlfLog.Infof("Accuracy monitor stopped: model=%s", modelUrl)
}

// runModelAccuracyLoop periodically checks accuracy for one model
func (p *Processor) runModelAccuracyLoop(
	ctx context.Context,
	modelUrl string,
	store *nwdaf_context.ModelAccuracyStore,
	accCfg *factory.AccuracyMonitorConfig,
) {
	// Warmup: skip evaluation while model accumulates data
	warmup := accCfg.WarmupDuration
	if warmup <= 0 {
		warmup = 120
	}
	mtlfLog.Infof("Accuracy monitor warmup: model=%s, waiting %ds", modelUrl, warmup)
	select {
	case <-time.After(time.Duration(warmup) * time.Second):
		mtlfLog.Infof("Accuracy monitor warmup complete: model=%s", modelUrl)
	case <-ctx.Done():
		mtlfLog.Infof("Accuracy monitor exiting during warmup: model=%s", modelUrl)
		return
	}

	// Start periodic checks
	interval := accCfg.CheckInterval
	if interval <= 0 {
		interval = 60
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.checkModelAccuracy(modelUrl, store, accCfg)
		case <-ctx.Done():
			mtlfLog.Infof("Accuracy monitor exiting: model=%s", modelUrl)
			return
		}
	}
}

// checkModelAccuracy compares predictions against ground truth for one model
func (p *Processor) checkModelAccuracy(
	modelUrl string,
	store *nwdaf_context.ModelAccuracyStore,
	accCfg *factory.AccuracyMonitorConfig,
) {
	nwdafCtx := nwdaf_context.GetSelf()

	// Get predictions whose target time has passed
	mature := store.ConsumeMaturePredictions()
	if len(mature) == 0 {
		return
	}

	// Collect matched pairs (prediction, actual ground truth)
	var pairs []matchedPair

	for _, pred := range mature {
		actual := p.lookupGroundTruth(nwdafCtx, pred)
		if actual != nil {
			pairs = append(pairs, matchedPair{
				predUl: pred.PredUlVol, predDl: pred.PredDlVol,
				actualUl: actual.ulVol, actualDl: actual.dlVol,
			})
		}
	}

	if len(pairs) == 0 {
		mtlfLog.Debugf("No matched pairs for model: %s", modelUrl)
		return
	}

	// Compute NRMSE
	deviation := computeNRMSE(pairs)
	inferenceNum := store.GetAndResetInferenceNum()
	store.UpdateDeviation(deviation)

	pseudoAccuracy := int(math.Max(0, 100-deviation*100))

	mtlfLog.Infof("Accuracy [%s]: deviation=%.4f, accuracy=%d%%, samples=%d, inferences=%d",
		modelUrl, deviation, pseudoAccuracy, len(pairs), inferenceNum)

	// Check threshold with configured strategy
	threshold := accCfg.DeviationThreshold
	if threshold <= 0 {
		threshold = 0.3
	}
	minSamples := accCfg.MinSamples
	if minSamples <= 0 {
		minSamples = 5
	}

	if len(pairs) < minSamples {
		mtlfLog.Debugf("Not enough samples [%s]: %d < %d", modelUrl, len(pairs), minSamples)
		return
	}

	// Apply configured trigger strategy
	strategy := accCfg.TriggerStrategy
	if strategy == "" {
		strategy = "consecutive"
	}

	switch strategy {
	case "ema":
		p.checkEMATrigger(modelUrl, store, deviation, threshold, accCfg)
	default: // "consecutive"
		p.checkConsecutiveTrigger(modelUrl, store, deviation, threshold, accCfg)
	}
}

// checkConsecutiveTrigger triggers retraining after N consecutive threshold breaches
func (p *Processor) checkConsecutiveTrigger(
	modelUrl string,
	store *nwdaf_context.ModelAccuracyStore,
	deviation, threshold float64,
	accCfg *factory.AccuracyMonitorConfig,
) {
	required := accCfg.ConsecutiveBreaches
	if required <= 0 {
		required = 3
	}

	if deviation > threshold {
		count := store.IncrementBreaches()
		mtlfLog.Warnf("Threshold breach [%s]: deviation=%.4f > %.2f (%d/%d)",
			modelUrl, deviation, threshold, count, required)
		if count >= required {
			store.ResetBreaches()
			p.triggerRetraining()
			// Stop monitor to prevent repeated triggers during training
			store.StopMonitor()
			mtlfLog.Infof("Accuracy monitor paused after retrain trigger: model=%s", modelUrl)
		}
	} else {
		store.ResetBreaches()
	}
}

// checkEMATrigger triggers retraining when EMA-smoothed deviation exceeds threshold
func (p *Processor) checkEMATrigger(
	modelUrl string,
	store *nwdaf_context.ModelAccuracyStore,
	deviation, threshold float64,
	accCfg *factory.AccuracyMonitorConfig,
) {
	alpha := accCfg.EmaAlpha
	if alpha <= 0 || alpha > 1 {
		alpha = 0.3
	}

	ema := store.UpdateEMA(deviation, alpha)
	mtlfLog.Infof("EMA update [%s]: raw=%.4f, ema=%.4f, threshold=%.2f",
		modelUrl, deviation, ema, threshold)

	if ema > threshold {
		mtlfLog.Warnf("EMA degradation [%s]: ema=%.4f > threshold=%.2f",
			modelUrl, ema, threshold)
		p.triggerRetraining()
		// Stop monitor to prevent repeated triggers during training
		store.StopMonitor()
		mtlfLog.Infof("Accuracy monitor paused after retrain trigger: model=%s", modelUrl)
	}
}

// groundTruth holds actual measurement values
type groundTruth struct {
	ulVol, dlVol int64
}

// lookupGroundTruth finds actual UPF traffic data matching a prediction
func (p *Processor) lookupGroundTruth(
	ctx *nwdaf_context.NWDAFContext,
	pred nwdaf_context.PredictionRecord,
) *groundTruth {
	dataList := ctx.GetTrafficDataByNwdafSubId(pred.NwdafSubId)
	if len(dataList) == 0 {
		return nil
	}

	window := 10 * time.Second
	for _, td := range dataList {
		td.Lock()
		for _, dp := range td.RawUpfData {
			diff := dp.Timestamp.Sub(pred.TargetTime)
			if diff >= 0 && diff < window {
				gt := &groundTruth{ulVol: dp.UlVolume, dlVol: dp.DlVolume}
				td.Unlock()
				return gt
			}
		}
		td.Unlock()
	}
	return nil
}

// matchedPair holds a prediction-truth pair for NRMSE computation
type matchedPair struct {
	predUl, predDl     int64
	actualUl, actualDl int64
}

// computeNRMSE calculates Normalized Root Mean Square Error
func computeNRMSE(pairs []matchedPair) float64 {
	if len(pairs) == 0 {
		return 0
	}

	var sumSquaredError float64
	var sumActual float64
	n := float64(len(pairs) * 2)

	for _, p := range pairs {
		errUl := float64(p.predUl - p.actualUl)
		errDl := float64(p.predDl - p.actualDl)
		sumSquaredError += errUl*errUl + errDl*errDl
		sumActual += math.Abs(float64(p.actualUl)) + math.Abs(float64(p.actualDl))
	}

	rmse := math.Sqrt(sumSquaredError / n)
	meanActual := sumActual / n

	if meanActual == 0 {
		if rmse == 0 {
			return 0
		}
		return 1.0
	}

	return rmse / meanActual
}

// triggerRetraining initiates Daisy FL retraining
func (p *Processor) triggerRetraining() {
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil || cfg.Configuration.Mtlf == nil {
		return
	}
	mtlfCfg := cfg.Configuration.Mtlf

	mtlfLog.Info("Triggering retraining due to accuracy degradation")

	go func() {
		if err := p.triggerTraining(mtlfCfg); err != nil {
			mtlfLog.Errorf("Accuracy-triggered retraining failed: %v", err)
			return
		}
		mtlfLog.Info("Accuracy-triggered retraining completed successfully")
	}()
}
