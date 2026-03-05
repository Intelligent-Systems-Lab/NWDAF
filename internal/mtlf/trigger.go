package mtlf

import (
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
)

// HandleDeviationReport receives accuracy information from AnLF and decides
// whether to trigger model retraining.
// Per TS 23.288 §6.2E: MTLF analyzes accuracy degradation reported by AnLF
// and determines whether retraining is necessary.
func (m *MtlfService) HandleDeviationReport(
	modelUrl string,
	deviation float64,
	store *nwdaf_context.ModelAccuracyStore,
) {
	cfg := factory.NwdafConfig
	if cfg == nil || cfg.Configuration == nil ||
		cfg.Configuration.Mtlf == nil ||
		cfg.Configuration.Mtlf.AccuracyMonitor == nil {
		return
	}
	accCfg := cfg.Configuration.Mtlf.AccuracyMonitor

	threshold := accCfg.DeviationThreshold
	if threshold <= 0 {
		threshold = 0.3
	}

	// Skip evaluation if a retrain is already in flight for this model.
	if store.IsRetraining() {
		mtlfLog.Debugf("Retraining in progress, skipping deviation check: model=%s", modelUrl)
		return
	}

	strategy := accCfg.TriggerStrategy
	if strategy == "" {
		strategy = "consecutive"
	}

	switch strategy {
	case "ema":
		m.checkEMATrigger(modelUrl, store, deviation, threshold, accCfg)
	default: // "consecutive"
		m.checkConsecutiveTrigger(modelUrl, store, deviation, threshold, accCfg)
	}
}

// checkConsecutiveTrigger triggers retraining after N consecutive threshold breaches.
func (m *MtlfService) checkConsecutiveTrigger(
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
			store.SetRetraining(true)
			m.TriggerRetraining(modelUrl, store)
		}
	} else {
		store.ResetBreaches()
	}
}

// checkEMATrigger triggers retraining when EMA-smoothed deviation exceeds threshold.
func (m *MtlfService) checkEMATrigger(
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
		store.SetRetraining(true)
		m.TriggerRetraining(modelUrl, store)
	}
}
