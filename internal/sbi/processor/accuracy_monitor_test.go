package processor

import (
	"testing"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

// ============================================================================
// computeSMAPE — Unit Tests
// ============================================================================

func TestComputeSMAPE_Empty(t *testing.T) {
	smape := computeSMAPE(nil)
	if smape != 0 {
		t.Errorf("computeSMAPE(nil) = %.4f, want 0", smape)
	}
}

func TestComputeSMAPE_PerfectPrediction(t *testing.T) {
	pairs := []matchedPair{
		{predUl: 100, predDl: 200, actualUl: 100, actualDl: 200},
		{predUl: 300, predDl: 400, actualUl: 300, actualDl: 400},
	}
	smape := computeSMAPE(pairs)
	if smape != 0 {
		t.Errorf("Perfect prediction sMAPE = %.4f, want 0", smape)
	}
}

func TestComputeSMAPE_SmallError(t *testing.T) {
	// ul: 5/102.5=0.0488, dl: 5/197.5=0.0253 → mean ≈ 0.037
	pairs := []matchedPair{
		{predUl: 105, predDl: 195, actualUl: 100, actualDl: 200},
	}
	smape := computeSMAPE(pairs)
	if smape <= 0 || smape >= 0.1 {
		t.Errorf("Small error sMAPE = %.4f, expected in (0, 0.1)", smape)
	}
}

func TestComputeSMAPE_LargeError(t *testing.T) {
	// ul: 900/550=1.636, dl: same → sMAPE ≈ 1.636
	pairs := []matchedPair{
		{predUl: 1000, predDl: 1000, actualUl: 100, actualDl: 100},
	}
	smape := computeSMAPE(pairs)
	if smape < 1.0 {
		t.Errorf("Large error sMAPE = %.4f, expected >= 1.0", smape)
	}
}

func TestComputeSMAPE_ZeroActual(t *testing.T) {
	// actual=0, pred>0: per-sample sMAPE = |pred|/(pred/2) = 2.0 (max)
	pairs := []matchedPair{
		{predUl: 100, predDl: 200, actualUl: 0, actualDl: 0},
	}
	smape := computeSMAPE(pairs)
	if smape != 2.0 {
		t.Errorf("Zero actual sMAPE = %.4f, want 2.0", smape)
	}
}

func TestComputeSMAPE_BothZero(t *testing.T) {
	// actual=0, pred=0: denom=0, sample skipped → sMAPE=0, no NaN
	pairs := []matchedPair{
		{predUl: 0, predDl: 0, actualUl: 0, actualDl: 0},
	}
	smape := computeSMAPE(pairs)
	if smape != 0 {
		t.Errorf("Both-zero sMAPE = %.4f, want 0", smape)
	}
}

func TestComputeSMAPE_MultipleSymmetric(t *testing.T) {
	// pair1 ul: 10/105=0.0952, dl: 10/205=0.0488
	// pair2 ul: 10/95=0.1053,  dl: 10/195=0.0513 → mean ≈ 0.075
	pairs := []matchedPair{
		{predUl: 110, predDl: 210, actualUl: 100, actualDl: 200},
		{predUl: 90, predDl: 190, actualUl: 100, actualDl: 200},
	}
	smape := computeSMAPE(pairs)
	if smape <= 0 || smape >= 0.15 {
		t.Errorf("Symmetric error sMAPE = %.4f, expected in (0, 0.15)", smape)
	}
}

// ============================================================================
// Trigger Strategy — Consecutive Breaches (via ModelAccuracyStore)
// ============================================================================

func TestConsecutiveTrigger_NoRetrain_BelowThreshold(t *testing.T) {
	// Deviation below threshold should not trigger
	// This tests the logic conceptually via the store
	nwdaf_store := createTestStore()

	// Good check → reset
	nwdaf_store.ResetBreaches()
	count := nwdaf_store.IncrementBreaches()
	if count != 1 {
		t.Errorf("After reset+increment, count = %d, want 1", count)
	}
}

func TestConsecutiveTrigger_RetainAfterN(t *testing.T) {
	nwdaf_store := createTestStore()
	required := 3

	var triggered bool
	for i := 0; i < required; i++ {
		count := nwdaf_store.IncrementBreaches()
		if count >= required {
			triggered = true
		}
	}
	if !triggered {
		t.Error("Should trigger after 3 consecutive breaches")
	}
}

func TestConsecutiveTrigger_ResetOnGoodCheck(t *testing.T) {
	nwdaf_store := createTestStore()

	// 2 breaches, then good check, then 2 more
	nwdaf_store.IncrementBreaches()
	nwdaf_store.IncrementBreaches()
	nwdaf_store.ResetBreaches() // good check

	nwdaf_store.IncrementBreaches()
	count := nwdaf_store.IncrementBreaches()

	if count >= 3 {
		t.Errorf("Should not reach 3 after reset, got %d", count)
	}
}

// ============================================================================
// Trigger Strategy — EMA (via ModelAccuracyStore)
// ============================================================================

func TestEMATrigger_SingleSpike_NoTrigger(t *testing.T) {
	nwdaf_store := createTestStore()
	threshold := 0.3
	alpha := 0.2 // Lower alpha = slower response to spike

	// 20 stable checks — EMA converges to ~0.1
	for i := 0; i < 20; i++ {
		nwdaf_store.UpdateEMA(0.1, alpha)
	}

	// Single spike
	ema := nwdaf_store.UpdateEMA(0.9, alpha)

	// EMA should NOT exceed threshold: 0.2*0.9 + 0.8*≈0.1 ≈ 0.26
	if ema > threshold {
		t.Errorf("Single spike EMA = %.4f, should be <= %.2f", ema, threshold)
	}
}

func TestEMATrigger_SustainedDegradation_Triggers(t *testing.T) {
	nwdaf_store := createTestStore()
	threshold := 0.3
	alpha := 0.3

	var ema float64
	for i := 0; i < 20; i++ {
		ema = nwdaf_store.UpdateEMA(0.5, alpha)
	}

	if ema <= threshold {
		t.Errorf("Sustained degradation EMA = %.4f, should be > %.2f", ema, threshold)
	}
}

func TestEMATrigger_RecoveryAfterDegradation(t *testing.T) {
	nwdaf_store := createTestStore()
	alpha := 0.3

	// Degradation phase
	for i := 0; i < 10; i++ {
		nwdaf_store.UpdateEMA(0.5, alpha)
	}
	emaBefore := nwdaf_store.GetEMA()

	// Recovery phase
	for i := 0; i < 10; i++ {
		nwdaf_store.UpdateEMA(0.05, alpha)
	}
	emaAfter := nwdaf_store.GetEMA()

	if emaAfter >= emaBefore {
		t.Errorf("EMA should decrease during recovery: before=%.4f, after=%.4f",
			emaBefore, emaAfter)
	}
}

func TestEMATrigger_HighAlpha_MoreSensitive(t *testing.T) {
	storeLow := createTestStore()
	storeHigh := createTestStore()

	// Same data, different alpha
	for i := 0; i < 3; i++ {
		storeLow.UpdateEMA(0.1, 0.1)
		storeHigh.UpdateEMA(0.1, 0.9)
	}

	// Spike
	emaLow := storeLow.UpdateEMA(0.9, 0.1)
	emaHigh := storeHigh.UpdateEMA(0.9, 0.9)

	if emaHigh <= emaLow {
		t.Errorf("Higher alpha should be more sensitive: low=%.4f, high=%.4f",
			emaLow, emaHigh)
	}
}

// ============================================================================
// Helpers
// ============================================================================

func createTestStore() *nwdaf_context.ModelAccuracyStore {
	return nwdaf_context.NewModelAccuracyStore("file:///test/model.pth")
}
