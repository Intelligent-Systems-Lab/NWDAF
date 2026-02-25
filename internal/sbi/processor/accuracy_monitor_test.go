package processor

import (
	"testing"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

// ============================================================================
// computeNRMSE — Unit Tests
// ============================================================================

func TestComputeNRMSE_Empty(t *testing.T) {
	nrmse := computeNRMSE(nil)
	if nrmse != 0 {
		t.Errorf("computeNRMSE(nil) = %.4f, want 0", nrmse)
	}
}

func TestComputeNRMSE_PerfectPrediction(t *testing.T) {
	pairs := []matchedPair{
		{predUl: 100, predDl: 200, actualUl: 100, actualDl: 200},
		{predUl: 300, predDl: 400, actualUl: 300, actualDl: 400},
	}
	nrmse := computeNRMSE(pairs)
	if nrmse != 0 {
		t.Errorf("Perfect prediction NRMSE = %.4f, want 0", nrmse)
	}
}

func TestComputeNRMSE_SmallError(t *testing.T) {
	pairs := []matchedPair{
		{predUl: 105, predDl: 195, actualUl: 100, actualDl: 200},
	}
	nrmse := computeNRMSE(pairs)
	if nrmse <= 0 || nrmse >= 0.1 {
		t.Errorf("Small error NRMSE = %.4f, expected small positive value", nrmse)
	}
}

func TestComputeNRMSE_LargeError(t *testing.T) {
	pairs := []matchedPair{
		{predUl: 1000, predDl: 1000, actualUl: 100, actualDl: 100},
	}
	nrmse := computeNRMSE(pairs)
	if nrmse < 1.0 {
		t.Errorf("Large error NRMSE = %.4f, expected >= 1.0", nrmse)
	}
}

func TestComputeNRMSE_ZeroActual(t *testing.T) {
	// All actual values are 0 but predictions are non-zero
	pairs := []matchedPair{
		{predUl: 100, predDl: 200, actualUl: 0, actualDl: 0},
	}
	nrmse := computeNRMSE(pairs)
	if nrmse != 1.0 {
		t.Errorf("Zero actual NRMSE = %.4f, want 1.0", nrmse)
	}
}

func TestComputeNRMSE_ZeroActualZeroPred(t *testing.T) {
	// Both actual and predicted are 0
	pairs := []matchedPair{
		{predUl: 0, predDl: 0, actualUl: 0, actualDl: 0},
	}
	nrmse := computeNRMSE(pairs)
	if nrmse != 0 {
		t.Errorf("Zero-zero NRMSE = %.4f, want 0", nrmse)
	}
}

func TestComputeNRMSE_MultipleSymmetric(t *testing.T) {
	// Symmetric errors should produce consistent NRMSE
	pairs := []matchedPair{
		{predUl: 110, predDl: 210, actualUl: 100, actualDl: 200},
		{predUl: 90, predDl: 190, actualUl: 100, actualDl: 200},
	}
	nrmse := computeNRMSE(pairs)
	if nrmse <= 0 || nrmse >= 0.15 {
		t.Errorf("Symmetric error NRMSE = %.4f, expected small positive", nrmse)
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
