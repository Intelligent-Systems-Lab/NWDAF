package mtlf

import (
	"testing"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

// ============================================================================
// Trigger Strategy — Consecutive Breaches (via ModelAccuracyStore)
// ============================================================================

func TestConsecutiveTrigger_NoRetrain_BelowThreshold(t *testing.T) {
	store := createTestStore()

	store.ResetBreaches()
	count := store.IncrementBreaches()
	if count != 1 {
		t.Errorf("After reset+increment, count = %d, want 1", count)
	}
}

func TestConsecutiveTrigger_RetainAfterN(t *testing.T) {
	store := createTestStore()
	required := 3

	var triggered bool
	for i := 0; i < required; i++ {
		count := store.IncrementBreaches()
		if count >= required {
			triggered = true
		}
	}
	if !triggered {
		t.Error("Should trigger after 3 consecutive breaches")
	}
}

func TestConsecutiveTrigger_ResetOnGoodCheck(t *testing.T) {
	store := createTestStore()

	store.IncrementBreaches()
	store.IncrementBreaches()
	store.ResetBreaches()

	store.IncrementBreaches()
	count := store.IncrementBreaches()

	if count >= 3 {
		t.Errorf("Should not reach 3 after reset, got %d", count)
	}
}

// ============================================================================
// Trigger Strategy — EMA (via ModelAccuracyStore)
// ============================================================================

func TestEMATrigger_SingleSpike_NoTrigger(t *testing.T) {
	store := createTestStore()
	threshold := 0.3
	alpha := 0.2

	for i := 0; i < 20; i++ {
		store.UpdateEMA(0.1, alpha)
	}
	ema := store.UpdateEMA(0.9, alpha)

	if ema > threshold {
		t.Errorf("Single spike EMA = %.4f, should be <= %.2f", ema, threshold)
	}
}

func TestEMATrigger_SustainedDegradation_Triggers(t *testing.T) {
	store := createTestStore()
	threshold := 0.3
	alpha := 0.3

	var ema float64
	for i := 0; i < 20; i++ {
		ema = store.UpdateEMA(0.5, alpha)
	}

	if ema <= threshold {
		t.Errorf("Sustained degradation EMA = %.4f, should be > %.2f", ema, threshold)
	}
}

func TestEMATrigger_RecoveryAfterDegradation(t *testing.T) {
	store := createTestStore()
	alpha := 0.3

	for i := 0; i < 10; i++ {
		store.UpdateEMA(0.5, alpha)
	}
	emaBefore := store.GetEMA()

	for i := 0; i < 10; i++ {
		store.UpdateEMA(0.05, alpha)
	}
	emaAfter := store.GetEMA()

	if emaAfter >= emaBefore {
		t.Errorf("EMA should decrease during recovery: before=%.4f, after=%.4f",
			emaBefore, emaAfter)
	}
}

func TestEMATrigger_HighAlpha_MoreSensitive(t *testing.T) {
	storeLow := createTestStore()
	storeHigh := createTestStore()

	for i := 0; i < 3; i++ {
		storeLow.UpdateEMA(0.1, 0.1)
		storeHigh.UpdateEMA(0.1, 0.9)
	}

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
