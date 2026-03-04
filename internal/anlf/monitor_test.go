package anlf

import (
	"testing"
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
