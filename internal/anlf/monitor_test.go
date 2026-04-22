package anlf

import (
	"math"
	"testing"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

const groupAScopeKey = "group:group-a"

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

func TestComputeMAE_PerfectPrediction(t *testing.T) {
	mae := computeMAE([]matchedPair{
		{predUl: 100, predDl: 200, actualUl: 100, actualDl: 200},
	})
	if mae != 0 {
		t.Errorf("computeMAE(perfect) = %.4f, want 0", mae)
	}
}

func TestComputeMAE_MixedError(t *testing.T) {
	mae := computeMAE([]matchedPair{
		{predUl: 105, predDl: 195, actualUl: 100, actualDl: 200},
	})
	if mae != 5 {
		t.Errorf("computeMAE(mixed) = %.4f, want 5", mae)
	}
}

func TestComputeMSE_PerfectPrediction(t *testing.T) {
	mse := computeMSE([]matchedPair{
		{predUl: 100, predDl: 200, actualUl: 100, actualDl: 200},
	})
	if mse != 0 {
		t.Errorf("computeMSE(perfect) = %.4f, want 0", mse)
	}
}

func TestComputeMSE_MixedError(t *testing.T) {
	mse := computeMSE([]matchedPair{
		{predUl: 105, predDl: 195, actualUl: 100, actualDl: 200},
	})
	if mse != 25 {
		t.Errorf("computeMSE(mixed) = %.4f, want 25", mse)
	}
}

func TestComputeWAPE_ZeroActual(t *testing.T) {
	wape := computeWAPE([]matchedPair{
		{predUl: 100, predDl: 200, actualUl: 0, actualDl: 0},
	})
	if wape != 0 {
		t.Errorf("computeWAPE(zero actual) = %.4f, want 0", wape)
	}
}

func TestComputeWAPE_MixedError(t *testing.T) {
	wape := computeWAPE([]matchedPair{
		{predUl: 105, predDl: 195, actualUl: 100, actualDl: 200},
	})
	if math.Abs(wape-(10.0/300.0)) > 1e-9 {
		t.Errorf("computeWAPE(mixed) = %.6f, want %.6f", wape, 10.0/300.0)
	}
}

func TestComputeNRMSE_ZeroActual(t *testing.T) {
	nrmse := computeNRMSE([]matchedPair{
		{predUl: 100, predDl: 200, actualUl: 0, actualDl: 0},
	})
	if nrmse != 0 {
		t.Errorf("computeNRMSE(zero actual) = %.4f, want 0", nrmse)
	}
}

func TestComputeNRMSE_MeanAbsoluteActualNormalization(t *testing.T) {
	nrmse := computeNRMSE([]matchedPair{
		{predUl: 105, predDl: 195, actualUl: 100, actualDl: 200},
	})
	if math.Abs(nrmse-(5.0/150.0)) > 1e-9 {
		t.Errorf("computeNRMSE(mixed) = %.6f, want %.6f", nrmse, 5.0/150.0)
	}
}

func TestComputeAll_IncludesCandidateMetrics(t *testing.T) {
	metrics := computeAll([]matchedPair{
		{predUl: 105, predDl: 195, actualUl: 100, actualDl: 200},
	})

	wantKeys := []string{"sMAPE", "MAE", "MSE", "WAPE", "NRMSE"}
	for _, key := range wantKeys {
		if _, ok := metrics[key]; !ok {
			t.Fatalf("computeAll() missing key %q", key)
		}
	}
}

func TestBuildAccuracyReports_PerScope(t *testing.T) {
	now := time.Now()
	scopedPairs := map[string]*scopedPairAccumulator{
		groupAScopeKey: {
			pairs: []matchedPair{
				{predUl: 110, predDl: 210, actualUl: 100, actualDl: 200},
				{predUl: 120, predDl: 220, actualUl: 100, actualDl: 200},
			},
			nwdafSubIDs: map[string]struct{}{"sub-a": {}},
			windowStart: now.Add(-2 * time.Minute),
			windowEnd:   now.Add(-time.Minute),
		},
		"supi:imsi-001": {
			pairs: []matchedPair{
				{predUl: 90, predDl: 190, actualUl: 100, actualDl: 200},
			},
			nwdafSubIDs: map[string]struct{}{"sub-b": {}},
			windowStart: now.Add(-30 * time.Second),
			windowEnd:   now,
		},
	}

	reports := buildAccuracyReports("file:///test/model.pth", scopedPairs, 7)
	if len(reports) != 2 {
		t.Fatalf("buildAccuracyReports() returned %d reports, want 2", len(reports))
	}
	if reports[0].ScopeKey != groupAScopeKey {
		t.Fatalf("reports[0].ScopeKey = %q, want %q", reports[0].ScopeKey, groupAScopeKey)
	}
	if reports[0].NwdafSubID != "sub-a" {
		t.Fatalf("reports[0].NwdafSubID = %q, want %q", reports[0].NwdafSubID, "sub-a")
	}
	if reports[0].SampleCount != 2 {
		t.Fatalf("reports[0].SampleCount = %d, want 2", reports[0].SampleCount)
	}
	if reports[0].InferenceNum != 7 {
		t.Fatalf("reports[0].InferenceNum = %d, want 7", reports[0].InferenceNum)
	}
	if reports[0].WindowStart != now.Add(-2*time.Minute) {
		t.Fatalf("reports[0].WindowStart = %v, want %v", reports[0].WindowStart, now.Add(-2*time.Minute))
	}
	if reports[0].WindowEnd != now.Add(-time.Minute) {
		t.Fatalf("reports[0].WindowEnd = %v, want %v", reports[0].WindowEnd, now.Add(-time.Minute))
	}
	if _, ok := reports[0].Metrics["MAE"]; !ok {
		t.Fatal("reports[0].Metrics missing MAE")
	}
	if reports[1].ScopeKey != "supi:imsi-001" {
		t.Fatalf("reports[1].ScopeKey = %q, want %q", reports[1].ScopeKey, "supi:imsi-001")
	}
}

func TestBuildAccuracyReports_MultipleSubscriptionsClearNwdafSubID(t *testing.T) {
	now := time.Now()
	scopedPairs := map[string]*scopedPairAccumulator{
		groupAScopeKey: {
			pairs: []matchedPair{
				{predUl: 110, predDl: 210, actualUl: 100, actualDl: 200},
			},
			nwdafSubIDs: map[string]struct{}{
				"sub-a": {},
				"sub-b": {},
			},
			windowStart: now,
			windowEnd:   now,
		},
	}

	reports := buildAccuracyReports("file:///test/model.pth", scopedPairs, 3)
	if len(reports) != 1 {
		t.Fatalf("buildAccuracyReports() returned %d reports, want 1", len(reports))
	}
	if reports[0].NwdafSubID != "" {
		t.Fatalf("reports[0].NwdafSubID = %q, want empty string", reports[0].NwdafSubID)
	}
}

func TestRecordScopedPair_TracksWindowAndSubID(t *testing.T) {
	now := time.Now()
	scopedPairs := make(map[string]*scopedPairAccumulator)
	recordScopedPair(scopedPairs, nwdaf_context.PredictionRecord{
		ScopeKey:   "group:group-a",
		NwdafSubId: "sub-a",
		TargetTime: now,
	}, matchedPair{predUl: 100, predDl: 200, actualUl: 100, actualDl: 200})
	recordScopedPair(scopedPairs, nwdaf_context.PredictionRecord{
		ScopeKey:   "group:group-a",
		NwdafSubId: "sub-a",
		TargetTime: now.Add(10 * time.Second),
	}, matchedPair{predUl: 110, predDl: 210, actualUl: 100, actualDl: 200})

	acc := scopedPairs["group:group-a"]
	if acc == nil {
		t.Fatal("recordScopedPair() did not create accumulator")
	}
	if len(acc.pairs) != 2 {
		t.Fatalf("len(acc.pairs) = %d, want 2", len(acc.pairs))
	}
	if acc.windowStart != now {
		t.Fatalf("acc.windowStart = %v, want %v", acc.windowStart, now)
	}
	if acc.windowEnd != now.Add(10*time.Second) {
		t.Fatalf("acc.windowEnd = %v, want %v", acc.windowEnd, now.Add(10*time.Second))
	}
	if _, ok := acc.nwdafSubIDs["sub-a"]; !ok {
		t.Fatal("acc.nwdafSubIDs missing sub-a")
	}
}
