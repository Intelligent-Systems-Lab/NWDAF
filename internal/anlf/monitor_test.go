package anlf

import (
	"context"
	"math"
	"testing"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
)

const groupAScopeKey = "group:group-a"

type testNwdafApp struct {
	ctx context.Context
}

func (a testNwdafApp) CancelContext() context.Context {
	return a.ctx
}

func setTestMonitorConfig(
	t *testing.T,
	samplingInterval int,
) *factory.AccuracyMonitorConfig {
	t.Helper()

	oldCfg := factory.NwdafConfig
	factory.NwdafConfig = &factory.Config{
		Configuration: &factory.Configuration{
			Mtlf: &factory.MtlfConfig{
				Enabled: true,
				AccuracyMonitor: &factory.AccuracyMonitorConfig{
					Enabled: true,
				},
			},
			Analytics: &factory.AnalyticsConfig{
				UeCommunication: &factory.ModelParams{
					SamplingInterval: samplingInterval,
				},
			},
		},
	}
	t.Cleanup(func() {
		factory.NwdafConfig = oldCfg
	})

	return factory.NwdafConfig.Configuration.Mtlf.AccuracyMonitor
}

func addGroundTruthRecord(
	ctx *nwdaf_context.NWDAFContext,
	nwdafSubID, corrID, ip string,
	targetTime time.Time,
	actualUL, actualDL int64,
) {
	ctx.AddNwdafSubResource(nwdafSubID, nwdaf_context.NwdafSubResource{
		CorrelationId: corrID,
	})
	setRawUpfData(ctx, corrID, ip, []nwdaf_context.UpfDataPoint{
		makeDP(targetTime.Unix(), actualUL, actualDL),
	})
}

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
	if reports[0].TrafficScale != 150 {
		t.Fatalf("reports[0].TrafficScale = %.2f, want 150", reports[0].TrafficScale)
	}
	if reports[0].PredictedTrafficScale != 165 {
		t.Fatalf("reports[0].PredictedTrafficScale = %.2f, want 165", reports[0].PredictedTrafficScale)
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
	if reports[1].TrafficScale != 150 {
		t.Fatalf("reports[1].TrafficScale = %.2f, want 150", reports[1].TrafficScale)
	}
	if reports[1].PredictedTrafficScale != 140 {
		t.Fatalf("reports[1].PredictedTrafficScale = %.2f, want 140", reports[1].PredictedTrafficScale)
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

func TestCheckModelAccuracy_LegacyDeviationUsesAllMatchedPairs(t *testing.T) {
	ctx := setupCtx(t)
	accCfg := setTestMonitorConfig(t, 5)
	accCfg.MinSamples = 1

	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := NewAnlfService(testNwdafApp{ctx: cancelCtx})
	store := nwdaf_context.NewModelAccuracyStore("file:///test/model.pth")
	now := time.Now()

	target1 := now.Add(-30 * time.Second)
	target2 := now.Add(-25 * time.Second)
	addGroundTruthRecord(ctx, "sub-1", "corr-1", "10.0.0.1", target1, 100, 200)
	addGroundTruthRecord(ctx, "sub-2", "corr-2", "10.0.0.2", target2, 300, 400)

	store.AddPrediction(nwdaf_context.PredictionRecord{
		ModelUrl:    "file:///test/model.pth",
		PredictedAt: target1.Add(-5 * time.Second),
		TargetTime:  target1,
		PredUlVol:   110,
		PredDlVol:   210,
		NwdafSubId:  "sub-1",
		ScopeKey:    groupAScopeKey,
	})
	store.AddPrediction(nwdaf_context.PredictionRecord{
		ModelUrl:    "file:///test/model.pth",
		PredictedAt: target2.Add(-5 * time.Second),
		TargetTime:  target2,
		PredUlVol:   330,
		PredDlVol:   440,
		NwdafSubId:  "sub-2",
		ScopeKey:    "",
	})

	var gotDeviation float64
	var gotDeviationCalls int
	var gotReports []AccuracyReport
	service.SetOnDeviationReport(func(modelURL string, deviation float64, store *nwdaf_context.ModelAccuracyStore) {
		gotDeviationCalls++
		gotDeviation = deviation
	})
	service.SetOnAccuracyReports(func(modelURL string, reports []AccuracyReport, store *nwdaf_context.ModelAccuracyStore) {
		gotReports = append([]AccuracyReport(nil), reports...)
	})

	service.checkModelAccuracy("file:///test/model.pth", store, accCfg)

	if gotDeviationCalls != 1 {
		t.Fatalf("deviation callback calls = %d, want 1", gotDeviationCalls)
	}

	wantDeviation := computeSMAPE([]matchedPair{
		{predUl: 110, predDl: 210, actualUl: 100, actualDl: 200},
		{predUl: 330, predDl: 440, actualUl: 300, actualDl: 400},
	})
	if math.Abs(gotDeviation-wantDeviation) > 1e-9 {
		t.Fatalf("deviation = %.10f, want %.10f", gotDeviation, wantDeviation)
	}
	if math.Abs(store.GetDeviation()-wantDeviation) > 1e-9 {
		t.Fatalf("store.GetDeviation() = %.10f, want %.10f", store.GetDeviation(), wantDeviation)
	}

	if len(gotReports) != 1 {
		t.Fatalf("len(gotReports) = %d, want 1", len(gotReports))
	}
	if gotReports[0].ScopeKey != groupAScopeKey {
		t.Fatalf("gotReports[0].ScopeKey = %q, want %q", gotReports[0].ScopeKey, groupAScopeKey)
	}
	if gotReports[0].SampleCount != 1 {
		t.Fatalf("gotReports[0].SampleCount = %d, want 1", gotReports[0].SampleCount)
	}
}

func TestCheckModelAccuracy_MinSamplesSkipsCallbacks(t *testing.T) {
	ctx := setupCtx(t)
	accCfg := setTestMonitorConfig(t, 5)
	accCfg.MinSamples = 2

	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := NewAnlfService(testNwdafApp{ctx: cancelCtx})
	store := nwdaf_context.NewModelAccuracyStore("file:///test/model.pth")
	target := time.Now().Add(-30 * time.Second)
	addGroundTruthRecord(ctx, "sub-1", "corr-1", "10.0.0.1", target, 100, 200)

	store.AddPrediction(nwdaf_context.PredictionRecord{
		ModelUrl:    "file:///test/model.pth",
		PredictedAt: target.Add(-5 * time.Second),
		TargetTime:  target,
		PredUlVol:   110,
		PredDlVol:   210,
		NwdafSubId:  "sub-1",
		ScopeKey:    groupAScopeKey,
	})

	var deviationCalls int
	var reportCalls int
	service.SetOnDeviationReport(func(modelURL string, deviation float64, store *nwdaf_context.ModelAccuracyStore) {
		deviationCalls++
	})
	service.SetOnAccuracyReports(func(modelURL string, reports []AccuracyReport, store *nwdaf_context.ModelAccuracyStore) {
		reportCalls++
	})

	service.checkModelAccuracy("file:///test/model.pth", store, accCfg)

	if deviationCalls != 0 {
		t.Fatalf("deviation callback calls = %d, want 0", deviationCalls)
	}
	if reportCalls != 0 {
		t.Fatalf("report callback calls = %d, want 0", reportCalls)
	}
}

func TestCheckModelAccuracy_MinSamplesAppliesPerScope(t *testing.T) {
	ctx := setupCtx(t)
	accCfg := setTestMonitorConfig(t, 5)
	accCfg.MinSamples = 2

	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := NewAnlfService(testNwdafApp{ctx: cancelCtx})
	store := nwdaf_context.NewModelAccuracyStore("file:///test/model.pth")
	now := time.Now()
	targetA1 := now.Add(-50 * time.Second)
	targetA2 := now.Add(-40 * time.Second)
	targetB1 := now.Add(-30 * time.Second)

	addGroundTruthRecord(ctx, "sub-a1", "corr-a1", "10.0.0.1", targetA1, 100, 200)
	addGroundTruthRecord(ctx, "sub-a2", "corr-a2", "10.0.0.2", targetA2, 150, 250)
	addGroundTruthRecord(ctx, "sub-b1", "corr-b1", "10.0.0.3", targetB1, 300, 400)

	store.AddPrediction(nwdaf_context.PredictionRecord{
		ModelUrl:    "file:///test/model.pth",
		PredictedAt: targetA1.Add(-5 * time.Second),
		TargetTime:  targetA1,
		PredUlVol:   110,
		PredDlVol:   210,
		NwdafSubId:  "sub-a1",
		ScopeKey:    groupAScopeKey,
	})
	store.AddPrediction(nwdaf_context.PredictionRecord{
		ModelUrl:    "file:///test/model.pth",
		PredictedAt: targetA2.Add(-5 * time.Second),
		TargetTime:  targetA2,
		PredUlVol:   140,
		PredDlVol:   260,
		NwdafSubId:  "sub-a2",
		ScopeKey:    groupAScopeKey,
	})
	store.AddPrediction(nwdaf_context.PredictionRecord{
		ModelUrl:    "file:///test/model.pth",
		PredictedAt: targetB1.Add(-5 * time.Second),
		TargetTime:  targetB1,
		PredUlVol:   330,
		PredDlVol:   440,
		NwdafSubId:  "sub-b1",
		ScopeKey:    "supi:imsi-001",
	})

	var gotReports []AccuracyReport
	var deviationCalls int
	service.SetOnAccuracyReports(func(modelURL string, reports []AccuracyReport, store *nwdaf_context.ModelAccuracyStore) {
		gotReports = append([]AccuracyReport(nil), reports...)
	})
	service.SetOnDeviationReport(func(modelURL string, deviation float64, store *nwdaf_context.ModelAccuracyStore) {
		deviationCalls++
	})

	service.checkModelAccuracy("file:///test/model.pth", store, accCfg)

	if len(gotReports) != 1 {
		t.Fatalf("len(gotReports) = %d, want 1 eligible scope report", len(gotReports))
	}
	if gotReports[0].ScopeKey != groupAScopeKey {
		t.Fatalf("gotReports[0].ScopeKey = %q, want %q", gotReports[0].ScopeKey, groupAScopeKey)
	}
	if gotReports[0].SampleCount != 2 {
		t.Fatalf("gotReports[0].SampleCount = %d, want 2", gotReports[0].SampleCount)
	}
	if deviationCalls != 1 {
		t.Fatalf(
			"deviation callback calls = %d, want 1 because total matched pairs still satisfy legacy gate",
			deviationCalls,
		)
	}
}

func TestCheckModelAccuracy_LegacyDeviationIndependentOfPersistence(t *testing.T) {
	ctx := setupCtx(t)
	accCfg := setTestMonitorConfig(t, 5)
	accCfg.MinSamples = 1

	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := NewAnlfService(testNwdafApp{ctx: cancelCtx})
	store := nwdaf_context.NewModelAccuracyStore("file:///test/model.pth")
	target := time.Now().Add(-30 * time.Second)
	addGroundTruthRecord(ctx, "sub-1", "corr-1", "10.0.0.1", target, 100, 200)

	store.AddPrediction(nwdaf_context.PredictionRecord{
		ModelUrl:    "file:///test/model.pth",
		PredictedAt: target.Add(-5 * time.Second),
		TargetTime:  target,
		PredUlVol:   110,
		PredDlVol:   210,
		NwdafSubId:  "sub-1",
		ScopeKey:    groupAScopeKey,
	})

	var deviationCalls int
	var reportCalls int
	service.SetOnDeviationReport(func(modelURL string, deviation float64, store *nwdaf_context.ModelAccuracyStore) {
		deviationCalls++
	})
	service.SetOnAccuracyReports(func(modelURL string, reports []AccuracyReport, store *nwdaf_context.ModelAccuracyStore) {
		reportCalls++
	})

	service.checkModelAccuracy("file:///test/model.pth", store, accCfg)

	if deviationCalls != 1 {
		t.Fatalf("deviation callback calls = %d, want 1", deviationCalls)
	}
	if reportCalls != 1 {
		t.Fatalf("report callback calls = %d, want 1", reportCalls)
	}
}

func TestAcquireStartupWarmupDuration_OnlyOnce(t *testing.T) {
	service := NewAnlfService(testNwdafApp{ctx: context.Background()})
	accCfg := &factory.AccuracyMonitorConfig{WarmupDuration: 7}

	if got := service.acquireStartupWarmupDuration(accCfg); got != 7 {
		t.Fatalf("first acquireStartupWarmupDuration() = %d, want 7", got)
	}
	if got := service.acquireStartupWarmupDuration(accCfg); got != 0 {
		t.Fatalf("second acquireStartupWarmupDuration() = %d, want 0", got)
	}
}

func TestAcquireStartupWarmupDuration_UsesDefaultOnce(t *testing.T) {
	service := NewAnlfService(testNwdafApp{ctx: context.Background()})
	accCfg := &factory.AccuracyMonitorConfig{}

	if got := service.acquireStartupWarmupDuration(accCfg); got != 120 {
		t.Fatalf("first acquireStartupWarmupDuration() = %d, want 120", got)
	}
	if got := service.acquireStartupWarmupDuration(accCfg); got != 0 {
		t.Fatalf("second acquireStartupWarmupDuration() = %d, want 0", got)
	}
}

func TestRunModelAccuracyLoop_ZeroWarmupKeepsPredictionsUntilTicker(t *testing.T) {
	service := NewAnlfService(testNwdafApp{ctx: context.Background()})
	store := nwdaf_context.NewModelAccuracyStore("file:///test/model.pth")
	target := time.Now().Add(-30 * time.Second)
	store.AddPrediction(nwdaf_context.PredictionRecord{
		ModelUrl:       "file:///test/model.pth",
		PredictedAt:    target.Add(-5 * time.Second),
		TargetTime:     target,
		TargetSlotTime: target,
		PredUlVol:      110,
		PredDlVol:      210,
		NwdafSubId:     "sub-1",
		ScopeKey:       groupAScopeKey,
	})

	accCfg := &factory.AccuracyMonitorConfig{
		CheckInterval: 30,
		MinSamples:    1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		service.runModelAccuracyLoop(ctx, "file:///test/model.pth", store, accCfg, 0)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runModelAccuracyLoop did not exit after cancel")
	}

	if got := len(store.SnapshotPredictions()); got != 1 {
		t.Fatalf("remaining predictions after zero-warmup loop = %d, want 1", got)
	}
	if got := store.GetAndResetInferenceNum(); got != 1 {
		t.Fatalf("remaining inference count after zero-warmup loop = %d, want 1", got)
	}
}

func TestLookupGroundTruth_RejectsAdjacentSlotWithinLegacyNearestWindow(t *testing.T) {
	ctx := setupCtx(t)
	setTestMonitorConfig(t, 10)

	service := NewAnlfService(testNwdafApp{ctx: context.Background()})
	target := snappedTs(100)
	// 6 seconds late is still within the old ±10s nearest window, but should map
	// to the next slot under the new slot-equality pairing.
	addGroundTruthRecord(ctx, "sub-1", "corr-1", "10.0.0.1", target.Add(6*time.Second), 100, 200)

	got := service.lookupGroundTruth(ctx, nwdaf_context.PredictionRecord{
		NwdafSubId:     "sub-1",
		TargetTime:     target,
		TargetSlotTime: target,
	})
	if got != nil {
		t.Fatalf("lookupGroundTruth() = %+v, want nil for adjacent-slot actual", got)
	}
}

func TestCheckModelAccuracy_RetriesPendingPredictionBeforeDiscard(t *testing.T) {
	setupCtx(t)
	accCfg := setTestMonitorConfig(t, 5)
	accCfg.CheckInterval = 20
	accCfg.MinSamples = 1

	service := NewAnlfService(testNwdafApp{ctx: context.Background()})
	store := nwdaf_context.NewModelAccuracyStore("file:///test/model.pth")
	target := time.Now().Add(-30 * time.Second)
	store.AddPrediction(nwdaf_context.PredictionRecord{
		ModelUrl:       "file:///test/model.pth",
		PredictedAt:    target.Add(-5 * time.Second),
		TargetTime:     target,
		TargetSlotTime: target,
		PredUlVol:      110,
		PredDlVol:      210,
		NwdafSubId:     "sub-1",
		ScopeKey:       groupAScopeKey,
	})

	service.checkModelAccuracy("file:///test/model.pth", store, accCfg)
	snapshot := store.SnapshotPredictions()
	if len(snapshot) != 1 {
		t.Fatalf("len(SnapshotPredictions()) after first miss = %d, want 1", len(snapshot))
	}
	if snapshot[0].MissCount != 1 {
		t.Fatalf("snapshot[0].MissCount after first miss = %d, want 1", snapshot[0].MissCount)
	}

	service.checkModelAccuracy("file:///test/model.pth", store, accCfg)
	if got := len(store.SnapshotPredictions()); got != 0 {
		t.Fatalf("len(SnapshotPredictions()) after discard threshold = %d, want 0", got)
	}
}

func TestCheckModelAccuracy_LateGroundTruthMatchesOnLaterRound(t *testing.T) {
	ctx := setupCtx(t)
	accCfg := setTestMonitorConfig(t, 5)
	accCfg.CheckInterval = 20
	accCfg.MinSamples = 1

	service := NewAnlfService(testNwdafApp{ctx: context.Background()})
	store := nwdaf_context.NewModelAccuracyStore("file:///test/model.pth")
	target := time.Now().Add(-30 * time.Second)
	store.AddPrediction(nwdaf_context.PredictionRecord{
		ModelUrl:       "file:///test/model.pth",
		PredictedAt:    target.Add(-5 * time.Second),
		TargetTime:     target,
		TargetSlotTime: target,
		PredUlVol:      110,
		PredDlVol:      210,
		NwdafSubId:     "sub-1",
		ScopeKey:       groupAScopeKey,
	})

	service.checkModelAccuracy("file:///test/model.pth", store, accCfg)
	if got := len(store.SnapshotPredictions()); got != 1 {
		t.Fatalf("len(SnapshotPredictions()) after first miss = %d, want 1", got)
	}

	addGroundTruthRecord(ctx, "sub-1", "corr-1", "10.0.0.1", target, 100, 200)

	var deviationCalls int
	service.SetOnDeviationReport(func(modelURL string, deviation float64, store *nwdaf_context.ModelAccuracyStore) {
		deviationCalls++
	})

	service.checkModelAccuracy("file:///test/model.pth", store, accCfg)
	if got := len(store.SnapshotPredictions()); got != 0 {
		t.Fatalf("len(SnapshotPredictions()) after late ground truth match = %d, want 0", got)
	}
	if deviationCalls != 1 {
		t.Fatalf("deviation callback calls = %d, want 1", deviationCalls)
	}
}
