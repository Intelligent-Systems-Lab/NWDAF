package mtlf

import (
	"testing"
	"time"

	anlf "github.com/free5gc/nwdaf/internal/mtlf/contract"
	"github.com/free5gc/nwdaf/pkg/factory"
)

const (
	testModelURL  = "file:///test/model.pth"
	testScopeKey  = "group:test"
	testScopeKeyB = "group:test-b"
)

func setTestAccuracyMonitorConfig(t *testing.T, cfg *factory.AccuracyMonitorConfig) *factory.Config {
	t.Helper()

	return &factory.Config{
		Configuration: &factory.Configuration{
			Mtlf: &factory.MtlfConfig{
				Enabled:        true,
				AccuracyPolicy: cfg,
			},
		},
	}
}

func testAccuracyReport(modelURL, scopeKey string, current float64) anlf.AccuracyReport {
	return testAccuracyReportWithMetrics(modelURL, scopeKey, map[string]float64{"MAE": current}, 0)
}

func testAccuracyReportWithMetrics(
	modelURL, scopeKey string,
	metrics map[string]float64,
	trafficScale float64,
) anlf.AccuracyReport {
	return testAccuracyReportWithMetricsAndPredicted(modelURL, scopeKey, metrics, trafficScale, 0)
}

func testAccuracyReportWithMetricsAndPredicted(
	modelURL, scopeKey string,
	metrics map[string]float64,
	trafficScale float64,
	predictedTrafficScale float64,
) anlf.AccuracyReport {
	return anlf.AccuracyReport{
		ModelURL:              modelURL,
		ScopeKey:              scopeKey,
		Metrics:               metrics,
		TrafficScale:          trafficScale,
		PredictedTrafficScale: predictedTrafficScale,
		SampleCount:           5,
	}
}

func prefillScope(scope *ScopeState, values ...float64) {
	now := time.Now()
	for _, value := range values {
		observation := ScopeObservation{
			Timestamp:    now,
			SampleCount:  5,
			TrafficScale: 2048,
			Metrics: map[string]float64{
				"MAE": value,
			},
		}
		scope.RecordObservation(observation)
		scope.RecordDegradationReference(observation)
	}
}

func TestSignalState(t *testing.T) {
	if got := signalState(false, true); got != "skipped" {
		t.Fatalf("signalState(false, true) = %q, want %q", got, "skipped")
	}
	if got := signalState(true, true); got != "true" {
		t.Fatalf("signalState(true, true) = %q, want %q", got, "true")
	}
	if got := signalState(true, false); got != "false" {
		t.Fatalf("signalState(true, false) = %q, want %q", got, "false")
	}
}

func TestComposeHitReason(t *testing.T) {
	if got := composeHitReason(false, false, false); got != "none" {
		t.Fatalf("composeHitReason(false, false, false) = %q, want %q", got, "none")
	}
	if got := composeHitReason(true, false, false); got != "degradation" {
		t.Fatalf("composeHitReason(true, false, false) = %q, want %q", got, "degradation")
	}
	if got := composeHitReason(false, true, true); got != "chronic+low_traffic_overprediction" {
		t.Fatalf("composeHitReason(false, true, true) = %q, want %q", got, "chronic+low_traffic_overprediction")
	}
}

func TestHandleAccuracyReports_ColdStartBuildsBaselineWithoutTrigger(t *testing.T) {
	cfg := setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:             true,
		PrimaryMetric:       "MAE",
		RecentBufferSize:    5,
		MinBufferSamples:    5,
		MinStd:              1,
		FixedFloor:          100,
		ZScoreThreshold:     3,
		ConsecutiveBreaches: 2,
	})

	m := newTestMtlfService(cfg)
	store := newTestRetrainingState()
	modelURL := testModelURL
	scopeKey := testScopeKey

	triggered := 0
	m.onRetrainTriggered = func(modelURL string, store retrainingState) {
		triggered++
	}

	report := testAccuracyReport(modelURL, scopeKey, 150)

	m.HandleAccuracyReports(modelURL, []anlf.AccuracyReport{report}, store)
	scope := m.stateStore.GetScope(modelURL, scopeKey)
	if scope == nil {
		t.Fatal("scope state was not created")
	}
	if got := scope.BreachCount(); got != 0 {
		t.Fatalf("BreachCount() after first cold-start report = %d, want 0", got)
	}
	if got := scope.SampleCount("MAE"); got != 1 {
		t.Fatalf("SampleCount() after first cold-start report = %d, want 1", got)
	}
	if triggered != 0 {
		t.Fatalf("triggered = %d, want 0 while baseline is not ready", triggered)
	}

	for i := 0; i < 3; i++ {
		m.HandleAccuracyReports(modelURL, []anlf.AccuracyReport{report}, store)
	}

	if got := scope.BreachCount(); got != 0 {
		t.Fatalf("BreachCount() after baseline fill = %d, want 0", got)
	}
	if got := scope.SampleCount("MAE"); got != 4 {
		t.Fatalf("SampleCount() after baseline fill = %d, want 4", got)
	}
	if triggered != 0 {
		t.Fatalf("triggered = %d, want 0 before baseline becomes ready", triggered)
	}
	if store.IsRetraining() {
		t.Fatal("store.IsRetraining() = true, want false while baseline is not ready")
	}
}

func TestHandleAccuracyReports_BaselineReadyRequiresRelGate(t *testing.T) {
	cfg := setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:             true,
		PrimaryMetric:       "MAE",
		RecentBufferSize:    5,
		MinBufferSamples:    3,
		MinStd:              100,
		FixedFloor:          100,
		ZScoreThreshold:     3,
		ConsecutiveBreaches: 2,
	})

	m := newTestMtlfService(cfg)
	store := newTestRetrainingState()
	modelURL := testModelURL
	scopeKey := testScopeKey

	scope := m.stateStore.GetOrCreateScope(modelURL, scopeKey, 5, 2)
	prefillScope(scope, 150, 150, 150)

	m.HandleAccuracyReports(modelURL, []anlf.AccuracyReport{
		testAccuracyReport(modelURL, scopeKey, 160),
	}, store)

	if got := scope.BreachCount(); got != 0 {
		t.Fatalf("BreachCount() = %d, want 0 when relative gate fails", got)
	}
	if store.IsRetraining() {
		t.Fatal("store.IsRetraining() = true, want false when relative gate fails")
	}
}

func TestHandleAccuracyReports_TriggersOnlyAfterBaselineReady(t *testing.T) {
	cfg := setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:             true,
		PrimaryMetric:       "MAE",
		RecentBufferSize:    5,
		MinBufferSamples:    3,
		MinStd:              1,
		FixedFloor:          100,
		ZScoreThreshold:     3,
		ConsecutiveBreaches: 2,
	})

	m := newTestMtlfService(cfg)
	store := newTestRetrainingState()
	triggered := 0
	m.onRetrainTriggered = func(modelURL string, store retrainingState) {
		triggered++
	}

	baseline := testAccuracyReport(testModelURL, testScopeKey, 150)
	high := testAccuracyReport(testModelURL, testScopeKey, 300)
	higher := testAccuracyReport(testModelURL, testScopeKey, 700)
	for i := 0; i < 2; i++ {
		m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{baseline}, store)
	}

	scope := m.stateStore.GetScope(testModelURL, testScopeKey)
	if scope == nil {
		t.Fatal("scope state was not created")
	}
	if got := scope.BreachCount(); got != 0 {
		t.Fatalf("BreachCount() before baseline ready = %d, want 0", got)
	}
	if triggered != 0 {
		t.Fatalf("triggered before baseline ready = %d, want 0", triggered)
	}

	m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{baseline}, store)
	if got := scope.BreachCount(); got != 0 {
		t.Fatalf("BreachCount() when baseline just became ready = %d, want 0", got)
	}
	if triggered != 0 {
		t.Fatalf("triggered when baseline just became ready = %d, want 0", triggered)
	}

	m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{high}, store)
	if got := scope.BreachCount(); got != 1 {
		t.Fatalf("BreachCount() after first baseline-ready breach = %d, want 1", got)
	}
	if triggered != 0 {
		t.Fatalf("triggered after first baseline-ready breach = %d, want 0", triggered)
	}

	m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{higher}, store)
	if triggered != 1 {
		t.Fatalf("triggered = %d, want 1 after second baseline-ready breach", triggered)
	}
	if !store.IsRetraining() {
		t.Fatal("store.IsRetraining() = false, want true after retrain trigger")
	}
}

func TestHandleAccuracyReports_DecisionWindowRetainsRecentHits(t *testing.T) {
	cfg := setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:              true,
		PrimaryMetric:        "MAE",
		RecentBufferSize:     5,
		MinBufferSamples:     3,
		MinStd:               1,
		FixedFloor:           100,
		ZScoreThreshold:      3,
		DecisionWindowSize:   3,
		RequiredHitsInWindow: 3,
	})

	m := newTestMtlfService(cfg)
	store := newTestRetrainingState()
	modelURL := testModelURL
	scopeKey := testScopeKey

	scope := m.stateStore.GetOrCreateScope(modelURL, scopeKey, 5, 3)
	prefillScope(scope, 150, 150, 150)

	bad := testAccuracyReport(modelURL, scopeKey, 300)
	badAgain := testAccuracyReport(modelURL, scopeKey, 400)
	good := testAccuracyReport(modelURL, scopeKey, 150)

	m.HandleAccuracyReports(modelURL, []anlf.AccuracyReport{bad}, store)
	if got := scope.BreachCount(); got != 1 {
		t.Fatalf("BreachCount() after bad round = %d, want 1", got)
	}

	m.HandleAccuracyReports(modelURL, []anlf.AccuracyReport{good}, store)
	if got := scope.BreachCount(); got != 1 {
		t.Fatalf("BreachCount() after good round = %d, want 1 retained hit in window", got)
	}

	m.HandleAccuracyReports(modelURL, []anlf.AccuracyReport{badAgain}, store)
	if got := scope.BreachCount(); got != 2 {
		t.Fatalf("BreachCount() after second bad round = %d, want 2", got)
	}
}

func TestHandleAccuracyReports_DegradationLowTrafficSkipsWindowUpdate(t *testing.T) {
	cfg := setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:              true,
		PrimaryMetric:        "MAE",
		RecentBufferSize:     5,
		MinBufferSamples:     3,
		MinStd:               1,
		FixedFloor:           100,
		ZScoreThreshold:      3,
		DecisionWindowSize:   3,
		RequiredHitsInWindow: 2,
		DegradationPolicy: &factory.DegradationPolicyConfig{
			MinDecisionTrafficScale: 1024,
		},
	})

	m := newTestMtlfService(cfg)
	store := newTestRetrainingState()
	scope := m.stateStore.GetOrCreateScope(testModelURL, testScopeKey, 5, 3)
	prefillScope(scope, 150, 150, 150)

	badLowTraffic := testAccuracyReportWithMetrics(testModelURL, testScopeKey, map[string]float64{"MAE": 700}, 100)
	badNormalTraffic := testAccuracyReportWithMetrics(testModelURL, testScopeKey, map[string]float64{"MAE": 1200}, 2048)

	m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{badLowTraffic}, store)
	if got := scope.BreachCount(); got != 0 {
		t.Fatalf("BreachCount() after low-traffic degradation round = %d, want 0", got)
	}

	m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{badNormalTraffic}, store)
	if got := scope.BreachCount(); got != 1 {
		t.Fatalf("BreachCount() after eligible degradation round = %d, want 1", got)
	}
}

func TestHandleAccuracyReports_DegradationSignalDoesNotPolluteReferenceBuffer(t *testing.T) {
	cfg := setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:              true,
		PrimaryMetric:        "MAE",
		RecentBufferSize:     5,
		MinBufferSamples:     3,
		MinStd:               1,
		FixedFloor:           100,
		ZScoreThreshold:      3,
		DecisionWindowSize:   3,
		RequiredHitsInWindow: 2,
	})

	m := newTestMtlfService(cfg)
	store := newTestRetrainingState()
	scope := m.stateStore.GetOrCreateScope(testModelURL, testScopeKey, 5, 3)
	prefillScope(scope, 150, 150, 150)

	m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{
		testAccuracyReport(testModelURL, testScopeKey, 700),
	}, store)

	if got := scope.RecentObservationCount(); got != 4 {
		t.Fatalf("RecentObservationCount() = %d, want 4", got)
	}
	if got := scope.DegradationReferenceCount(); got != 3 {
		t.Fatalf("DegradationReferenceCount() = %d, want 3 after signal round", got)
	}
}

func TestHandleAccuracyReports_DoesNotRepeatAnLFMatchedSampleGate(t *testing.T) {
	cfg := setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:          true,
		PrimaryMetric:    "MAE",
		MinBufferSamples: 3,
	})
	m := newTestMtlfService(cfg)
	report := testAccuracyReport(testModelURL, testScopeKey, 50)
	report.SampleCount = 1

	m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{report}, newTestRetrainingState())

	scope := m.stateStore.GetOrCreateScope(testModelURL, testScopeKey, 20, 3)
	if got := scope.DegradationReferenceCount(); got != 1 {
		t.Fatalf("DegradationReferenceCount() = %d, want 1 for an AnLF-admitted report", got)
	}
}

func TestHandleAccuracyReports_SkipWhenRetrainingInFlight(t *testing.T) {
	cfg := setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:             true,
		PrimaryMetric:       "MAE",
		RecentBufferSize:    5,
		MinBufferSamples:    3,
		MinStd:              1,
		FixedFloor:          100,
		ZScoreThreshold:     3,
		ConsecutiveBreaches: 2,
	})

	m := newTestMtlfService(cfg)
	store := newTestRetrainingState()
	store.SetRetraining(true)

	modelURL := testModelURL
	scopeKey := testScopeKey
	m.HandleAccuracyReports(modelURL, []anlf.AccuracyReport{
		testAccuracyReport(modelURL, scopeKey, 200),
	}, store)

	if m.stateStore.ModelExists(modelURL) {
		t.Fatal("state store should remain empty while retraining is in flight")
	}
}

func TestHandleAccuracyReports_MultiScopeIsolation(t *testing.T) {
	cfg := setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:             true,
		PrimaryMetric:       "MAE",
		RecentBufferSize:    5,
		MinBufferSamples:    3,
		MinStd:              1,
		FixedFloor:          100,
		ZScoreThreshold:     3,
		ConsecutiveBreaches: 2,
	})

	m := newTestMtlfService(cfg)
	store := newTestRetrainingState()

	scopeA := m.stateStore.GetOrCreateScope(testModelURL, testScopeKey, 5, 2)
	scopeB := m.stateStore.GetOrCreateScope(testModelURL, testScopeKeyB, 5, 2)
	prefillScope(scopeA, 150, 150, 150)
	prefillScope(scopeB, 150, 150, 150)

	m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{
		testAccuracyReport(testModelURL, testScopeKey, 300),
		testAccuracyReport(testModelURL, testScopeKeyB, 150),
	}, store)

	if got := scopeA.BreachCount(); got != 1 {
		t.Fatalf("scopeA BreachCount() = %d, want 1", got)
	}
	if got := scopeB.BreachCount(); got != 0 {
		t.Fatalf("scopeB BreachCount() = %d, want 0", got)
	}
	if store.IsRetraining() {
		t.Fatal("store.IsRetraining() = true, want false before consecutive threshold")
	}
}

func TestHandleAccuracyReports_AnyScopeTriggersRetrain(t *testing.T) {
	cfg := setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:             true,
		PrimaryMetric:       "MAE",
		RecentBufferSize:    5,
		MinBufferSamples:    3,
		MinStd:              1,
		FixedFloor:          100,
		ZScoreThreshold:     3,
		ConsecutiveBreaches: 1,
	})

	m := newTestMtlfService(cfg)
	store := newTestRetrainingState()

	scopeA := m.stateStore.GetOrCreateScope(testModelURL, testScopeKey, 5, 1)
	scopeB := m.stateStore.GetOrCreateScope(testModelURL, testScopeKeyB, 5, 1)
	prefillScope(scopeA, 150, 150, 150)
	prefillScope(scopeB, 150, 150, 150)

	triggered := 0
	m.onRetrainTriggered = func(modelURL string, store retrainingState) {
		triggered++
	}

	m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{
		testAccuracyReport(testModelURL, testScopeKeyB, 150),
		testAccuracyReport(testModelURL, testScopeKey, 300),
	}, store)

	if triggered != 1 {
		t.Fatalf("triggered = %d, want 1", triggered)
	}
	if !store.IsRetraining() {
		t.Fatal("store.IsRetraining() = false, want true after any-scope trigger")
	}
	if got := scopeA.BreachCount(); got != 0 {
		t.Fatalf("scopeA BreachCount() after trigger = %d, want 0", got)
	}
	if got := scopeB.BreachCount(); got != 0 {
		t.Fatalf("scopeB BreachCount() after trigger = %d, want 0", got)
	}
}

func TestHandleAccuracyReports_MissingPrimaryMetricSkipsScope(t *testing.T) {
	cfg := setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:             true,
		PrimaryMetric:       "MAE",
		RecentBufferSize:    5,
		MinBufferSamples:    3,
		MinStd:              1,
		FixedFloor:          100,
		ZScoreThreshold:     3,
		ConsecutiveBreaches: 1,
	})

	m := newTestMtlfService(cfg)
	store := newTestRetrainingState()

	m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{{
		ModelURL:    testModelURL,
		ScopeKey:    testScopeKey,
		Metrics:     map[string]float64{"WAPE": 0.8},
		SampleCount: 5,
	}}, store)

	if m.stateStore.ModelExists(testModelURL) {
		t.Fatal("state store should remain empty when primary metric is missing")
	}
	if store.IsRetraining() {
		t.Fatal("store.IsRetraining() = true, want false when primary metric is missing")
	}
}

func TestHandleAccuracyReports_ZeroHistoryStillRequiresAbsGate(t *testing.T) {
	cfg := setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:             true,
		PrimaryMetric:       "MAE",
		RecentBufferSize:    5,
		MinBufferSamples:    3,
		MinStd:              1,
		FixedFloor:          100,
		ZScoreThreshold:     3,
		ConsecutiveBreaches: 1,
	})

	m := newTestMtlfService(cfg)
	store := newTestRetrainingState()
	scope := m.stateStore.GetOrCreateScope(testModelURL, testScopeKey, 5, 1)
	prefillScope(scope, 0, 0, 0)

	m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{
		testAccuracyReport(testModelURL, testScopeKey, 10),
	}, store)

	if got := scope.BreachCount(); got != 0 {
		t.Fatalf("BreachCount() = %d, want 0 when degradation eligibility fails despite high relative change", got)
	}
	if store.IsRetraining() {
		t.Fatal("store.IsRetraining() = true, want false when degradation eligibility fails")
	}
}

func TestHandleAccuracyReports_DecisionWindowToleratesMisses(t *testing.T) {
	cfg := setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:              true,
		PrimaryMetric:        "MAE",
		RecentBufferSize:     5,
		MinBufferSamples:     3,
		MinStd:               1,
		FixedFloor:           100,
		ZScoreThreshold:      3,
		DecisionWindowSize:   5,
		RequiredHitsInWindow: 3,
	})

	m := newTestMtlfService(cfg)
	store := newTestRetrainingState()
	triggered := 0
	m.onRetrainTriggered = func(modelURL string, store retrainingState) {
		triggered++
	}

	scope := m.stateStore.GetOrCreateScope(testModelURL, testScopeKey, 5, 5)
	prefillScope(scope, 150, 150, 150)

	reports := []anlf.AccuracyReport{
		testAccuracyReport(testModelURL, testScopeKey, 300),
		testAccuracyReport(testModelURL, testScopeKey, 150),
		testAccuracyReport(testModelURL, testScopeKey, 400),
		testAccuracyReport(testModelURL, testScopeKey, 150),
		testAccuracyReport(testModelURL, testScopeKey, 700),
	}
	for i, report := range reports {
		m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{report}, store)
		if i < len(reports)-1 && triggered != 0 {
			t.Fatalf("triggered early at round %d", i+1)
		}
	}

	if triggered != 1 {
		t.Fatalf("triggered = %d, want 1 after 3 hits in latest 5 rounds", triggered)
	}
}

func TestHandleAccuracyReports_ChronicPathTriggersWithoutDegradation(t *testing.T) {
	enabled := true
	cfg := setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:              true,
		PrimaryMetric:        "MAE",
		RecentBufferSize:     5,
		MinBufferSamples:     3,
		MinStd:               1,
		FixedFloor:           1000,
		ZScoreThreshold:      3,
		DecisionWindowSize:   3,
		RequiredHitsInWindow: 2,
		ChronicPolicy: &factory.ChronicPolicyConfig{
			Enabled:                 &enabled,
			Metric:                  "WAPE",
			Aggregator:              "percentile",
			Percentile:              50,
			Threshold:               0.8,
			MinDecisionTrafficScale: 100,
		},
	})

	m := newTestMtlfService(cfg)
	store := newTestRetrainingState()
	triggered := 0
	m.onRetrainTriggered = func(modelURL string, store retrainingState) {
		triggered++
	}

	report := testAccuracyReportWithMetrics(testModelURL, testScopeKey, map[string]float64{
		"MAE":  150,
		"WAPE": 1.1,
	}, 500)

	for i := 0; i < 5; i++ {
		m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{report}, store)
	}

	scope := m.stateStore.GetScope(testModelURL, testScopeKey)
	if scope == nil {
		t.Fatal("scope state was not created")
	}
	if got := scope.BreachCount(); got != 0 {
		t.Fatalf("degradation hits = %d, want 0", got)
	}
	if got := scope.ChronicHitCount(); got != 0 {
		t.Fatalf("chronic hits after trigger reset = %d, want 0", got)
	}
	if triggered != 1 {
		t.Fatalf("triggered = %d, want 1 from chronic path", triggered)
	}
	if !store.IsRetraining() {
		t.Fatal("store.IsRetraining() = false, want true after chronic trigger")
	}
}

func TestHandleAccuracyReports_ChronicPathRequiresTrafficScale(t *testing.T) {
	enabled := true
	cfg := setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:              true,
		PrimaryMetric:        "MAE",
		RecentBufferSize:     5,
		MinBufferSamples:     3,
		MinStd:               1,
		FixedFloor:           1000,
		ZScoreThreshold:      3,
		DecisionWindowSize:   3,
		RequiredHitsInWindow: 1,
		ChronicPolicy: &factory.ChronicPolicyConfig{
			Enabled:                 &enabled,
			Metric:                  "WAPE",
			Aggregator:              "percentile",
			Percentile:              50,
			Threshold:               0.8,
			MinDecisionTrafficScale: 1000,
		},
	})

	m := newTestMtlfService(cfg)
	store := newTestRetrainingState()
	report := testAccuracyReportWithMetrics(testModelURL, testScopeKey, map[string]float64{
		"MAE":  150,
		"WAPE": 1.1,
	}, 100)

	for i := 0; i < 4; i++ {
		m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{report}, store)
	}

	scope := m.stateStore.GetScope(testModelURL, testScopeKey)
	if scope == nil {
		t.Fatal("scope state was not created")
	}
	if got := scope.ChronicHitCount(); got != 0 {
		t.Fatalf("chronic hits = %d, want 0 when traffic scale is below threshold", got)
	}
	if store.IsRetraining() {
		t.Fatal("store.IsRetraining() = true, want false when chronic path is ineligible")
	}
}

func TestHandleAccuracyReports_LowTrafficOverpredictionTriggers(t *testing.T) {
	enabled := true
	cfg := setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:              true,
		PrimaryMetric:        "MAE",
		RecentBufferSize:     5,
		MinBufferSamples:     3,
		MinStd:               1,
		FixedFloor:           5000,
		ZScoreThreshold:      3,
		DecisionWindowSize:   3,
		RequiredHitsInWindow: 2,
		DegradationPolicy: &factory.DegradationPolicyConfig{
			MinDecisionTrafficScale: 1024,
		},
		LowTrafficPolicy: &factory.LowTrafficPolicyConfig{
			Enabled:                  &enabled,
			MaxActualTrafficScale:    1024,
			MinPredictedTrafficScale: 4096,
			PredictionOvershootRatio: 4.0,
		},
	})

	m := newTestMtlfService(cfg)
	store := newTestRetrainingState()
	triggered := 0
	m.onRetrainTriggered = func(modelURL string, store retrainingState) {
		triggered++
	}

	scope := m.stateStore.GetOrCreateScope(testModelURL, testScopeKey, 5, 3)
	prefillScope(scope, 150, 150, 150)

	report := testAccuracyReportWithMetricsAndPredicted(
		testModelURL,
		testScopeKey,
		map[string]float64{"MAE": 200},
		100,
		5000,
	)

	m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{report}, store)
	if got := scope.LowTrafficHitCount(); got != 1 {
		t.Fatalf("LowTrafficHitCount() after first hit = %d, want 1", got)
	}
	if triggered != 0 {
		t.Fatalf("triggered = %d, want 0 after first low-traffic hit", triggered)
	}

	m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{report}, store)
	if triggered != 1 {
		t.Fatalf("triggered = %d, want 1 after second low-traffic hit", triggered)
	}
	if !store.IsRetraining() {
		t.Fatal("store.IsRetraining() = false, want true after low-traffic overprediction trigger")
	}
}

func TestHandleAccuracyReports_LowTrafficOverpredictionRequiresPredictedFloor(t *testing.T) {
	enabled := true
	cfg := setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:              true,
		PrimaryMetric:        "MAE",
		RecentBufferSize:     5,
		MinBufferSamples:     3,
		MinStd:               1,
		FixedFloor:           5000,
		ZScoreThreshold:      3,
		DecisionWindowSize:   3,
		RequiredHitsInWindow: 1,
		DegradationPolicy: &factory.DegradationPolicyConfig{
			MinDecisionTrafficScale: 1024,
		},
		LowTrafficPolicy: &factory.LowTrafficPolicyConfig{
			Enabled:                  &enabled,
			MaxActualTrafficScale:    1024,
			MinPredictedTrafficScale: 4096,
			PredictionOvershootRatio: 4.0,
		},
	})

	m := newTestMtlfService(cfg)
	store := newTestRetrainingState()
	scope := m.stateStore.GetOrCreateScope(testModelURL, testScopeKey, 5, 3)
	prefillScope(scope, 150, 150, 150)

	report := testAccuracyReportWithMetricsAndPredicted(
		testModelURL,
		testScopeKey,
		map[string]float64{"MAE": 200},
		100,
		2000,
	)

	m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{report}, store)
	if got := scope.LowTrafficHitCount(); got != 0 {
		t.Fatalf("LowTrafficHitCount() = %d, want 0 when predicted traffic floor is not met", got)
	}
	if store.IsRetraining() {
		t.Fatal("store.IsRetraining() = true, want false when predicted traffic floor is not met")
	}
}
