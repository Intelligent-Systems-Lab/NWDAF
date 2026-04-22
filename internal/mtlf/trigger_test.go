package mtlf

import (
	"testing"
	"time"

	"github.com/free5gc/nwdaf/internal/anlf"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
)

const (
	testModelURL  = "file:///test/model.pth"
	testScopeKey  = "group:test"
	testScopeKeyB = "group:test-b"
)

func setTestAccuracyMonitorConfig(t *testing.T, cfg *factory.AccuracyMonitorConfig) {
	t.Helper()

	oldCfg := factory.NwdafConfig
	factory.NwdafConfig = &factory.Config{
		Configuration: &factory.Configuration{
			Mtlf: &factory.MtlfConfig{
				Enabled:         true,
				AccuracyMonitor: cfg,
			},
		},
	}
	t.Cleanup(func() {
		factory.NwdafConfig = oldCfg
	})
}

func testAccuracyReport(modelURL, scopeKey string, current float64) anlf.AccuracyReport {
	return anlf.AccuracyReport{
		ModelURL:    modelURL,
		ScopeKey:    scopeKey,
		Metrics:     map[string]float64{"MAE": current},
		SampleCount: 5,
	}
}

func prefillScope(scope *ScopeState, values ...float64) {
	for _, value := range values {
		scope.RecordMetric("MAE", value, time.Now())
	}
}

func TestHandleAccuracyReports_ColdStartUsesAbsGate(t *testing.T) {
	setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:             true,
		PrimaryMetric:       "MAE",
		RecentBufferSize:    5,
		MinBufferSamples:    5,
		MinStd:              1,
		FixedFloor:          100,
		ZScoreThreshold:     3,
		ConsecutiveBreaches: 2,
	})

	m := NewMtlfService(nil)
	store := nwdaf_context.NewModelAccuracyStore(testModelURL)
	modelURL := testModelURL
	scopeKey := testScopeKey

	triggered := 0
	m.onRetrainTriggered = func(modelURL string, store *nwdaf_context.ModelAccuracyStore) {
		triggered++
	}

	report := testAccuracyReport(modelURL, scopeKey, 150)

	m.HandleAccuracyReports(modelURL, []anlf.AccuracyReport{report}, store)
	scope := m.stateStore.GetScope(modelURL, scopeKey)
	if scope == nil {
		t.Fatal("scope state was not created")
	}
	if got := scope.BreachCount(); got != 1 {
		t.Fatalf("BreachCount() after first cold-start trigger = %d, want 1", got)
	}
	if triggered != 0 {
		t.Fatalf("triggered = %d, want 0 after first report", triggered)
	}

	m.HandleAccuracyReports(modelURL, []anlf.AccuracyReport{report}, store)
	if triggered != 1 {
		t.Fatalf("triggered = %d, want 1 after second report", triggered)
	}
	if !store.IsRetraining() {
		t.Fatal("store.IsRetraining() = false, want true after retrain trigger")
	}
	if got := scope.BreachCount(); got != 0 {
		t.Fatalf("BreachCount() after retrain trigger = %d, want 0", got)
	}
}

func TestHandleAccuracyReports_BaselineReadyRequiresRelGate(t *testing.T) {
	setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:             true,
		PrimaryMetric:       "MAE",
		RecentBufferSize:    5,
		MinBufferSamples:    3,
		MinStd:              100,
		FixedFloor:          100,
		ZScoreThreshold:     3,
		ConsecutiveBreaches: 2,
	})

	m := NewMtlfService(nil)
	store := nwdaf_context.NewModelAccuracyStore(testModelURL)
	modelURL := testModelURL
	scopeKey := testScopeKey

	scope := m.stateStore.GetOrCreateScope(modelURL, scopeKey, 5)
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

func TestHandleAccuracyReports_ResetBreachOnGoodRound(t *testing.T) {
	setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:             true,
		PrimaryMetric:       "MAE",
		RecentBufferSize:    5,
		MinBufferSamples:    3,
		MinStd:              1,
		FixedFloor:          100,
		ZScoreThreshold:     3,
		ConsecutiveBreaches: 3,
	})

	m := NewMtlfService(nil)
	store := nwdaf_context.NewModelAccuracyStore(testModelURL)
	modelURL := testModelURL
	scopeKey := testScopeKey

	scope := m.stateStore.GetOrCreateScope(modelURL, scopeKey, 5)
	prefillScope(scope, 150, 150, 150)

	bad := testAccuracyReport(modelURL, scopeKey, 300)
	badAgain := testAccuracyReport(modelURL, scopeKey, 400)
	good := testAccuracyReport(modelURL, scopeKey, 150)

	m.HandleAccuracyReports(modelURL, []anlf.AccuracyReport{bad}, store)
	if got := scope.BreachCount(); got != 1 {
		t.Fatalf("BreachCount() after bad round = %d, want 1", got)
	}

	m.HandleAccuracyReports(modelURL, []anlf.AccuracyReport{good}, store)
	if got := scope.BreachCount(); got != 0 {
		t.Fatalf("BreachCount() after good round = %d, want 0", got)
	}

	m.HandleAccuracyReports(modelURL, []anlf.AccuracyReport{badAgain}, store)
	if got := scope.BreachCount(); got != 1 {
		t.Fatalf("BreachCount() after second bad round = %d, want 1", got)
	}
}

func TestHandleAccuracyReports_SkipWhenRetrainingInFlight(t *testing.T) {
	setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:             true,
		PrimaryMetric:       "MAE",
		RecentBufferSize:    5,
		MinBufferSamples:    3,
		MinStd:              1,
		FixedFloor:          100,
		ZScoreThreshold:     3,
		ConsecutiveBreaches: 2,
	})

	m := NewMtlfService(nil)
	store := nwdaf_context.NewModelAccuracyStore(testModelURL)
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
	setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:             true,
		PrimaryMetric:       "MAE",
		RecentBufferSize:    5,
		MinBufferSamples:    3,
		MinStd:              1,
		FixedFloor:          100,
		ZScoreThreshold:     3,
		ConsecutiveBreaches: 2,
	})

	m := NewMtlfService(nil)
	store := nwdaf_context.NewModelAccuracyStore(testModelURL)

	scopeA := m.stateStore.GetOrCreateScope(testModelURL, testScopeKey, 5)
	scopeB := m.stateStore.GetOrCreateScope(testModelURL, testScopeKeyB, 5)
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
	setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:             true,
		PrimaryMetric:       "MAE",
		RecentBufferSize:    5,
		MinBufferSamples:    3,
		MinStd:              1,
		FixedFloor:          100,
		ZScoreThreshold:     3,
		ConsecutiveBreaches: 1,
	})

	m := NewMtlfService(nil)
	store := nwdaf_context.NewModelAccuracyStore(testModelURL)

	scopeA := m.stateStore.GetOrCreateScope(testModelURL, testScopeKey, 5)
	scopeB := m.stateStore.GetOrCreateScope(testModelURL, testScopeKeyB, 5)
	prefillScope(scopeA, 150, 150, 150)
	prefillScope(scopeB, 150, 150, 150)

	triggered := 0
	m.onRetrainTriggered = func(modelURL string, store *nwdaf_context.ModelAccuracyStore) {
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
	setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:             true,
		PrimaryMetric:       "MAE",
		RecentBufferSize:    5,
		MinBufferSamples:    3,
		MinStd:              1,
		FixedFloor:          100,
		ZScoreThreshold:     3,
		ConsecutiveBreaches: 1,
	})

	m := NewMtlfService(nil)
	store := nwdaf_context.NewModelAccuracyStore(testModelURL)

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
	setTestAccuracyMonitorConfig(t, &factory.AccuracyMonitorConfig{
		Enabled:             true,
		PrimaryMetric:       "MAE",
		RecentBufferSize:    5,
		MinBufferSamples:    3,
		MinStd:              1,
		FixedFloor:          100,
		ZScoreThreshold:     3,
		ConsecutiveBreaches: 1,
	})

	m := NewMtlfService(nil)
	store := nwdaf_context.NewModelAccuracyStore(testModelURL)
	scope := m.stateStore.GetOrCreateScope(testModelURL, testScopeKey, 5)
	prefillScope(scope, 0, 0, 0)

	m.HandleAccuracyReports(testModelURL, []anlf.AccuracyReport{
		testAccuracyReport(testModelURL, testScopeKey, 10),
	}, store)

	if got := scope.BreachCount(); got != 0 {
		t.Fatalf("BreachCount() = %d, want 0 when absGate fails despite high relative change", got)
	}
	if store.IsRetraining() {
		t.Fatal("store.IsRetraining() = true, want false when absGate fails")
	}
}
