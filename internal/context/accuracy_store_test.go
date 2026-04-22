package context

import (
	"testing"
	"time"
)

// ============================================================================
// ModelAccuracyStore — Prediction Management
// ============================================================================

func TestModelAccuracyStore_AddPrediction(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")

	store.AddPrediction(PredictionRecord{
		ModelUrl:    "file:///test/model.pth",
		PredictedAt: time.Now(),
		TargetTime:  time.Now().Add(10 * time.Second),
		PredUlVol:   100,
		PredDlVol:   200,
		NwdafSubId:  "sub-001",
		ScopeKey:    "group:group-a",
	})

	store.AddPrediction(PredictionRecord{
		ModelUrl:    "file:///test/model.pth",
		PredictedAt: time.Now(),
		TargetTime:  time.Now().Add(20 * time.Second),
		PredUlVol:   150,
		PredDlVol:   250,
		NwdafSubId:  "sub-001",
		ScopeKey:    "supi:imsi-001",
	})

	num := store.GetAndResetInferenceNum()
	if num != 2 {
		t.Errorf("GetAndResetInferenceNum() = %d, want 2", num)
	}

	num = store.GetAndResetInferenceNum()
	if num != 0 {
		t.Errorf("GetAndResetInferenceNum() after reset = %d, want 0", num)
	}
}

func TestModelAccuracyStore_PreservesScopeKey(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")
	scopeKey := "group:group-a"

	store.AddPrediction(PredictionRecord{
		ModelUrl:    "file:///test/model.pth",
		PredictedAt: time.Now(),
		TargetTime:  time.Now().Add(-time.Second),
		PredUlVol:   100,
		PredDlVol:   200,
		NwdafSubId:  "sub-001",
		ScopeKey:    scopeKey,
	})

	mature := store.ConsumeMaturePredictions(0)
	if len(mature) != 1 {
		t.Fatalf("ConsumeMaturePredictions() returned %d, want 1", len(mature))
	}
	if mature[0].ScopeKey != scopeKey {
		t.Fatalf("mature[0].ScopeKey = %q, want %q", mature[0].ScopeKey, scopeKey)
	}
}

func TestModelAccuracyStore_ConsumeMaturePredictions(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")
	now := time.Now()

	store.AddPrediction(PredictionRecord{
		TargetTime: now.Add(-10 * time.Second),
		PredUlVol:  100,
		PredDlVol:  200,
	})

	store.AddPrediction(PredictionRecord{
		TargetTime: now.Add(60 * time.Second),
		PredUlVol:  300,
		PredDlVol:  400,
	})

	store.AddPrediction(PredictionRecord{
		TargetTime: now.Add(-5 * time.Second),
		PredUlVol:  500,
		PredDlVol:  600,
	})

	mature := store.ConsumeMaturePredictions(0)
	if len(mature) != 2 {
		t.Fatalf("ConsumeMaturePredictions() returned %d, want 2", len(mature))
	}

	mature2 := store.ConsumeMaturePredictions(0)
	if len(mature2) != 0 {
		t.Errorf("Second ConsumeMaturePredictions() returned %d, want 0", len(mature2))
	}
}

func TestModelAccuracyStore_ConsumeMaturePredictions_Empty(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")
	mature := store.ConsumeMaturePredictions(0)
	if len(mature) != 0 {
		t.Errorf("ConsumeMaturePredictions() on empty store returned %d, want 0", len(mature))
	}
}

// ============================================================================
// ModelAccuracyStore — Deviation Tracking
// ============================================================================

func TestModelAccuracyStore_Deviation(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")

	if d := store.GetDeviation(); d != 0 {
		t.Errorf("Initial deviation = %.2f, want 0", d)
	}

	store.UpdateDeviation(0.42)
	if d := store.GetDeviation(); d != 0.42 {
		t.Errorf("GetDeviation() = %.2f, want 0.42", d)
	}

	store.UpdateDeviation(0.15)
	if d := store.GetDeviation(); d != 0.15 {
		t.Errorf("GetDeviation() after update = %.2f, want 0.15", d)
	}
}

// ============================================================================
// ModelAccuracyStore — Goroutine Lifecycle
// ============================================================================

func TestModelAccuracyStore_MonitorLifecycle(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")

	if store.IsMonitorRunning() {
		t.Error("New store should not have monitor running")
	}

	canceled := false
	if !store.TryStartMonitor(func() { canceled = true }) {
		t.Error("TryStartMonitor should return true on first call")
	}

	if !store.IsMonitorRunning() {
		t.Error("IsMonitorRunning() should be true after TryStartMonitor")
	}

	store.StopMonitor()

	if store.IsMonitorRunning() {
		t.Error("IsMonitorRunning() should be false after StopMonitor")
	}

	if !canceled {
		t.Error("Cancel function should have been called")
	}
}

func TestModelAccuracyStore_StopMonitor_Idempotent(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")
	store.StopMonitor()

	if store.IsMonitorRunning() {
		t.Error("Store should not be running after StopMonitor on unstarted store")
	}
}

// ============================================================================
// ModelAccuracyStore — Retraining Guard
// ============================================================================

func TestModelAccuracyStore_RetrainingFlag(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")

	if store.IsRetraining() {
		t.Fatal("new store should not be marked retraining")
	}

	store.SetRetraining(true)
	if !store.IsRetraining() {
		t.Fatal("IsRetraining() = false, want true after SetRetraining(true)")
	}

	store.SetRetraining(false)
	if store.IsRetraining() {
		t.Fatal("IsRetraining() = true, want false after SetRetraining(false)")
	}
}
