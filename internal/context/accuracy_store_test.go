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

	// Should be reset after call
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

	// Past prediction (mature)
	store.AddPrediction(PredictionRecord{
		TargetTime: now.Add(-10 * time.Second),
		PredUlVol:  100,
		PredDlVol:  200,
	})

	// Future prediction (pending)
	store.AddPrediction(PredictionRecord{
		TargetTime: now.Add(60 * time.Second),
		PredUlVol:  300,
		PredDlVol:  400,
	})

	// Another past prediction
	store.AddPrediction(PredictionRecord{
		TargetTime: now.Add(-5 * time.Second),
		PredUlVol:  500,
		PredDlVol:  600,
	})

	mature := store.ConsumeMaturePredictions(0)
	if len(mature) != 2 {
		t.Fatalf("ConsumeMaturePredictions() returned %d, want 2", len(mature))
	}

	// Second call should only return the future one when it matures
	mature2 := store.ConsumeMaturePredictions(0)
	if len(mature2) != 0 {
		t.Errorf("Second ConsumeMaturePredictions() returned %d, want 0 (future still pending)", len(mature2))
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

	// Stop on a store that was never started — should not panic
	store.StopMonitor()

	if store.IsMonitorRunning() {
		t.Error("Store should not be running after StopMonitor on unstarted store")
	}
}

// ============================================================================
// ModelAccuracyStore — Consecutive Breaches
// ============================================================================

func TestModelAccuracyStore_ConsecutiveBreaches(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")

	// Increment 3 times
	for i := 1; i <= 3; i++ {
		count := store.IncrementBreaches()
		if count != i {
			t.Errorf("IncrementBreaches() = %d, want %d", count, i)
		}
	}

	// Reset
	store.ResetBreaches()

	count := store.IncrementBreaches()
	if count != 1 {
		t.Errorf("After reset, IncrementBreaches() = %d, want 1", count)
	}
}

func TestModelAccuracyStore_ConsecutiveBreaches_ResetOnGood(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")

	// Simulate: 2 breaches → 1 good check → breach again
	store.IncrementBreaches()
	store.IncrementBreaches()
	store.ResetBreaches() // good check resets

	count := store.IncrementBreaches()
	if count != 1 {
		t.Errorf("After reset, breach count = %d, want 1 (fresh start)", count)
	}
}

// ============================================================================
// ModelAccuracyStore — EMA
// ============================================================================

func TestModelAccuracyStore_EMA_Initialization(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")

	if ema := store.GetEMA(); ema != 0 {
		t.Errorf("Initial EMA = %.4f, want 0", ema)
	}

	// First update seeds EMA with the raw value
	ema := store.UpdateEMA(0.5, 0.3)
	if ema != 0.5 {
		t.Errorf("First EMA update = %.4f, want 0.5 (seeded)", ema)
	}
}

func TestModelAccuracyStore_EMA_Smoothing(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")
	alpha := 0.3

	// Seed: EMA = 0.2
	store.UpdateEMA(0.2, alpha)

	// Second update: EMA = 0.3 * 0.8 + 0.7 * 0.2 = 0.24 + 0.14 = 0.38
	ema := store.UpdateEMA(0.8, alpha)
	expected := alpha*0.8 + (1-alpha)*0.2
	if abs(ema-expected) > 0.0001 {
		t.Errorf("EMA after spike = %.4f, want %.4f", ema, expected)
	}

	// Third update with good value: EMA should decrease
	ema = store.UpdateEMA(0.1, alpha)
	expected = alpha*0.1 + (1-alpha)*expected
	if abs(ema-expected) > 0.0001 {
		t.Errorf("EMA after recovery = %.4f, want %.4f", ema, expected)
	}
}

func TestModelAccuracyStore_EMA_BurstDoesNotTrigger(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")
	threshold := 0.3
	alpha := 0.2 // Lower alpha = slower response to spike

	// 20 stable checks — EMA converges to ~0.1
	for i := 0; i < 20; i++ {
		store.UpdateEMA(0.1, alpha)
	}

	// Single burst at 0.9
	ema := store.UpdateEMA(0.9, alpha)

	// EMA should NOT exceed threshold: 0.2*0.9 + 0.8*≈0.1 ≈ 0.26
	if ema > threshold {
		t.Errorf("EMA after single burst = %.4f, should be <= %.2f (spike absorbed)", ema, threshold)
	}
}

func TestModelAccuracyStore_EMA_SustainedDegradation(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")
	threshold := 0.3
	alpha := 0.3

	// Sustained high deviation
	var ema float64
	for i := 0; i < 10; i++ {
		ema = store.UpdateEMA(0.5, alpha)
	}

	// After sustained degradation, EMA should exceed threshold
	if ema <= threshold {
		t.Errorf("EMA after sustained degradation = %.4f, should be > %.2f", ema, threshold)
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
