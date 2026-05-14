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

	snapshot := store.SnapshotPredictions()
	if len(snapshot) != 2 {
		t.Fatalf("len(SnapshotPredictions()) = %d, want 2", len(snapshot))
	}
	if snapshot[0].ID == 0 || snapshot[1].ID == 0 {
		t.Fatal("SnapshotPredictions() returned prediction with zero ID")
	}
}

func TestModelAccuracyStore_PreservesScopeKey(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")
	scopeKey := "group:group-a"

	store.AddPrediction(PredictionRecord{
		ModelUrl:       "file:///test/model.pth",
		PredictedAt:    time.Now(),
		TargetTime:     time.Now().Add(-time.Second),
		TargetSlotTime: time.Now().Add(-time.Second),
		PredUlVol:      100,
		PredDlVol:      200,
		NwdafSubId:     "sub-001",
		ScopeKey:       scopeKey,
	})

	snapshot := store.SnapshotPredictions()
	if len(snapshot) != 1 {
		t.Fatalf("SnapshotPredictions() returned %d, want 1", len(snapshot))
	}
	if snapshot[0].ScopeKey != scopeKey {
		t.Fatalf("snapshot[0].ScopeKey = %q, want %q", snapshot[0].ScopeKey, scopeKey)
	}
}

func TestModelAccuracyStore_ResolvePredictions(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")
	now := time.Now()

	store.AddPrediction(PredictionRecord{
		TargetTime:     now.Add(-10 * time.Second),
		TargetSlotTime: now.Add(-10 * time.Second),
		PredUlVol:      100,
		PredDlVol:      200,
	})

	store.AddPrediction(PredictionRecord{
		TargetTime:     now.Add(60 * time.Second),
		TargetSlotTime: now.Add(60 * time.Second),
		PredUlVol:      300,
		PredDlVol:      400,
	})

	store.AddPrediction(PredictionRecord{
		TargetTime:     now.Add(-5 * time.Second),
		TargetSlotTime: now.Add(-5 * time.Second),
		PredUlVol:      500,
		PredDlVol:      600,
	})

	snapshot := store.SnapshotPredictions()
	if len(snapshot) != 3 {
		t.Fatalf("len(SnapshotPredictions()) = %d, want 3", len(snapshot))
	}

	matchedIDs := map[uint64]struct{}{
		snapshot[0].ID: {},
	}
	missedIDs := map[uint64]struct{}{
		snapshot[1].ID: {},
		snapshot[2].ID: {},
	}

	matched, discarded := store.ResolvePredictions(matchedIDs, missedIDs, 2)
	if matched != 1 {
		t.Fatalf("ResolvePredictions() matched = %d, want 1", matched)
	}
	if discarded != 0 {
		t.Fatalf("ResolvePredictions() discarded = %d, want 0", discarded)
	}

	snapshot = store.SnapshotPredictions()
	if len(snapshot) != 2 {
		t.Fatalf("len(SnapshotPredictions()) after resolve = %d, want 2", len(snapshot))
	}
	for _, pred := range snapshot {
		if pred.MissCount != 1 {
			t.Fatalf("pred.MissCount = %d, want 1 after first miss", pred.MissCount)
		}
	}

	matched, discarded = store.ResolvePredictions(nil, missedIDs, 2)
	if matched != 0 {
		t.Fatalf("second ResolvePredictions() matched = %d, want 0", matched)
	}
	if discarded != 2 {
		t.Fatalf("second ResolvePredictions() discarded = %d, want 2", discarded)
	}
	if got := len(store.SnapshotPredictions()); got != 0 {
		t.Fatalf("len(SnapshotPredictions()) after discard = %d, want 0", got)
	}
}

func TestModelAccuracyStore_DiscardAllPredictions_Empty(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")
	if discarded := store.DiscardAllPredictions(); discarded != 0 {
		t.Errorf("DiscardAllPredictions() on empty store returned %d, want 0", discarded)
	}
}

func TestModelAccuracyStore_DiscardAllPredictions(t *testing.T) {
	store := NewModelAccuracyStore("file:///test/model.pth")
	now := time.Now()
	store.AddPrediction(PredictionRecord{
		TargetTime:     now,
		TargetSlotTime: now,
	})
	store.AddPrediction(PredictionRecord{
		TargetTime:     now.Add(time.Second),
		TargetSlotTime: now.Add(time.Second),
	})

	if discarded := store.DiscardAllPredictions(); discarded != 2 {
		t.Fatalf("DiscardAllPredictions() = %d, want 2", discarded)
	}
	if got := len(store.SnapshotPredictions()); got != 0 {
		t.Fatalf("len(SnapshotPredictions()) after discard = %d, want 0", got)
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
