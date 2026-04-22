package mtlf

import (
	"math"
	"testing"
	"time"
)

func TestScopeState_RecordMetricAndStats(t *testing.T) {
	scope := newScopeState("scope-a", 3, 3)
	now := time.Now()

	scope.RecordMetric("MAE", 10, now)
	scope.RecordMetric("MAE", 20, now)
	scope.RecordMetric("MAE", 30, now)

	if got := scope.SampleCount("MAE"); got != 3 {
		t.Fatalf("SampleCount() = %d, want 3", got)
	}
	if got := scope.Mean("MAE"); got != 20 {
		t.Fatalf("Mean() = %.4f, want 20", got)
	}
	if got := scope.Min("MAE"); got != 10 {
		t.Fatalf("Min() = %.4f, want 10", got)
	}
	if got := scope.Max("MAE"); got != 30 {
		t.Fatalf("Max() = %.4f, want 30", got)
	}

	wantStd := math.Sqrt((100 + 0 + 100) / 3.0)
	if got := scope.Std("MAE"); math.Abs(got-wantStd) > 1e-9 {
		t.Fatalf("Std() = %.10f, want %.10f", got, wantStd)
	}

	scope.RecordMetric("MAE", 40, now)
	if got := scope.SampleCount("MAE"); got != 3 {
		t.Fatalf("SampleCount() after overwrite = %d, want 3", got)
	}
	if got := scope.Mean("MAE"); got != 30 {
		t.Fatalf("Mean() after overwrite = %.4f, want 30", got)
	}
	if got := scope.Min("MAE"); got != 20 {
		t.Fatalf("Min() after overwrite = %.4f, want 20", got)
	}
	if got := scope.Max("MAE"); got != 40 {
		t.Fatalf("Max() after overwrite = %.4f, want 40", got)
	}
}

func TestScopeState_Percentile(t *testing.T) {
	scope := newScopeState("scope-a", 5, 3)
	now := time.Now()
	for _, value := range []float64{10, 20, 30, 40} {
		scope.RecordMetric("WAPE", value, now)
	}

	if got := scope.Percentile("WAPE", 50); got != 25 {
		t.Fatalf("Percentile(50) = %.2f, want 25", got)
	}
	if got := scope.Percentile("WAPE", 75); got != 32.5 {
		t.Fatalf("Percentile(75) = %.2f, want 32.5", got)
	}
}

func TestMonitorStateStore_GCExpiredScopes(t *testing.T) {
	store := NewMonitorStateStore()
	modelURL := "file:///test/model.pth"
	scope := store.GetOrCreateScope(modelURL, "scope-a", 3, 3)

	oldNow := time.Now().Add(-2 * time.Minute)
	scope.RecordMetric("MAE", 10, oldNow)

	store.GCExpiredScopes(time.Now(), time.Minute)
	if store.ModelExists(modelURL) {
		t.Fatal("ModelExists() = true, want false after TTL GC")
	}
}
