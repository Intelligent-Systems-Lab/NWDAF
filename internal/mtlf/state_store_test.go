package mtlf

import (
	"math"
	"testing"
	"time"
)

func testObservation(metric string, value float64, now time.Time) ScopeObservation {
	return ScopeObservation{
		Timestamp:    now,
		SampleCount:  5,
		TrafficScale: 2048,
		Metrics: map[string]float64{
			metric: value,
		},
	}
}

func TestScopeState_RecordMetricAndStats(t *testing.T) {
	scope := newScopeState("scope-a", 3, 3)
	now := time.Now()

	scope.RecordObservation(testObservation("MAE", 10, now))
	scope.RecordObservation(testObservation("MAE", 20, now))
	scope.RecordObservation(testObservation("MAE", 30, now))

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

	scope.RecordObservation(testObservation("MAE", 40, now))
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
		scope.RecordObservation(testObservation("WAPE", value, now))
	}

	if got := scope.Percentile("WAPE", 50); got != 25 {
		t.Fatalf("Percentile(50) = %.2f, want 25", got)
	}
	if got := scope.Percentile("WAPE", 75); got != 32.5 {
		t.Fatalf("Percentile(75) = %.2f, want 32.5", got)
	}
}

func TestScopeState_DegradationReferenceStats(t *testing.T) {
	scope := newScopeState("scope-a", 5, 3)
	now := time.Now()

	scope.RecordObservation(ScopeObservation{
		Timestamp:    now,
		SampleCount:  5,
		TrafficScale: 2048,
		Metrics: map[string]float64{
			"MAE": 99,
		},
	})
	for _, value := range []float64{10, 20, 30} {
		scope.RecordDegradationReference(ScopeObservation{
			Timestamp:    now,
			SampleCount:  5,
			TrafficScale: 2048,
			Metrics: map[string]float64{
				"MAE": value,
			},
		})
	}

	if got := scope.SampleCount("MAE"); got != 1 {
		t.Fatalf("SampleCount() = %d, want 1 for recent observations", got)
	}
	if got := scope.DegradationSampleCount("MAE"); got != 3 {
		t.Fatalf("DegradationSampleCount() = %d, want 3", got)
	}
	if got := scope.DegradationMean("MAE"); got != 20 {
		t.Fatalf("DegradationMean() = %.4f, want 20", got)
	}
	wantStd := math.Sqrt((100 + 0 + 100) / 3.0)
	if got := scope.DegradationStd("MAE"); math.Abs(got-wantStd) > 1e-9 {
		t.Fatalf("DegradationStd() = %.10f, want %.10f", got, wantStd)
	}
}

func TestMonitorStateStore_GCExpiredScopes(t *testing.T) {
	store := NewMonitorStateStore()
	modelURL := "file:///test/model.pth"
	scope := store.GetOrCreateScope(modelURL, "scope-a", 3, 3)

	oldNow := time.Now().Add(-2 * time.Minute)
	scope.RecordObservation(testObservation("MAE", 10, oldNow))

	store.GCExpiredScopes(time.Now(), time.Minute)
	if store.ModelExists(modelURL) {
		t.Fatal("ModelExists() = true, want false after TTL GC")
	}
}
