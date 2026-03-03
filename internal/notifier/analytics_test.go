package notifier

import (
	"testing"
	"time"
)

// rfc3339 returns the RFC3339 string for a Unix timestamp (UTC).
// Used to build test observations with deterministic, readable timestamps.
func rfc3339(unix int64) string {
	return time.Unix(unix, 0).UTC().Format(time.RFC3339)
}

// makeObs builds a TrafficObservation with all 10 numeric fields set.
func makeObs(unix int64, totalVol, ulVol, dlVol, totalPkts, ulPkts, dlPkts, ulThr, dlThr, ulPktThr, dlPktThr float64) TrafficObservation {
	return TrafficObservation{
		Ts:          rfc3339(unix),
		TotalVol:    totalVol,
		UlVol:       ulVol,
		DlVol:       dlVol,
		TotalNbPkts: totalPkts,
		UlNbPkts:    ulPkts,
		DlNbPkts:    dlPkts,
		UlThr:       ulThr,
		DlThr:       dlThr,
		UlPktThr:    ulPktThr,
		DlPktThr:    dlPktThr,
	}
}

// TestAggregateObservationsByTimeBucket_Guard checks the early-return guard conditions.
func TestAggregateObservationsByTimeBucket_Guard(t *testing.T) {
	tests := []struct {
		name             string
		obs              []TrafficObservation
		samplingInterval int
		wantLen          int
		wantFirstUlVol   float64
	}{
		{
			name:             "nil input returns nil",
			obs:              nil,
			samplingInterval: 5,
			wantLen:          0,
		},
		{
			name:             "empty slice returns empty",
			obs:              []TrafficObservation{},
			samplingInterval: 5,
			wantLen:          0,
		},
		{
			name:             "zero samplingInterval returns obs unchanged",
			obs:              []TrafficObservation{{Ts: rfc3339(100), UlVol: 10}},
			samplingInterval: 0,
			wantLen:          1,
			wantFirstUlVol:   10,
		},
		{
			name:             "negative samplingInterval returns obs unchanged",
			obs:              []TrafficObservation{{Ts: rfc3339(100), UlVol: 20}},
			samplingInterval: -1,
			wantLen:          1,
			wantFirstUlVol:   20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := aggregateObservationsByTimeBucket(tt.obs, tt.samplingInterval)
			if len(result) != tt.wantLen {
				t.Errorf("len = %d, want %d", len(result), tt.wantLen)
			}
			if tt.wantLen > 0 && result[0].UlVol != tt.wantFirstUlVol {
				t.Errorf("result[0].UlVol = %v, want %v", result[0].UlVol, tt.wantFirstUlVol)
			}
		})
	}
}

// TestAggregateObservationsByTimeBucket_SingleSupi verifies that a single SUPI
// subscription (one record per time bucket) passes through unchanged.
func TestAggregateObservationsByTimeBucket_SingleSupi(t *testing.T) {
	obs := []TrafficObservation{
		{Ts: rfc3339(100), UlVol: 10, DlVol: 20},
		{Ts: rfc3339(105), UlVol: 12, DlVol: 25},
		{Ts: rfc3339(110), UlVol: 11, DlVol: 22},
	}

	result := aggregateObservationsByTimeBucket(obs, 5)

	if len(result) != 3 {
		t.Fatalf("len = %d, want 3", len(result))
	}
	wantUl := []float64{10, 12, 11}
	wantDl := []float64{20, 25, 22}
	for i := range result {
		if result[i].UlVol != wantUl[i] {
			t.Errorf("result[%d].UlVol = %v, want %v", i, result[i].UlVol, wantUl[i])
		}
		if result[i].DlVol != wantDl[i] {
			t.Errorf("result[%d].DlVol = %v, want %v", i, result[i].DlVol, wantDl[i])
		}
	}
}

// TestAggregateObservationsByTimeBucket_GroupThreeSupis is the primary regression
// test for the group-subscription bug: 3 SUPIs each reporting at t=100/105/110
// must be summed into 3 group-level observations, not concatenated into 9.
func TestAggregateObservationsByTimeBucket_GroupThreeSupis(t *testing.T) {
	// samplingInterval = 5s, so t=100/105/110 each form a distinct bucket
	obs := []TrafficObservation{
		// t=100: SUPI-1, SUPI-2, SUPI-3
		{Ts: rfc3339(100), UlVol: 10, DlVol: 20, TotalVol: 30, TotalNbPkts: 5},
		{Ts: rfc3339(100), UlVol: 15, DlVol: 30, TotalVol: 45, TotalNbPkts: 8},
		{Ts: rfc3339(100), UlVol: 5, DlVol: 10, TotalVol: 15, TotalNbPkts: 3},
		// t=105: SUPI-1, SUPI-2, SUPI-3
		{Ts: rfc3339(105), UlVol: 12, DlVol: 25, TotalVol: 37, TotalNbPkts: 6},
		{Ts: rfc3339(105), UlVol: 18, DlVol: 35, TotalVol: 53, TotalNbPkts: 9},
		{Ts: rfc3339(105), UlVol: 8, DlVol: 15, TotalVol: 23, TotalNbPkts: 4},
		// t=110: SUPI-1, SUPI-2, SUPI-3
		{Ts: rfc3339(110), UlVol: 11, DlVol: 22, TotalVol: 33, TotalNbPkts: 5},
		{Ts: rfc3339(110), UlVol: 16, DlVol: 32, TotalVol: 48, TotalNbPkts: 8},
		{Ts: rfc3339(110), UlVol: 6, DlVol: 12, TotalVol: 18, TotalNbPkts: 3},
	}

	result := aggregateObservationsByTimeBucket(obs, 5)

	if len(result) != 3 {
		t.Fatalf("len = %d, want 3 (9 per-SUPI records should collapse to 3 group buckets)", len(result))
	}

	tests := []struct {
		bucketIdx    int
		wantUlVol    float64
		wantDlVol    float64
		wantTotalVol float64
		wantPkts     float64
		wantTs       string
	}{
		{0, 30, 60, 90, 16, rfc3339(100)},  // 10+15+5, 20+30+10, 30+45+15, 5+8+3
		{1, 38, 75, 113, 19, rfc3339(105)}, // 12+18+8, 25+35+15, 37+53+23, 6+9+4
		{2, 33, 66, 99, 16, rfc3339(110)},  // 11+16+6, 22+32+12, 33+48+18, 5+8+3
	}

	for _, tt := range tests {
		r := result[tt.bucketIdx]
		if r.Ts != tt.wantTs {
			t.Errorf("result[%d].Ts = %v, want %v", tt.bucketIdx, r.Ts, tt.wantTs)
		}
		if r.UlVol != tt.wantUlVol {
			t.Errorf("result[%d].UlVol = %v, want %v", tt.bucketIdx, r.UlVol, tt.wantUlVol)
		}
		if r.DlVol != tt.wantDlVol {
			t.Errorf("result[%d].DlVol = %v, want %v", tt.bucketIdx, r.DlVol, tt.wantDlVol)
		}
		if r.TotalVol != tt.wantTotalVol {
			t.Errorf("result[%d].TotalVol = %v, want %v", tt.bucketIdx, r.TotalVol, tt.wantTotalVol)
		}
		if r.TotalNbPkts != tt.wantPkts {
			t.Errorf("result[%d].TotalNbPkts = %v, want %v", tt.bucketIdx, r.TotalNbPkts, tt.wantPkts)
		}
	}
}

// TestAggregateObservationsByTimeBucket_MisalignedTimestamps verifies that records
// with timestamps within the same samplingInterval window are merged into one bucket.
// e.g. t=100, t=101, t=102 all fall in bucket [100,105) when si=5.
func TestAggregateObservationsByTimeBucket_MisalignedTimestamps(t *testing.T) {
	obs := []TrafficObservation{
		{Ts: rfc3339(100), UlVol: 10, DlVol: 20},
		{Ts: rfc3339(101), UlVol: 15, DlVol: 30},
		{Ts: rfc3339(102), UlVol: 5, DlVol: 10},
	}

	result := aggregateObservationsByTimeBucket(obs, 5)

	if len(result) != 1 {
		t.Fatalf("len = %d, want 1 (t=100/101/102 are all in the same 5s bucket)", len(result))
	}
	if result[0].UlVol != 30 { // 10+15+5
		t.Errorf("UlVol = %v, want 30", result[0].UlVol)
	}
	if result[0].DlVol != 60 { // 20+30+10
		t.Errorf("DlVol = %v, want 60", result[0].DlVol)
	}
	// Bucket Ts should be aligned to the bucket start (truncated, not rounded)
	if result[0].Ts != rfc3339(100) {
		t.Errorf("Ts = %v, want %v (bucket start, not raw timestamp)", result[0].Ts, rfc3339(100))
	}
}

// TestAggregateObservationsByTimeBucket_SortedOutput verifies that buckets are always
// returned in ascending chronological order regardless of input order.
func TestAggregateObservationsByTimeBucket_SortedOutput(t *testing.T) {
	// Input intentionally in reverse order
	obs := []TrafficObservation{
		{Ts: rfc3339(110), UlVol: 11},
		{Ts: rfc3339(100), UlVol: 10},
		{Ts: rfc3339(105), UlVol: 12},
	}

	result := aggregateObservationsByTimeBucket(obs, 5)

	if len(result) != 3 {
		t.Fatalf("len = %d, want 3", len(result))
	}
	expectedTs := []string{rfc3339(100), rfc3339(105), rfc3339(110)}
	for i, wantTs := range expectedTs {
		if result[i].Ts != wantTs {
			t.Errorf("result[%d].Ts = %v, want %v (output must be sorted ASC)", i, result[i].Ts, wantTs)
		}
	}
}

// TestAggregateObservationsByTimeBucket_InvalidTimestampSkipped verifies that
// observations with unparseable Ts are silently dropped without panic.
func TestAggregateObservationsByTimeBucket_InvalidTimestampSkipped(t *testing.T) {
	obs := []TrafficObservation{
		{Ts: "not-a-timestamp", UlVol: 99},
		{Ts: rfc3339(100), UlVol: 10, DlVol: 20},
		{Ts: "", UlVol: 99},
	}

	result := aggregateObservationsByTimeBucket(obs, 5)

	if len(result) != 1 {
		t.Fatalf("len = %d, want 1 (invalid Ts entries must be skipped)", len(result))
	}
	if result[0].UlVol != 10 {
		t.Errorf("UlVol = %v, want 10 (only the valid entry should contribute)", result[0].UlVol)
	}
}

// TestAggregateObservationsByTimeBucket_AllFieldsSummed verifies that all 10 numeric
// fields are correctly summed when two observations share the same bucket.
func TestAggregateObservationsByTimeBucket_AllFieldsSummed(t *testing.T) {
	obs := []TrafficObservation{
		makeObs(100, 100, 40, 60, 10, 4, 6, 1, 2, 3, 4),
		makeObs(100, 200, 80, 120, 20, 8, 12, 3, 4, 5, 6),
	}

	result := aggregateObservationsByTimeBucket(obs, 5)

	if len(result) != 1 {
		t.Fatalf("len = %d, want 1", len(result))
	}
	r := result[0]

	cases := []struct {
		field string
		got   float64
		want  float64
	}{
		{"TotalVol", r.TotalVol, 300},   // 100+200
		{"UlVol", r.UlVol, 120},         // 40+80
		{"DlVol", r.DlVol, 180},         // 60+120
		{"TotalNbPkts", r.TotalNbPkts, 30}, // 10+20
		{"UlNbPkts", r.UlNbPkts, 12},    // 4+8
		{"DlNbPkts", r.DlNbPkts, 18},    // 6+12
		{"UlThr", r.UlThr, 4},           // 1+3
		{"DlThr", r.DlThr, 6},           // 2+4
		{"UlPktThr", r.UlPktThr, 8},     // 3+5
		{"DlPktThr", r.DlPktThr, 10},    // 4+6
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.field, c.got, c.want)
		}
	}
}
