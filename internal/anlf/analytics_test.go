package anlf

import (
	"testing"
	"time"
)

// makePoint builds a trafficPoint for test inputs.
func makePoint(
	totalVol, ulVol, dlVol, totalPkts, ulPkts, dlPkts,
	ulThr, dlThr, ulPktThr, dlPktThr float64,
) trafficPoint {
	return trafficPoint{
		TotalVol: totalVol, UlVol: ulVol, DlVol: dlVol,
		TotalNbPkts: totalPkts, UlNbPkts: ulPkts, DlNbPkts: dlPkts,
		UlThr: ulThr, DlThr: dlThr, UlPktThr: ulPktThr, DlPktThr: dlPktThr,
	}
}

// snapped returns a time.Time snapped to Unix timestamp unix (UTC).
func snapped(unix int64) time.Time { return time.Unix(unix, 0).UTC() }

// TestZipStreams_Empty verifies that empty input returns nil.
func TestZipStreams_Empty(t *testing.T) {
	result := zipStreams(nil, 5, 5, snapped(100))
	if result != nil {
		t.Errorf("want nil, got %v", result)
	}
	result = zipStreams([][]trafficPoint{}, 5, 5, snapped(100))
	if result != nil {
		t.Errorf("want nil for empty streams, got %v", result)
	}
}

// TestZipStreams_SingleStream verifies that a single stream with 3 points produces
// 3 output entries with correct values and derived timestamps.
func TestZipStreams_SingleStream(t *testing.T) {
	si := 5
	now := snapped(115) // snappedNow
	stream := []trafficPoint{
		{UlVol: 10, DlVol: 20}, // pos 0 → now - 2*si = t=105
		{UlVol: 12, DlVol: 25}, // pos 1 → now - 1*si = t=110
		{UlVol: 11, DlVol: 22}, // pos 2 → now        = t=115
	}
	result := zipStreams([][]trafficPoint{stream}, 5, si, now)

	if len(result) != 3 {
		t.Fatalf("len = %d, want 3", len(result))
	}
	wantUl := []float64{10, 12, 11}
	wantDl := []float64{20, 25, 22}
	wantTs := []time.Time{snapped(105), snapped(110), snapped(115)}
	for i, r := range result {
		if r.UlVol != wantUl[i] {
			t.Errorf("[%d] UlVol = %v, want %v", i, r.UlVol, wantUl[i])
		}
		if r.DlVol != wantDl[i] {
			t.Errorf("[%d] DlVol = %v, want %v", i, r.DlVol, wantDl[i])
		}
		wantTsStr := wantTs[i].UTC().Format(time.RFC3339)
		if r.Ts != wantTsStr {
			t.Errorf("[%d] Ts = %v, want %v", i, r.Ts, wantTsStr)
		}
	}
}

// TestZipStreams_MultiStreamSameLength verifies that 3 streams of equal length are
// summed correctly at each position.
func TestZipStreams_MultiStreamSameLength(t *testing.T) {
	now := snapped(110)
	streams := [][]trafficPoint{
		{{UlVol: 10, DlVol: 20}, {UlVol: 12, DlVol: 25}, {UlVol: 11, DlVol: 22}},
		{{UlVol: 15, DlVol: 30}, {UlVol: 18, DlVol: 35}, {UlVol: 16, DlVol: 32}},
		{{UlVol: 5, DlVol: 10}, {UlVol: 8, DlVol: 15}, {UlVol: 6, DlVol: 12}},
	}
	result := zipStreams(streams, 5, 5, now)

	if len(result) != 3 {
		t.Fatalf("len = %d, want 3", len(result))
	}
	wantUl := []float64{30, 38, 33} // 10+15+5, 12+18+8, 11+16+6
	wantDl := []float64{60, 75, 66} // 20+30+10, 25+35+15, 22+32+12
	for i, r := range result {
		if r.UlVol != wantUl[i] {
			t.Errorf("[%d] UlVol = %v, want %v", i, r.UlVol, wantUl[i])
		}
		if r.DlVol != wantDl[i] {
			t.Errorf("[%d] DlVol = %v, want %v", i, r.DlVol, wantDl[i])
		}
	}
}

// TestZipStreams_DifferentLengthAlignedFromEnd verifies that streams of different
// lengths are aligned from the end: the shorter stream's head is padded with zeros.
// A=5pts, B=3pts → output has 5 entries; B contributes 0 at positions 0 and 1.
func TestZipStreams_DifferentLengthAlignedFromEnd(t *testing.T) {
	now := snapped(120)
	streamA := []trafficPoint{ // 5 points
		{UlVol: 10}, {UlVol: 11}, {UlVol: 12}, {UlVol: 13}, {UlVol: 14},
	}
	streamB := []trafficPoint{ // 3 points — aligns to positions 2,3,4
		{UlVol: 100}, {UlVol: 101}, {UlVol: 102},
	}
	result := zipStreams([][]trafficPoint{streamA, streamB}, 10, 5, now)

	if len(result) != 5 {
		t.Fatalf("len = %d, want 5", len(result))
	}
	// pos 0,1: only A contributes (B is too short)
	if result[0].UlVol != 10 {
		t.Errorf("[0] UlVol = %v, want 10 (only A)", result[0].UlVol)
	}
	if result[1].UlVol != 11 {
		t.Errorf("[1] UlVol = %v, want 11 (only A)", result[1].UlVol)
	}
	// pos 2,3,4: A + B
	wantUl := []float64{112, 114, 116} // A[2]+B[0], A[3]+B[1], A[4]+B[2]
	for i, pos := range []int{2, 3, 4} {
		if result[pos].UlVol != wantUl[i] {
			t.Errorf("[%d] UlVol = %v, want %v", pos, result[pos].UlVol, wantUl[i])
		}
	}
}

// TestZipStreams_AllFieldsSummed verifies all 10 numeric fields are summed correctly.
func TestZipStreams_AllFieldsSummed(t *testing.T) {
	now := snapped(100)
	streams := [][]trafficPoint{
		{makePoint(100, 40, 60, 10, 4, 6, 1, 2, 3, 4)},
		{makePoint(200, 80, 120, 20, 8, 12, 3, 4, 5, 6)},
	}
	result := zipStreams(streams, 1, 5, now)

	if len(result) != 1 {
		t.Fatalf("len = %d, want 1", len(result))
	}
	r := result[0]
	cases := []struct {
		field string
		got   float64
		want  float64
	}{
		{"TotalVol", r.TotalVol, 300},
		{"UlVol", r.UlVol, 120},
		{"DlVol", r.DlVol, 180},
		{"TotalNbPkts", r.TotalNbPkts, 30},
		{"UlNbPkts", r.UlNbPkts, 12},
		{"DlNbPkts", r.DlNbPkts, 18},
		{"UlThr", r.UlThr, 4},
		{"DlThr", r.DlThr, 6},
		{"UlPktThr", r.UlPktThr, 8},
		{"DlPktThr", r.DlPktThr, 10},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.field, c.got, c.want)
		}
	}
}

// TestZipStreams_DerivedTimestamps verifies output Ts values are derived from
// snappedNow regardless of input data timestamps.
func TestZipStreams_DerivedTimestamps(t *testing.T) {
	si := 5
	now := snapped(100) // snappedNow
	stream := []trafficPoint{{UlVol: 1}, {UlVol: 2}, {UlVol: 3}}
	result := zipStreams([][]trafficPoint{stream}, 5, si, now)

	if len(result) != 3 {
		t.Fatalf("len = %d, want 3", len(result))
	}
	// outputLen=3: pos0→now-2*si=t=90, pos1→now-1*si=t=95, pos2→now=t=100
	wantTs := []time.Time{snapped(90), snapped(95), snapped(100)}
	for i, wt := range wantTs {
		want := wt.UTC().Format(time.RFC3339)
		if result[i].Ts != want {
			t.Errorf("[%d] Ts = %v, want %v", i, result[i].Ts, want)
		}
	}
}

// TestZipStreams_InputWindowCap verifies that outputLen is capped at inputWindow
// even when streams are longer.
func TestZipStreams_InputWindowCap(t *testing.T) {
	now := snapped(150)
	stream := make([]trafficPoint, 10) // 10 points, but inputWindow=5
	for i := range stream {
		stream[i].UlVol = float64(i + 1)
	}
	result := zipStreams([][]trafficPoint{stream}, 5, 5, now)

	if len(result) != 5 {
		t.Fatalf("len = %d, want 5 (capped at inputWindow)", len(result))
	}
	// Should be the last 5: UlVol = 6,7,8,9,10
	wantUl := []float64{6, 7, 8, 9, 10}
	for i, r := range result {
		if r.UlVol != wantUl[i] {
			t.Errorf("[%d] UlVol = %v, want %v", i, r.UlVol, wantUl[i])
		}
	}
}
