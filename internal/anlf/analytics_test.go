package anlf

import (
	"testing"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

// makeDP builds an UpfDataPoint with only the fields used by tests.
func makeDP(unixSec int64, ulVol, dlVol int64) nwdaf_context.UpfDataPoint {
	return nwdaf_context.UpfDataPoint{
		Timestamp: time.Unix(unixSec, 0),
		UlVolume:  ulVol,
		DlVolume:  dlVol,
	}
}

// makeDPf builds an UpfDataPoint with a fractional-second timestamp (for drift tests).
func makeDPf(unixSec float64, ulVol, dlVol int64) nwdaf_context.UpfDataPoint {
	sec := int64(unixSec)
	nsec := int64((unixSec - float64(sec)) * 1e9)
	return nwdaf_context.UpfDataPoint{
		Timestamp: time.Unix(sec, nsec),
		UlVolume:  ulVol,
		DlVolume:  dlVol,
	}
}

// setupCtx re-initializes the global NWDAF context and returns it.
func setupCtx(t *testing.T) *nwdaf_context.NWDAFContext {
	t.Helper()
	nwdaf_context.Init()
	return nwdaf_context.GetSelf()
}

// snappedTs returns a UTC time from a unix second value.
func snappedTs(unix int64) time.Time { return time.Unix(unix, 0).UTC() }

// --- helpers ---

func setRawUpfData(ctx *nwdaf_context.NWDAFContext, corrId, ip string, pts []nwdaf_context.UpfDataPoint) {
	td := ctx.GetOrCreateTrafficData(corrId, ip)
	td.Lock()
	td.RawUpfData = pts
	td.Unlock()
}

// --- tests ---

// TestAggregateInMemory_NoCorrIds verifies that nil corrIds returns nil.
func TestAggregateInMemory_NoCorrIds(t *testing.T) {
	ctx := setupCtx(t)
	result, _ := alignAndZipInMemory(nil, ctx, 5, 5, snappedTs(100))
	if result != nil {
		t.Errorf("want nil, got %v", result)
	}
}

// TestAggregateInMemory_NoData verifies that a corrId with no data returns nil.
func TestAggregateInMemory_NoData(t *testing.T) {
	ctx := setupCtx(t)
	setRawUpfData(ctx, "corr-1", "1.1.1.1", nil)
	result, _ := alignAndZipInMemory([]string{"corr-1"}, ctx, 5, 5, snappedTs(100))
	if result != nil {
		t.Errorf("want nil for empty data, got %v", result)
	}
}

// TestAggregateInMemory_SingleIP_Basic verifies values and timestamps for a single
// IP with 3 clean reports at exact si intervals.
// si=5, snappedNow=15, anchor=0: reports at 0,5,10 → global indices -3,-2,-1 → Ts=0,5,10
func TestAggregateInMemory_SingleIP_Basic(t *testing.T) {
	ctx := setupCtx(t)
	si := 5
	setRawUpfData(ctx, "corr-1", "1.1.1.1", []nwdaf_context.UpfDataPoint{
		makeDP(0, 10, 20),
		makeDP(5, 12, 25),
		makeDP(10, 11, 22),
	})

	result, _ := alignAndZipInMemory([]string{"corr-1"}, ctx, 5, si, snappedTs(15))

	if len(result) != 3 {
		t.Fatalf("len=%d, want 3", len(result))
	}
	wantUl := []float64{10, 12, 11}
	wantDl := []float64{20, 25, 22}
	wantTs := []time.Time{snappedTs(0), snappedTs(5), snappedTs(10)}
	for i, r := range result {
		if r.UlVol != wantUl[i] {
			t.Errorf("[%d] UlVol=%v, want %v", i, r.UlVol, wantUl[i])
		}
		if r.DlVol != wantDl[i] {
			t.Errorf("[%d] DlVol=%v, want %v", i, r.DlVol, wantDl[i])
		}
		want := wantTs[i].UTC().Format(time.RFC3339)
		if r.Ts != want {
			t.Errorf("[%d] Ts=%v, want %v", i, r.Ts, want)
		}
	}
}

// TestAggregateInMemory_Drift verifies that a report arriving slightly before the
// next period boundary is still assigned to the correct slot via anchor-based round.
// anchor=0.3, si=5: report at 9.8 should be slot index 2 (center≈10.3), not slot 1.
func TestAggregateInMemory_Drift(t *testing.T) {
	ctx := setupCtx(t)
	si := 5
	setRawUpfData(ctx, "corr-1", "1.1.1.1", []nwdaf_context.UpfDataPoint{
		makeDPf(0.3, 10, 0),
		makeDPf(5.3, 12, 0),
		makeDPf(9.8, 11, 0), // drifted: expected ≈10.3, arrived 0.5s early
		makeDPf(15.3, 14, 0),
	})

	// snappedNow=20, so global indices: 0.3→-4, 5.3→-3, 9.8→-2 (rounds to center≈10.3), 15.3→-1
	result, _ := alignAndZipInMemory([]string{"corr-1"}, ctx, 10, si, snappedTs(20))

	if len(result) != 4 {
		t.Fatalf("len=%d, want 4 (drift report must be in its own slot)", len(result))
	}
	// The UlVol sequence should be 10, 12, 11, 14 in ascending time order.
	wantUl := []float64{10, 12, 11, 14}
	for i, r := range result {
		if r.UlVol != wantUl[i] {
			t.Errorf("[%d] UlVol=%v, want %v", i, r.UlVol, wantUl[i])
		}
	}
}

// TestAggregateInMemory_Dedup verifies that two reports from the same IP that land
// on the same anchor-relative slot are deduplicated (last wins).
func TestAggregateInMemory_Dedup(t *testing.T) {
	ctx := setupCtx(t)
	si := 5
	setRawUpfData(ctx, "corr-1", "1.1.1.1", []nwdaf_context.UpfDataPoint{
		makeDP(0, 10, 20),
		makeDP(1, 99, 99), // rounds to slot 0 (anchor=0, round(1/5)=0) → last wins
		makeDP(5, 12, 25),
	})

	result, _ := alignAndZipInMemory([]string{"corr-1"}, ctx, 5, si, snappedTs(10))

	if len(result) != 2 {
		t.Fatalf("len=%d, want 2 (slot 0 deduped to 1 entry)", len(result))
	}
	// Slot 0: last of {10,99} wins → UlVol=99
	if result[0].UlVol != 99 {
		t.Errorf("slot 0 UlVol=%v, want 99 (last-wins dedup)", result[0].UlVol)
	}
	if result[1].UlVol != 12 {
		t.Errorf("slot 1 UlVol=%v, want 12", result[1].UlVol)
	}
}

// TestAggregateInMemory_MultiIP_SameAnchor verifies that two IPs with the same
// anchor and timing have their values summed in each slot.
func TestAggregateInMemory_MultiIP_SameAnchor(t *testing.T) {
	ctx := setupCtx(t)
	si := 5
	setRawUpfData(ctx, "corr-1", "1.1.1.1", []nwdaf_context.UpfDataPoint{
		makeDP(0, 10, 20),
		makeDP(5, 12, 25),
	})
	setRawUpfData(ctx, "corr-2", "2.2.2.2", []nwdaf_context.UpfDataPoint{
		makeDP(0, 5, 10),
		makeDP(5, 8, 15),
	})

	result, _ := alignAndZipInMemory([]string{"corr-1", "corr-2"}, ctx, 5, si, snappedTs(10))

	if len(result) != 2 {
		t.Fatalf("len=%d, want 2", len(result))
	}
	// Slot 0: 10+5=15 UlVol, 20+10=30 DlVol
	if result[0].UlVol != 15 {
		t.Errorf("slot 0 UlVol=%v, want 15", result[0].UlVol)
	}
	if result[0].DlVol != 30 {
		t.Errorf("slot 0 DlVol=%v, want 30", result[0].DlVol)
	}
	// Slot 1: 12+8=20 UlVol, 25+15=40 DlVol
	if result[1].UlVol != 20 {
		t.Errorf("slot 1 UlVol=%v, want 20", result[1].UlVol)
	}
	if result[1].DlVol != 40 {
		t.Errorf("slot 1 DlVol=%v, want 40", result[1].DlVol)
	}
}

// TestAggregateInMemory_MultiIP_DifferentAnchor verifies that a late-joining IP
// (anchor far from others) is correctly co-located via global round, not mis-aligned.
//
// si=5, snappedNow=30
// IP-A anchor=0:  slots at t=25 (global -1) and t=30 (global 0)
// IP-B anchor=27: slot  at t=27 → per-IP center=27 → global round((27-30)/5)=-1
// So global -1 should sum IP-A(t=25) + IP-B(t=27), Ts=mean(25,27)=26
// Global  0 should have only IP-A(t=30)
func TestAggregateInMemory_MultiIP_DifferentAnchor(t *testing.T) {
	ctx := setupCtx(t)
	si := 5
	// IP-A: long-running, reports every 5s from t=0
	setRawUpfData(ctx, "corr-1", "1.1.1.1", []nwdaf_context.UpfDataPoint{
		makeDP(25, 10, 0),
		makeDP(30, 20, 0),
	})
	// IP-B: joined late, first report at t=27
	setRawUpfData(ctx, "corr-2", "2.2.2.2", []nwdaf_context.UpfDataPoint{
		makeDP(27, 5, 0),
	})

	result, _ := alignAndZipInMemory([]string{"corr-1", "corr-2"}, ctx, 5, si, snappedTs(30))

	if len(result) != 2 {
		t.Fatalf("len=%d, want 2", len(result))
	}
	// Slot global -1: IP-A center=25, IP-B center=27 → UlVol=10+5=15, mean Ts=26
	if result[0].UlVol != 15 {
		t.Errorf("slot -1 UlVol=%v, want 15 (IP-A+IP-B summed)", result[0].UlVol)
	}
	wantTs0 := snappedTs(26).UTC().Format(time.RFC3339)
	if result[0].Ts != wantTs0 {
		t.Errorf("slot -1 Ts=%v, want %v (mean of 25,27)", result[0].Ts, wantTs0)
	}
	// Slot global 0: only IP-A center=30 → UlVol=20
	if result[1].UlVol != 20 {
		t.Errorf("slot 0 UlVol=%v, want 20 (only IP-A)", result[1].UlVol)
	}
}

// TestAggregateInMemory_InputWindowCap verifies that only the last inputWindow
// slots are returned when there are more slots than inputWindow.
func TestAggregateInMemory_InputWindowCap(t *testing.T) {
	ctx := setupCtx(t)
	si := 5
	pts := make([]nwdaf_context.UpfDataPoint, 10)
	for i := range pts {
		pts[i] = makeDP(int64(i*si), int64(i+1), 0)
	}
	setRawUpfData(ctx, "corr-1", "1.1.1.1", pts)

	result, _ := alignAndZipInMemory([]string{"corr-1"}, ctx, 4, si, snappedTs(int64(9*si)))

	if len(result) != 4 {
		t.Fatalf("len=%d, want 4 (capped at inputWindow)", len(result))
	}
	// Last 4 slots: UlVol = 7,8,9,10
	wantUl := []float64{7, 8, 9, 10}
	for i, r := range result {
		if r.UlVol != wantUl[i] {
			t.Errorf("[%d] UlVol=%v, want %v", i, r.UlVol, wantUl[i])
		}
	}
}
