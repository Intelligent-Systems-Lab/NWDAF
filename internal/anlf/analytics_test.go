package anlf

import (
	"testing"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
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

func makeUeCommunicationSub(
	id string,
	tgtUe *models.TargetUeInformation,
) *nwdaf_context.Subscription {
	return &nwdaf_context.Subscription{
		ID: id,
		EventSubs: []models.NwdafEventsSubscriptionEventSubscription{
			{
				Event: models.NwdafEvent_UE_COMMUNICATION,
				TgtUe: tgtUe,
			},
		},
	}
}

// --- tests ---

func TestBaseTargetTimeFromHistorical_AddsSamplingInterval(t *testing.T) {
	got, err := baseTargetTimeFromHistorical("1970-01-01T00:01:40Z", 5)
	if err != nil {
		t.Fatalf("baseTargetTimeFromHistorical() error = %v, want nil", err)
	}
	want := snappedTs(105)
	if !got.Equal(want) {
		t.Fatalf("baseTargetTimeFromHistorical() = %v, want %v", got, want)
	}
}

func TestPredictionTargetTime_StepZeroStartsAtBaseTargetTime(t *testing.T) {
	baseTargetTime := snappedTs(100)

	got := predictionTargetTime(baseTargetTime, 5, 0)

	if !got.Equal(baseTargetTime) {
		t.Fatalf("predictionTargetTime(step=0) = %v, want %v", got, baseTargetTime)
	}
}

func TestPredictionTargetTime_MultiStepAdvancesBySamplingInterval(t *testing.T) {
	baseTargetTime := snappedTs(100)

	got := predictionTargetTime(baseTargetTime, 5, 3)
	want := snappedTs(115)

	if !got.Equal(want) {
		t.Fatalf("predictionTargetTime(step=3) = %v, want %v", got, want)
	}
}

func TestResolveMonitoringScope_SupisSortedAndCompacted(t *testing.T) {
	ctx := setupCtx(t)
	sub := makeUeCommunicationSub("sub-supis", &models.TargetUeInformation{
		Supis: []string{"imsi-002", "imsi-001", "imsi-002", "  imsi-001  "},
	})
	ctx.AddSubscription(sub)

	scopeKey, ok := resolveMonitoringScope(sub.ID, ctx)
	if !ok {
		t.Fatal("resolveMonitoringScope() returned ok=false, want true")
	}
	if scopeKey != "supis:imsi-001,imsi-002" {
		t.Fatalf("resolveMonitoringScope() = %q, want %q",
			scopeKey, "supis:imsi-001,imsi-002")
	}
}

func TestResolveMonitoringScope_GroupsSortedAndCompacted(t *testing.T) {
	ctx := setupCtx(t)
	sub := makeUeCommunicationSub("sub-groups", &models.TargetUeInformation{
		IntGroupIds: []string{"group-b", "group-a", "group-b", " group-a "},
	})
	ctx.AddSubscription(sub)

	scopeKey, ok := resolveMonitoringScope(sub.ID, ctx)
	if !ok {
		t.Fatal("resolveMonitoringScope() returned ok=false, want true")
	}
	if scopeKey != "groups:group-a,group-b" {
		t.Fatalf("resolveMonitoringScope() = %q, want %q",
			scopeKey, "groups:group-a,group-b")
	}
}

func TestResolveMonitoringScope_MixedTargets(t *testing.T) {
	ctx := setupCtx(t)
	sub := makeUeCommunicationSub("sub-mixed", &models.TargetUeInformation{
		IntGroupIds: []string{"group-b", "group-a"},
		Supis:       []string{"imsi-002", "imsi-001"},
	})
	ctx.AddSubscription(sub)

	scopeKey, ok := resolveMonitoringScope(sub.ID, ctx)
	if !ok {
		t.Fatal("resolveMonitoringScope() returned ok=false, want true")
	}
	if scopeKey != "target:groups=group-a,group-b;supis=imsi-001,imsi-002" {
		t.Fatalf("resolveMonitoringScope() = %q, want %q",
			scopeKey, "target:groups=group-a,group-b;supis=imsi-001,imsi-002")
	}
}

func TestResolveMonitoringScope_FallsBackToTrackedResources(t *testing.T) {
	ctx := setupCtx(t)
	ctx.AddNwdafSubResource("sub-resource", nwdaf_context.NwdafSubResource{
		Supi:            "imsi-group-001",
		CorrelationId:   "corr-1",
		OriginalGroupId: "group-a",
	})
	ctx.AddNwdafSubResource("sub-resource", nwdaf_context.NwdafSubResource{
		Supi:            "imsi-group-002",
		CorrelationId:   "corr-2",
		OriginalGroupId: "group-a",
	})

	scopeKey, ok := resolveMonitoringScope("sub-resource", ctx)
	if !ok {
		t.Fatal("resolveMonitoringScope() returned ok=false, want true")
	}
	if scopeKey != "group:group-a" {
		t.Fatalf("resolveMonitoringScope() = %q, want %q", scopeKey, "group:group-a")
	}
}

func TestResolveMonitoringScope_AnyUeUnsupported(t *testing.T) {
	ctx := setupCtx(t)
	sub := makeUeCommunicationSub("sub-anyue", &models.TargetUeInformation{
		AnyUe: true,
	})
	ctx.AddSubscription(sub)

	scopeKey, ok := resolveMonitoringScope(sub.ID, ctx)
	if ok {
		t.Fatalf("resolveMonitoringScope() = %q, want unresolved scope", scopeKey)
	}
}

func TestPredictionScopeKeySnapshotSurvivesSubscriptionUpdate(t *testing.T) {
	ctx := setupCtx(t)
	sub := makeUeCommunicationSub("sub-snapshot", &models.TargetUeInformation{
		IntGroupIds: []string{"group-a"},
	})
	ctx.AddSubscription(sub)

	scopeKey, ok := resolveMonitoringScope(sub.ID, ctx)
	if !ok {
		t.Fatal("resolveMonitoringScope() returned ok=false, want true")
	}

	store := nwdaf_context.NewModelAccuracyStore("file:///test/model.pth")
	store.AddPrediction(nwdaf_context.PredictionRecord{
		ModelUrl:       "file:///test/model.pth",
		PredictedAt:    time.Now(),
		TargetTime:     time.Now().Add(-time.Second),
		TargetSlotTime: time.Now().Add(-time.Second),
		PredUlVol:      100,
		PredDlVol:      200,
		NwdafSubId:     sub.ID,
		ScopeKey:       scopeKey,
	})

	sub.EventSubs[0].TgtUe = &models.TargetUeInformation{
		Supis: []string{"imsi-999"},
	}
	if !ctx.UpdateSubscription(sub) {
		t.Fatal("UpdateSubscription() returned false, want true")
	}

	snapshot := store.SnapshotPredictions()
	if len(snapshot) != 1 {
		t.Fatalf("SnapshotPredictions() returned %d records, want 1", len(snapshot))
	}
	if snapshot[0].ScopeKey != "group:group-a" {
		t.Fatalf("snapshot[0].ScopeKey = %q, want %q", snapshot[0].ScopeKey, "group:group-a")
	}
}

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

// TestAggregateInMemory_ZeroPadding verifies that missing slots within the output
// range are filled with zero-valued observations so the ML model receives an
// evenly-spaced sequence.
//
// si=5, snappedNow=20, inputWindow=4
// IP-A reports at t=0 (global -4) and t=10 (global -2), skipping t=5 (global -3).
// Expected output (4 slots): [t=0: UlVol=10], [t=5: zero], [t=10: UlVol=11], [t=15: zero... wait
// Actually startIdx = max(keys[0], endIdx-inputWindow+1) = max(-4, -2-4+1) = max(-4,-5) = -4
// endIdx=-2, range = -4,-3,-2 → 3 slots (inputWindow=4 but data only spans 3 slots from -4 to -2)
// slot -4: UlVol=10, slot -3: zero, slot -2: UlVol=11
func TestAggregateInMemory_ZeroPadding(t *testing.T) {
	ctx := setupCtx(t)
	si := 5
	setRawUpfData(ctx, "corr-1", "1.1.1.1", []nwdaf_context.UpfDataPoint{
		makeDP(0, 10, 20),
		makeDP(10, 11, 22), // skip t=5
	})

	result, _ := alignAndZipInMemory([]string{"corr-1"}, ctx, 4, si, snappedTs(20))

	// 3 slots: t=0 (data), t=5 (zero-pad), t=10 (data)
	if len(result) != 3 {
		t.Fatalf("len=%d, want 3 (gap filled with zero)", len(result))
	}
	if result[0].UlVol != 10 {
		t.Errorf("[0] UlVol=%v, want 10", result[0].UlVol)
	}
	if result[1].UlVol != 0 {
		t.Errorf("[1] UlVol=%v, want 0 (zero-padded)", result[1].UlVol)
	}
	if result[1].DlVol != 0 {
		t.Errorf("[1] DlVol=%v, want 0 (zero-padded)", result[1].DlVol)
	}
	if result[2].UlVol != 11 {
		t.Errorf("[2] UlVol=%v, want 11", result[2].UlVol)
	}
	// Zero-padded Ts should be snappedNow + idx*si = 20 + (-3)*5 = 5
	wantZeroTs := snappedTs(5).UTC().Format(time.RFC3339)
	if result[1].Ts != wantZeroTs {
		t.Errorf("[1] Ts=%v, want %v (derived from snappedNow)", result[1].Ts, wantZeroTs)
	}
}
