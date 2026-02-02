package context

import (
	"sync"
	"testing"
	"time"
)

// =============================================================================
// TrafficDataBucket Tests
// =============================================================================

func TestTrafficDataBucket_Basic(t *testing.T) {
	Init()

	bucket := NewTrafficDataBucket("test-corr-001")

	if bucket.CorrelationId != "test-corr-001" {
		t.Errorf("CorrelationId = %q, want %q", bucket.CorrelationId, "test-corr-001")
	}

	if bucket.Count() != 0 {
		t.Errorf("Initial count = %d, want 0", bucket.Count())
	}
}

func TestTrafficDataBucket_GetOrCreate(t *testing.T) {
	Init()

	bucket := NewTrafficDataBucket("test-corr-001")

	// First call should create
	data1 := bucket.GetOrCreate("192.168.1.1")
	if data1 == nil {
		t.Fatal("GetOrCreate returned nil")
	}
	if data1.IpAddress != "192.168.1.1" {
		t.Errorf("IpAddress = %q, want %q", data1.IpAddress, "192.168.1.1")
	}
	if data1.CorrelationId != "test-corr-001" {
		t.Errorf("CorrelationId = %q, want %q", data1.CorrelationId, "test-corr-001")
	}

	// Second call should return same instance
	data2 := bucket.GetOrCreate("192.168.1.1")
	if data1 != data2 {
		t.Error("GetOrCreate should return same instance for same IP")
	}

	// Different IP should create new instance
	data3 := bucket.GetOrCreate("192.168.1.2")
	if data1 == data3 {
		t.Error("Different IPs should have different instances")
	}

	if bucket.Count() != 2 {
		t.Errorf("Count = %d, want 2", bucket.Count())
	}
}

func TestTrafficDataBucket_Get(t *testing.T) {
	Init()

	bucket := NewTrafficDataBucket("test-corr-001")

	// Get on non-existent should return nil
	if bucket.Get("192.168.1.1") != nil {
		t.Error("Get on non-existent IP should return nil")
	}

	// After create, Get should return data
	bucket.GetOrCreate("192.168.1.1")
	data := bucket.Get("192.168.1.1")
	if data == nil {
		t.Error("Get after create should return data")
	}
}

func TestTrafficDataBucket_GetAll(t *testing.T) {
	Init()

	bucket := NewTrafficDataBucket("test-corr-001")

	ips := []string{"192.168.1.1", "192.168.1.2", "10.0.0.1"}
	for _, ip := range ips {
		bucket.GetOrCreate(ip)
	}

	all := bucket.GetAll()
	if len(all) != 3 {
		t.Fatalf("GetAll length = %d, want 3", len(all))
	}

	// Verify all IPs are present
	ipSet := make(map[string]bool)
	for _, data := range all {
		ipSet[data.IpAddress] = true
	}
	for _, ip := range ips {
		if !ipSet[ip] {
			t.Errorf("IP %s not found in GetAll result", ip)
		}
	}
}

func TestTrafficDataBucket_Delete(t *testing.T) {
	Init()

	bucket := NewTrafficDataBucket("test-corr-001")
	bucket.GetOrCreate("192.168.1.1")
	bucket.GetOrCreate("192.168.1.2")

	if bucket.Count() != 2 {
		t.Fatalf("Count before delete = %d, want 2", bucket.Count())
	}

	bucket.Delete("192.168.1.1")

	if bucket.Count() != 1 {
		t.Errorf("Count after delete = %d, want 1", bucket.Count())
	}

	if bucket.Get("192.168.1.1") != nil {
		t.Error("Deleted IP should not be found")
	}

	if bucket.Get("192.168.1.2") == nil {
		t.Error("Non-deleted IP should still exist")
	}
}

func TestTrafficDataBucket_GetIpAddresses(t *testing.T) {
	Init()

	bucket := NewTrafficDataBucket("test-corr-001")
	bucket.GetOrCreate("192.168.1.1")
	bucket.GetOrCreate("192.168.1.2")

	ips := bucket.GetIpAddresses()
	if len(ips) != 2 {
		t.Fatalf("GetIpAddresses length = %d, want 2", len(ips))
	}
}

func TestTrafficDataBucket_Concurrent(t *testing.T) {
	Init()

	bucket := NewTrafficDataBucket("test-corr-001")
	var wg sync.WaitGroup
	numGoroutines := 100

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			ip := "192.168.1.1" // All goroutines use same IP
			data := bucket.GetOrCreate(ip)
			data.Lock()
			data.Supi = "test-supi"
			data.Unlock()
		}(i)
	}
	wg.Wait()

	// Should only have one entry (all used same IP)
	if bucket.Count() != 1 {
		t.Errorf("Count = %d, want 1", bucket.Count())
	}
}

// =============================================================================
// TrafficData Tests
// =============================================================================

func TestTrafficData_EnrichWithSupi(t *testing.T) {
	Init()

	data := &TrafficData{
		IpAddress: "192.168.1.1",
	}

	// First enrichment should succeed
	if !data.EnrichWithSupi("imsi-001") {
		t.Error("First enrichment should return true")
	}
	if data.Supi != "imsi-001" {
		t.Errorf("Supi = %q, want %q", data.Supi, "imsi-001")
	}

	// Second enrichment should not overwrite
	if data.EnrichWithSupi("imsi-002") {
		t.Error("Second enrichment should return false")
	}
	if data.Supi != "imsi-001" {
		t.Errorf("Supi = %q, want %q (should not change)", data.Supi, "imsi-001")
	}
}

func TestTrafficData_AppendDataPoint(t *testing.T) {
	Init()

	data := &TrafficData{
		IpAddress:  "192.168.1.1",
		RawUpfData: make([]UpfDataPoint, 0),
	}

	ts := time.Now()
	point := UpfDataPoint{
		Timestamp: ts,
		UlVolume:  1000,
		DlVolume:  2000,
	}

	data.AppendDataPoint(point)

	if len(data.RawUpfData) != 1 {
		t.Fatalf("RawUpfData length = %d, want 1", len(data.RawUpfData))
	}

	if data.RawUpfData[0].UlVolume != 1000 {
		t.Errorf("UlVolume = %d, want 1000", data.RawUpfData[0].UlVolume)
	}

	if !data.LastUpdate.Equal(ts) {
		t.Errorf("LastUpdate = %v, want %v", data.LastUpdate, ts)
	}
}

// =============================================================================
// NWDAFContext Traffic Data Methods Tests
// =============================================================================

func TestNWDAFContext_TrafficBucket(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearTrafficDataStore()

	correlationId := "test-corr-001"

	// GetTrafficBucket on non-existent should return nil
	if ctx.GetTrafficBucket(correlationId) != nil {
		t.Error("GetTrafficBucket should return nil for non-existent")
	}

	// GetOrCreateTrafficBucket should create
	bucket := ctx.GetOrCreateTrafficBucket(correlationId)
	if bucket == nil {
		t.Fatal("GetOrCreateTrafficBucket returned nil")
	}

	// Second call should return same instance
	bucket2 := ctx.GetOrCreateTrafficBucket(correlationId)
	if bucket != bucket2 {
		t.Error("GetOrCreateTrafficBucket should return same instance")
	}

	// GetTrafficBucket should now return the bucket
	bucket3 := ctx.GetTrafficBucket(correlationId)
	if bucket3 == nil {
		t.Error("GetTrafficBucket should return created bucket")
	}

	// Delete should remove
	ctx.DeleteTrafficBucket(correlationId)
	if ctx.GetTrafficBucket(correlationId) != nil {
		t.Error("GetTrafficBucket after delete should return nil")
	}
}

func TestNWDAFContext_GetOrCreateTrafficData(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearTrafficDataStore()

	correlationId := "test-corr-001"
	ipAddr := "192.168.1.1"

	data := ctx.GetOrCreateTrafficData(correlationId, ipAddr)
	if data == nil {
		t.Fatal("GetOrCreateTrafficData returned nil")
	}

	if data.CorrelationId != correlationId {
		t.Errorf("CorrelationId = %q, want %q", data.CorrelationId, correlationId)
	}

	if data.IpAddress != ipAddr {
		t.Errorf("IpAddress = %q, want %q", data.IpAddress, ipAddr)
	}
}

func TestNWDAFContext_GetAllTrafficDataForCorrelation(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearTrafficDataStore()

	correlationId := "test-corr-001"

	// Create multiple IPs in same bucket
	ctx.GetOrCreateTrafficData(correlationId, "192.168.1.1")
	ctx.GetOrCreateTrafficData(correlationId, "192.168.1.2")
	ctx.GetOrCreateTrafficData(correlationId, "10.0.0.1")

	all := ctx.GetAllTrafficDataForCorrelation(correlationId)
	if len(all) != 3 {
		t.Fatalf("GetAllTrafficDataForCorrelation length = %d, want 3", len(all))
	}
}

// =============================================================================
// SmfSubscription Tests
// =============================================================================

func TestNWDAFContext_SmfSubscription(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearSmfSubscriptions()

	correlationId := "test-corr-001"
	nwdafSubId := "nwdaf-sub-001"

	// GetSmfSubscription on non-existent should return nil
	if ctx.GetSmfSubscription(correlationId) != nil {
		t.Error("GetSmfSubscription should return nil for non-existent")
	}

	// GetOrCreate should create new subscription
	sub, isNew := ctx.GetOrCreateSmfSubscription(correlationId, nwdafSubId)
	if sub == nil {
		t.Fatal("GetOrCreateSmfSubscription returned nil")
	}
	if !isNew {
		t.Error("First GetOrCreateSmfSubscription should return isNew=true")
	}

	// Second call should return same instance
	sub2, isNew2 := ctx.GetOrCreateSmfSubscription(correlationId, nwdafSubId)
	if sub != sub2 {
		t.Error("GetOrCreateSmfSubscription should return same instance")
	}
	if isNew2 {
		t.Error("Second GetOrCreateSmfSubscription should return isNew=false")
	}

	// Update with details
	sub.Lock()
	sub.TargetType = TargetType_SUPI
	sub.Supi = "imsi-001"
	sub.SmfEndpoint = "http://smf:8080"
	sub.SmfSubId = "smf-sub-001"
	sub.Unlock()

	// Retrieve and verify
	retrieved := ctx.GetSmfSubscription(correlationId)
	if retrieved == nil {
		t.Fatal("GetSmfSubscription should return stored sub")
	}

	if retrieved.TargetType != TargetType_SUPI {
		t.Errorf("TargetType = %q, want %q", retrieved.TargetType, TargetType_SUPI)
	}

	if retrieved.Supi != "imsi-001" {
		t.Errorf("Supi = %q, want %q", retrieved.Supi, "imsi-001")
	}

	// Delete
	ctx.DeleteSmfSubscription(correlationId)
	if ctx.GetSmfSubscription(correlationId) != nil {
		t.Error("GetSmfSubscription after delete should return nil")
	}
}

func TestSmfSubscription_ReferenceCount(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearSmfSubscriptions()

	correlationId := "test-corr-001"
	nwdafSub1 := "nwdaf-sub-001"
	nwdafSub2 := "nwdaf-sub-002"

	// First NWDAF subscription
	sub, isNew := ctx.GetOrCreateSmfSubscription(correlationId, nwdafSub1)
	if !isNew || sub.RefCount != 1 {
		t.Errorf("Initial RefCount = %d, want 1", sub.RefCount)
	}

	// Second NWDAF subscription adds reference
	sub.AddReference(nwdafSub2)
	if sub.RefCount != 2 {
		t.Errorf("RefCount after AddReference = %d, want 2", sub.RefCount)
	}

	// Release first - should not delete
	shouldDelete, _ := ctx.ReleaseSmfSubscription(correlationId, nwdafSub1)
	if shouldDelete {
		t.Error("ReleaseSmfSubscription should return false when refCount > 0")
	}
	if sub.RefCount != 1 {
		t.Errorf("RefCount after first release = %d, want 1", sub.RefCount)
	}

	// Release second - should delete
	shouldDelete, _ = ctx.ReleaseSmfSubscription(correlationId, nwdafSub2)
	if !shouldDelete {
		t.Error("ReleaseSmfSubscription should return true when refCount = 0")
	}

	// Subscription should be gone
	if ctx.GetSmfSubscription(correlationId) != nil {
		t.Error("SmfSubscription should be deleted after all references released")
	}
}

func TestSmfSubscription_UpdateLastSeen(t *testing.T) {
	sub := &SmfSubscription{
		CorrelationId: "test-corr-001",
		CreatedAt:     time.Now(),
	}

	time.Sleep(10 * time.Millisecond)
	sub.UpdateLastSeen()

	if sub.LastUpdate.IsZero() {
		t.Error("LastUpdate should be set after UpdateLastSeen")
	}

	if !sub.LastUpdate.After(sub.CreatedAt) {
		t.Error("LastUpdate should be after CreatedAt")
	}
}

func TestSmfSubscription_ValidateInvariant(t *testing.T) {
	sub := &SmfSubscription{
		CorrelationId: "test-corr-001",
		RefCount:      2,
		NwdafSubIds:   map[string]bool{"sub1": true, "sub2": true},
	}

	if err := sub.ValidateInvariant(); err != nil {
		t.Errorf("ValidateInvariant failed: %v", err)
	}

	// Break invariant
	sub.RefCount = 3
	if err := sub.ValidateInvariant(); err == nil {
		t.Error("ValidateInvariant should fail when RefCount != len(NwdafSubIds)")
	}
}

// =============================================================================
// Unified Query Tests (nwdafSubId → correlationIds → data)
// =============================================================================

func TestGetCorrelationIdsByNwdafSubId(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearNwdafSubResourcesMap()
	ctx.ClearTrafficDataStore()

	nwdafSubId := "nwdaf-sub-001"

	// Add resources for this NWDAF subscription
	ctx.AddNwdafSubResource(nwdafSubId, NwdafSubResource{
		SmfEndpoint:   "http://smf:8080",
		TargetType:    TargetType_SUPI,
		Supi:          "imsi-001",
		CorrelationId: "corr-001",
	})
	ctx.AddNwdafSubResource(nwdafSubId, NwdafSubResource{
		SmfEndpoint:   "http://smf:8080",
		TargetType:    TargetType_GROUP_ID,
		GroupId:       "group-001",
		CorrelationId: "corr-002",
	})

	ids := ctx.GetCorrelationIdsByNwdafSubId(nwdafSubId)
	if len(ids) != 2 {
		t.Fatalf("GetCorrelationIdsByNwdafSubId length = %d, want 2", len(ids))
	}

	// Verify both IDs are present
	idSet := make(map[string]bool)
	for _, id := range ids {
		idSet[id] = true
	}
	if !idSet["corr-001"] || !idSet["corr-002"] {
		t.Errorf("Missing expected correlation IDs: got %v", ids)
	}
}

func TestGetCorrelationIdsByNwdafSubId_Empty(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearNwdafSubResourcesMap()

	ids := ctx.GetCorrelationIdsByNwdafSubId("non-existent")
	if ids != nil {
		t.Errorf("GetCorrelationIdsByNwdafSubId for non-existent should return nil, got %v", ids)
	}
}

func TestGetTrafficBucketsByNwdafSubId(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearNwdafSubResourcesMap()
	ctx.ClearTrafficDataStore()

	nwdafSubId := "nwdaf-sub-001"

	// Add resources
	ctx.AddNwdafSubResource(nwdafSubId, NwdafSubResource{
		CorrelationId: "corr-001",
	})
	ctx.AddNwdafSubResource(nwdafSubId, NwdafSubResource{
		CorrelationId: "corr-002",
	})

	// Create buckets for these correlations
	ctx.GetOrCreateTrafficBucket("corr-001")
	ctx.GetOrCreateTrafficBucket("corr-002")

	buckets := ctx.GetTrafficBucketsByNwdafSubId(nwdafSubId)
	if len(buckets) != 2 {
		t.Fatalf("GetTrafficBucketsByNwdafSubId length = %d, want 2", len(buckets))
	}
}

func TestGetTrafficDataByNwdafSubId(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearNwdafSubResourcesMap()
	ctx.ClearTrafficDataStore()

	nwdafSubId := "nwdaf-sub-001"

	// Add resource
	ctx.AddNwdafSubResource(nwdafSubId, NwdafSubResource{
		CorrelationId: "corr-001",
	})

	// Create traffic data
	bucket := ctx.GetOrCreateTrafficBucket("corr-001")
	bucket.GetOrCreate("192.168.1.1")
	bucket.GetOrCreate("192.168.1.2")

	data := ctx.GetTrafficDataByNwdafSubId(nwdafSubId)
	if len(data) != 2 {
		t.Fatalf("GetTrafficDataByNwdafSubId length = %d, want 2", len(data))
	}
}

func TestGetTrafficDataByNwdafSubId_Empty(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearNwdafSubResourcesMap()
	ctx.ClearTrafficDataStore()

	data := ctx.GetTrafficDataByNwdafSubId("non-existent")
	if data != nil {
		t.Errorf("GetTrafficDataByNwdafSubId for non-existent should return nil, got %v", data)
	}
}

func TestGetTrafficDataByNwdafSubId_MultipleCorrelations(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearNwdafSubResourcesMap()
	ctx.ClearTrafficDataStore()

	nwdafSubId := "nwdaf-sub-001"

	// Add two correlation IDs for same NWDAF subscription
	ctx.AddNwdafSubResource(nwdafSubId, NwdafSubResource{CorrelationId: "corr-001"})
	ctx.AddNwdafSubResource(nwdafSubId, NwdafSubResource{CorrelationId: "corr-002"})

	// Create traffic data in each bucket
	bucket1 := ctx.GetOrCreateTrafficBucket("corr-001")
	bucket1.GetOrCreate("192.168.1.1")
	bucket1.GetOrCreate("192.168.1.2")

	bucket2 := ctx.GetOrCreateTrafficBucket("corr-002")
	bucket2.GetOrCreate("10.0.0.1")

	data := ctx.GetTrafficDataByNwdafSubId(nwdafSubId)
	if len(data) != 3 {
		t.Fatalf("GetTrafficDataByNwdafSubId length = %d, want 3", len(data))
	}

	// Verify all IPs are present
	ipSet := make(map[string]bool)
	for _, d := range data {
		ipSet[d.IpAddress] = true
	}
	if !ipSet["192.168.1.1"] || !ipSet["192.168.1.2"] || !ipSet["10.0.0.1"] {
		t.Errorf("Missing expected IPs: got %v", ipSet)
	}
}

func TestGetTrafficDataByNwdafSubId_NoBucket(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearNwdafSubResourcesMap()
	ctx.ClearTrafficDataStore()

	nwdafSubId := "nwdaf-sub-001"

	// Add resource but don't create bucket
	ctx.AddNwdafSubResource(nwdafSubId, NwdafSubResource{CorrelationId: "corr-001"})

	// Should return nil (bucket doesn't exist yet)
	data := ctx.GetTrafficDataByNwdafSubId(nwdafSubId)
	if data != nil {
		t.Errorf("GetTrafficDataByNwdafSubId with no bucket should return nil, got %v", data)
	}
}
