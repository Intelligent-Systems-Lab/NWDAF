package context

import (
	"sync"
	"testing"
	"time"
)

// TestCorrelationToSupiCRUD tests correlation to SUPI mapping operations
func TestCorrelationToSupiCRUD(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearCorrelationToSupiMap()

	correlationId := "uuid-test-001"
	supi := "imsi-208930000000001"

	// Test Store
	ctx.StoreCorrelationToSupi(correlationId, supi)

	// Test Get
	retrieved, ok := ctx.GetSupiByCorrelationId(correlationId)
	if !ok {
		t.Error("GetSupiByCorrelationId() returned false for existing mapping")
	}
	if retrieved != supi {
		t.Errorf("GetSupiByCorrelationId() = %v, want %v", retrieved, supi)
	}

	// Test Get non-existent
	_, ok = ctx.GetSupiByCorrelationId("non-existent-id")
	if ok {
		t.Error("GetSupiByCorrelationId() should return false for non-existent correlationId")
	}

	// Test Delete
	ctx.DeleteCorrelationToSupi(correlationId)
	_, ok = ctx.GetSupiByCorrelationId(correlationId)
	if ok {
		t.Error("GetSupiByCorrelationId() should return false after delete")
	}
}

// TestNwdafSubResourcesCRUD tests NWDAF subscription resource tracking
func TestNwdafSubResourcesCRUD(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearNwdafSubResourcesMap()

	nwdafSubId := "nwdaf-sub-001"

	// Test Add single resource
	resource1 := NwdafSubResource{
		SmfEndpoint:   "http://smf:8080",
		Supi:          "imsi-001",
		CorrelationId: "uuid-AAA",
		CreatedAt:     time.Now(),
	}
	ctx.AddNwdafSubResource(nwdafSubId, resource1)

	// Test Get
	resources := ctx.GetNwdafSubResources(nwdafSubId)
	if len(resources) != 1 {
		t.Errorf("GetNwdafSubResources() returned %d resources, want 1", len(resources))
	}
	if resources[0].Supi != "imsi-001" {
		t.Errorf("Resource Supi = %v, want imsi-001", resources[0].Supi)
	}

	// Test Add second resource
	resource2 := NwdafSubResource{
		SmfEndpoint:   "http://smf:8080",
		Supi:          "imsi-002",
		CorrelationId: "uuid-BBB",
		CreatedAt:     time.Now(),
	}
	ctx.AddNwdafSubResource(nwdafSubId, resource2)

	resources = ctx.GetNwdafSubResources(nwdafSubId)
	if len(resources) != 2 {
		t.Errorf("GetNwdafSubResources() returned %d resources, want 2", len(resources))
	}

	// Test Get non-existent
	noResources := ctx.GetNwdafSubResources("non-existent-sub")
	if noResources != nil && len(noResources) > 0 {
		t.Error("GetNwdafSubResources() should return nil for non-existent subscription")
	}

	// Test Delete
	ctx.DeleteNwdafSubResources(nwdafSubId)
	resources = ctx.GetNwdafSubResources(nwdafSubId)
	if resources != nil && len(resources) > 0 {
		t.Error("GetNwdafSubResources() should return nil after delete")
	}
}

// TestMultipleSubscriptionsSharedResource tests the scenario where
// multiple NWDAF subscriptions share the same SMF resource
func TestMultipleSubscriptionsSharedResource(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearCorrelationToSupiMap()
	ctx.ClearNwdafSubResourcesMap()
	ctx.ClearSmfResources()

	smfEndpoint := "http://smf:8080"
	supi := "imsi-shared-001"
	correlationId := "uuid-shared"

	// === Consumer A subscribes ===
	nwdafSubA := "nwdaf-sub-A"

	// First subscription creates SMF resource
	resourceA, isNewA := ctx.GetOrCreateSmfResource(smfEndpoint, supi, nwdafSubA)
	if !isNewA {
		t.Error("First subscription should create new resource")
	}

	// Store correlation mapping (only on new)
	ctx.StoreCorrelationToSupi(correlationId, supi)

	// Update resource
	resourceA.Lock()
	resourceA.SmfSubId = "smf-sub-123"
	resourceA.CorrelationId = correlationId
	resourceA.Unlock()

	// Store cleanup tracking
	ctx.AddNwdafSubResource(nwdafSubA, NwdafSubResource{
		SmfEndpoint:   smfEndpoint,
		Supi:          supi,
		CorrelationId: correlationId,
		CreatedAt:     time.Now(),
	})

	// === Consumer B subscribes (reuse) ===
	nwdafSubB := "nwdaf-sub-B"

	_, isNewB := ctx.GetOrCreateSmfResource(smfEndpoint, supi, nwdafSubB)
	if isNewB {
		t.Error("Second subscription should reuse existing resource")
	}

	// Store cleanup tracking (each subscription tracks independently)
	ctx.AddNwdafSubResource(nwdafSubB, NwdafSubResource{
		SmfEndpoint:   smfEndpoint,
		Supi:          supi,
		CorrelationId: correlationId,
		CreatedAt:     time.Now(),
	})

	// === Verify both have separate tracking ===
	resourcesA := ctx.GetNwdafSubResources(nwdafSubA)
	resourcesB := ctx.GetNwdafSubResources(nwdafSubB)

	if len(resourcesA) != 1 {
		t.Errorf("Consumer A should have 1 resource, got %d", len(resourcesA))
	}
	if len(resourcesB) != 1 {
		t.Errorf("Consumer B should have 1 resource, got %d", len(resourcesB))
	}

	// === Consumer A deletes ===
	for _, res := range resourcesA {
		shouldDelete, _ := ctx.ReleaseSmfResource(res.SmfEndpoint, res.Supi, nwdafSubA)
		if shouldDelete {
			t.Error("Should not delete SMF resource - B is still using it")
		}
	}
	ctx.DeleteNwdafSubResources(nwdafSubA)

	// Verify A's tracking is deleted but B's remains
	if ctx.GetNwdafSubResources(nwdafSubA) != nil {
		t.Error("Consumer A's resources should be deleted")
	}
	if len(ctx.GetNwdafSubResources(nwdafSubB)) != 1 {
		t.Error("Consumer B's resources should still exist")
	}

	// Correlation mapping should still exist (B is still using)
	_, ok := ctx.GetSupiByCorrelationId(correlationId)
	if !ok {
		t.Error("Correlation mapping should still exist")
	}

	// === Consumer B deletes ===
	for _, res := range resourcesB {
		shouldDelete, _ := ctx.ReleaseSmfResource(res.SmfEndpoint, res.Supi, nwdafSubB)
		if !shouldDelete {
			t.Error("Should delete SMF resource - last reference released")
		}
		// Only delete correlation mapping when last reference is released
		ctx.DeleteCorrelationToSupi(res.CorrelationId)
	}
	ctx.DeleteNwdafSubResources(nwdafSubB)

	// Verify both are fully cleaned up
	_, ok = ctx.GetSupiByCorrelationId(correlationId)
	if ok {
		t.Error("Correlation mapping should be deleted after last reference")
	}

	_, ok = ctx.GetSmfResource(smfEndpoint, supi)
	if ok {
		t.Error("SMF resource should be deleted after last reference")
	}
}

// TestConcurrentCorrelationAccess tests thread safety
func TestConcurrentCorrelationAccess(t *testing.T) {
	Init()
	ctx := GetSelf()
	ctx.ClearCorrelationToSupiMap()

	var wg sync.WaitGroup

	// 50 goroutines writing different correlationIds
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			corrId := "uuid-" + string(rune('A'+id))
			supi := "imsi-" + string(rune('0'+id))
			ctx.StoreCorrelationToSupi(corrId, supi)
		}(i)
	}
	wg.Wait()

	// 50 goroutines reading
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			corrId := "uuid-" + string(rune('A'+id))
			_, _ = ctx.GetSupiByCorrelationId(corrId)
		}(i)
	}
	wg.Wait()

	// No race conditions should occur (test with -race flag)
}
