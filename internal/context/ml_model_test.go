package context

import (
	"testing"

	"github.com/free5gc/openapi/models"
)

// ============================================================================
// SharedModelInfo — Basic Operations
// ============================================================================

func TestSharedModelInfo_New(t *testing.T) {
	shared := NewSharedModelInfo("file:///model.pth", models.NwdafEvent_UE_COMMUNICATION)

	if shared.ModelUrl != "file:///model.pth" {
		t.Errorf("ModelUrl = %s, want file:///model.pth", shared.ModelUrl)
	}
	if shared.Event != models.NwdafEvent_UE_COMMUNICATION {
		t.Errorf("Event = %v, want UE_COMMUNICATION", shared.Event)
	}
	if shared.SubscriberCount() != 0 {
		t.Errorf("SubscriberCount() = %d, want 0", shared.SubscriberCount())
	}
}

// ============================================================================
// SharedModelInfo — Subscriber Management
// ============================================================================

func TestSharedModelInfo_AddRemoveSubscriber(t *testing.T) {
	shared := NewSharedModelInfo("file:///model.pth", models.NwdafEvent_UE_COMMUNICATION)

	// Add first subscriber
	count := shared.AddSubscriber("sub-001")
	if count != 1 {
		t.Errorf("AddSubscriber(sub-001) = %d, want 1", count)
	}

	// Add second subscriber
	count = shared.AddSubscriber("sub-002")
	if count != 2 {
		t.Errorf("AddSubscriber(sub-002) = %d, want 2", count)
	}

	// Remove first
	remaining := shared.RemoveSubscriber("sub-001")
	if remaining != 1 {
		t.Errorf("RemoveSubscriber(sub-001) = %d, want 1", remaining)
	}

	// Remove second
	remaining = shared.RemoveSubscriber("sub-002")
	if remaining != 0 {
		t.Errorf("RemoveSubscriber(sub-002) = %d, want 0", remaining)
	}
}

func TestSharedModelInfo_DuplicateSubscriber(t *testing.T) {
	shared := NewSharedModelInfo("file:///model.pth", models.NwdafEvent_UE_COMMUNICATION)

	shared.AddSubscriber("sub-001")
	count := shared.AddSubscriber("sub-001") // duplicate

	// map-based, so duplicates overwrite — count stays 1
	if count != 1 {
		t.Errorf("Duplicate AddSubscriber = %d, want 1 (idempotent)", count)
	}
}

func TestSharedModelInfo_RemoveNonExistent(t *testing.T) {
	shared := NewSharedModelInfo("file:///model.pth", models.NwdafEvent_UE_COMMUNICATION)

	shared.AddSubscriber("sub-001")
	remaining := shared.RemoveSubscriber("sub-999")

	if remaining != 1 {
		t.Errorf("Remove non-existent subscriber = %d, want 1 (no change)", remaining)
	}
}

// ============================================================================
// Context — SharedModel Registry
// ============================================================================

func TestContext_SharedModelRegistry(t *testing.T) {
	Init()
	ctx := GetSelf()

	// GetOrCreate — new
	shared, isNew := ctx.GetOrCreateSharedModel("file:///model-A.pth", models.NwdafEvent_UE_COMMUNICATION)
	if !isNew {
		t.Error("First GetOrCreateSharedModel should return isNew=true")
	}
	if shared == nil {
		t.Fatal("GetOrCreateSharedModel returned nil")
	}

	// GetOrCreate — existing
	shared2, isNew2 := ctx.GetOrCreateSharedModel("file:///model-A.pth", models.NwdafEvent_UE_COMMUNICATION)
	if isNew2 {
		t.Error("Second GetOrCreateSharedModel should return isNew=false")
	}
	if shared2 != shared {
		t.Error("Should return the same SharedModelInfo instance")
	}

	// Get
	got := ctx.GetSharedModel("file:///model-A.pth")
	if got != shared {
		t.Error("GetSharedModel should return the same instance")
	}

	// Get non-existent
	got = ctx.GetSharedModel("file:///non-existent.pth")
	if got != nil {
		t.Error("GetSharedModel for non-existent should return nil")
	}

	// Delete
	ctx.DeleteSharedModel("file:///model-A.pth")
	got = ctx.GetSharedModel("file:///model-A.pth")
	if got != nil {
		t.Error("After delete, GetSharedModel should return nil")
	}
}

func TestContext_SharedModelRegistry_MultipleModels(t *testing.T) {
	Init()
	ctx := GetSelf()

	sharedA, _ := ctx.GetOrCreateSharedModel("file:///model-A.pth", models.NwdafEvent_UE_COMMUNICATION)
	sharedB, _ := ctx.GetOrCreateSharedModel("file:///model-B.pth", models.NwdafEvent_UE_COMMUNICATION)

	if sharedA == sharedB {
		t.Error("Different modelUrls should create different SharedModelInfo instances")
	}

	if ctx.GetSharedModel("file:///model-A.pth") != sharedA {
		t.Error("Model A correlation should remain addressable")
	}
	if ctx.GetSharedModel("file:///model-B.pth") != sharedB {
		t.Error("Model B correlation should remain addressable")
	}
}

// ============================================================================
// Context — ModelAccuracyStore Registry
// ============================================================================

func TestContext_ModelAccuracyStoreRegistry(t *testing.T) {
	Init()
	ctx := GetSelf()

	// GetOrCreate — new
	store, isNew := ctx.GetOrCreateModelAccuracyStore("file:///model-A.pth")
	if !isNew {
		t.Error("First GetOrCreateModelAccuracyStore should return isNew=true")
	}
	if store == nil {
		t.Fatal("Returned nil store")
	}

	// GetOrCreate — existing
	store2, isNew2 := ctx.GetOrCreateModelAccuracyStore("file:///model-A.pth")
	if isNew2 {
		t.Error("Second call should return isNew=false")
	}
	if store2 != store {
		t.Error("Should return same instance")
	}

	// Get
	got := ctx.GetModelAccuracyStore("file:///model-A.pth")
	if got != store {
		t.Error("GetModelAccuracyStore should return same instance")
	}

	// Delete — should also stop monitor
	canceled := false
	store.TryStartMonitor(func() { canceled = true })
	ctx.DeleteModelAccuracyStore("file:///model-A.pth")

	if !canceled {
		t.Error("DeleteModelAccuracyStore should stop the monitor")
	}
	if ctx.GetModelAccuracyStore("file:///model-A.pth") != nil {
		t.Error("After delete, store should be nil")
	}
}
