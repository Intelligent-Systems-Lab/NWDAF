package context

import (
	"testing"

	"github.com/free5gc/openapi/models"
)

func TestNewSubscriptionId(t *testing.T) {
	id1 := NewSubscriptionId()
	id2 := NewSubscriptionId()

	if id1 == "" {
		t.Error("NewSubscriptionId() returned empty string")
	}

	if id1 == id2 {
		t.Error("NewSubscriptionId() returned duplicate IDs")
	}
}

func TestSubscriptionCRUD(t *testing.T) {
	// Initialize context
	Init()
	ctx := GetSelf()

	// Test Add
	sub := &Subscription{
		ID:              NewSubscriptionId(),
		NotificationURI: "http://localhost:9090/callback",
		EventSubs: []models.NwdafEventsSubscriptionEventSubscription{
			{
				Event: models.NwdafEvent_ABNORMAL_BEHAVIOUR,
			},
		},
	}

	ctx.AddSubscription(sub)

	// Test Get
	retrieved := ctx.GetSubscription(sub.ID)
	if retrieved == nil {
		t.Fatalf("GetSubscription() returned nil for existing subscription")
	}

	if retrieved.NotificationURI != sub.NotificationURI {
		t.Errorf("GetSubscription() NotificationURI = %v, want %v",
			retrieved.NotificationURI, sub.NotificationURI)
	}

	// Test Update
	sub.NotificationURI = "http://localhost:9091/new-callback"
	success := ctx.UpdateSubscription(sub)
	if !success {
		t.Error("UpdateSubscription() returned false for existing subscription")
	}

	updated := ctx.GetSubscription(sub.ID)
	if updated.NotificationURI != "http://localhost:9091/new-callback" {
		t.Errorf("UpdateSubscription() failed to update NotificationURI")
	}

	// Test Count
	count := ctx.SubscriptionCount()
	if count != 1 {
		t.Errorf("SubscriptionCount() = %v, want 1", count)
	}

	// Test Delete
	success = ctx.DeleteSubscription(sub.ID)
	if !success {
		t.Error("DeleteSubscription() returned false for existing subscription")
	}

	// Verify deletion
	deleted := ctx.GetSubscription(sub.ID)
	if deleted != nil {
		t.Error("GetSubscription() should return nil after deletion")
	}

	// Test delete non-existent
	success = ctx.DeleteSubscription("non-existent-id")
	if success {
		t.Error("DeleteSubscription() should return false for non-existent subscription")
	}
}

func TestGetAllSubscriptions(t *testing.T) {
	Init()
	ctx := GetSelf()

	// Add multiple subscriptions
	for i := 0; i < 3; i++ {
		ctx.AddSubscription(&Subscription{
			ID:              NewSubscriptionId(),
			NotificationURI: "http://localhost/callback",
		})
	}

	subs := ctx.GetAllSubscriptions()
	if len(subs) != 3 {
		t.Errorf("GetAllSubscriptions() returned %v subscriptions, want 3", len(subs))
	}
}
