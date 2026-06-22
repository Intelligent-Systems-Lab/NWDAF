package context

import (
	"testing"

	"github.com/free5gc/openapi/models"
)

type stubScheduler struct {
	stopCalls int
}

func (s *stubScheduler) Stop() {
	s.stopCalls++
}

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
	if retrieved := ctx.GetSubscription(sub.ID); retrieved == nil {
		t.Fatalf("GetSubscription() returned nil for existing subscription")
	} else if retrieved.NotificationURI != sub.NotificationURI {
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

func TestStopAllSubscriptionSchedulers(t *testing.T) {
	Init()
	ctx := GetSelf()

	schedulerA := &stubScheduler{}
	schedulerB := &stubScheduler{}

	ctx.AddSubscription(&Subscription{
		ID:        "sub-1",
		Scheduler: schedulerA,
	})
	ctx.AddSubscription(&Subscription{
		ID:        "sub-2",
		Scheduler: schedulerB,
	})
	ctx.AddSubscription(&Subscription{
		ID: "sub-3",
	})

	stopped := ctx.StopAllSubscriptionSchedulers()
	if stopped != 2 {
		t.Fatalf("StopAllSubscriptionSchedulers() = %d, want 2", stopped)
	}

	if schedulerA.stopCalls != 1 || schedulerB.stopCalls != 1 {
		t.Fatalf("expected each scheduler to be stopped once, got %d and %d",
			schedulerA.stopCalls, schedulerB.stopCalls)
	}

	if sub := ctx.GetSubscription("sub-1"); sub == nil || sub.Scheduler != nil {
		t.Fatalf("subscription sub-1 scheduler should be cleared")
	}
	if sub := ctx.GetSubscription("sub-2"); sub == nil || sub.Scheduler != nil {
		t.Fatalf("subscription sub-2 scheduler should be cleared")
	}
}
