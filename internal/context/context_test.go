package context

import (
	"reflect"
	"testing"

	"github.com/free5gc/openapi/models"
)

func TestReplaceSmfPeerResourceAssociationsIsTargetAwareAndAtomic(t *testing.T) {
	ctx := &NWDAFContext{smfPeerRoutes: make(map[string]SmfPeerResourceRoute)}
	for _, target := range []string{"http://smf-a.example", "http://smf-b.example"} {
		if !ctx.AddSmfPeerResourceRoute(&SmfPeerResourceRoute{
			SubscriptionID:   "shared-peer-id",
			ResourceLocation: target + "/subscriptions/shared-peer-id",
			TargetAPIBaseURI: target,
		}) {
			t.Fatalf("could not add route for %s", target)
		}
	}

	if !ctx.ReplaceSmfPeerResourceAssociations([]SmfPeerResourceAssociation{
		{
			TargetAPIBaseURI:     "http://smf-a.example",
			PeerSubscriptionID:   "shared-peer-id",
			NwdafSubscriptionIDs: []string{"nwdaf-sub-a", "nwdaf-sub-b"},
		},
	}) {
		t.Fatal("ReplaceSmfPeerResourceAssociations() rejected known peer tuple")
	}
	routeA, _ := ctx.GetSmfPeerResourceRoute("http://smf-a.example", "shared-peer-id")
	routeB, _ := ctx.GetSmfPeerResourceRoute("http://smf-b.example", "shared-peer-id")
	if !reflect.DeepEqual(routeA.NwdafSubscriptionIDs, []string{"nwdaf-sub-a", "nwdaf-sub-b"}) {
		t.Fatalf("route A associations = %v", routeA.NwdafSubscriptionIDs)
	}
	if len(routeB.NwdafSubscriptionIDs) != 0 {
		t.Fatalf("route B associations = %v, want cleared", routeB.NwdafSubscriptionIDs)
	}
	if routeA.PendingCleanup {
		t.Fatal("route A marked pending cleanup while it still has an active association")
	}
	if !routeB.PendingCleanup {
		t.Fatal("route B did not become pending cleanup after its associations were cleared")
	}

	if ctx.ReplaceSmfPeerResourceAssociations([]SmfPeerResourceAssociation{
		{
			TargetAPIBaseURI:     "http://unknown.example",
			PeerSubscriptionID:   "shared-peer-id",
			NwdafSubscriptionIDs: []string{"nwdaf-sub-c"},
		},
	}) {
		t.Fatal("ReplaceSmfPeerResourceAssociations() accepted an unknown peer tuple")
	}
	routeA, _ = ctx.GetSmfPeerResourceRoute("http://smf-a.example", "shared-peer-id")
	if !reflect.DeepEqual(routeA.NwdafSubscriptionIDs, []string{"nwdaf-sub-a", "nwdaf-sub-b"}) {
		t.Fatalf("failed replacement partially mutated route A: %v", routeA.NwdafSubscriptionIDs)
	}
	if routeA.PendingCleanup {
		t.Fatal("failed replacement changed route A cleanup state")
	}
}

func TestSubscriptionCompleteRuntime(t *testing.T) {
	tests := []struct {
		name       string
		active     bool
		current    int64
		completion int64
		want       RuntimeCompletionDisposition
		wantActive bool
	}{
		{
			name: "current active", active: true, current: 2, completion: 2,
			want: RuntimeCompletionCompleted, wantActive: false,
		},
		{
			name: "duplicate", active: false, current: 2, completion: 2,
			want: RuntimeCompletionAlreadyInactive, wantActive: false,
		},
		{
			name: "stale", active: true, current: 3, completion: 2,
			want: RuntimeCompletionStale, wantActive: true,
		},
		{
			name: "future", active: true, current: 2, completion: 3,
			want: RuntimeCompletionFuture, wantActive: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			subscription := &Subscription{IsActive: test.active, RuntimeRevision: test.current}
			if got := subscription.CompleteRuntime(test.completion); got != test.want {
				t.Fatalf("disposition = %v, want %v", got, test.want)
			}
			_, _, _, active := subscription.RuntimeSnapshot()
			if active != test.wantActive {
				t.Fatalf("active = %v, want %v", active, test.wantActive)
			}
		})
	}
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
