package context

import (
	"reflect"
	"testing"
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

	accepted, changed := ctx.ReplaceSmfPeerResourceAssociations([]SmfPeerResourceAssociation{
		{
			TargetAPIBaseURI:     "http://smf-a.example",
			PeerSubscriptionID:   "shared-peer-id",
			NwdafSubscriptionIDs: []string{"nwdaf-sub-a", "nwdaf-sub-b"},
		},
	})
	if !accepted || !changed {
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

	accepted, _ = ctx.ReplaceSmfPeerResourceAssociations([]SmfPeerResourceAssociation{
		{
			TargetAPIBaseURI:     "http://unknown.example",
			PeerSubscriptionID:   "shared-peer-id",
			NwdafSubscriptionIDs: []string{"nwdaf-sub-c"},
		},
	})
	if accepted {
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
