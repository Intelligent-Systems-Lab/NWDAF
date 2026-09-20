package context

import "testing"

func TestMLModelTrainingRouteCorrelationIsUniqueAndCloned(t *testing.T) {
	Init()
	ctx := GetSelf()
	round := int64(2)
	route := MLModelTrainingSubscriptionRoute{
		SubscriptionID:                  "route-a",
		OwnerNFInstanceID:               ctx.NfId,
		PeerRoute:                       MLModelPeerRoute{Direction: MLModelRouteDirectionInbound},
		NotificationCorrelationID:       "correlation-a",
		ExpectedRoundIndicator:          &round,
		AcceptedRepresentation:          []byte(`{"roundInd":2}`),
		OfferedSupportedFeatures:        "4",
		NegotiatedSupportedFeatures:     "4",
		HierarchicalFLFeatureNegotiated: true,
		BoundParticipantNFInstanceID:    "10000000-0000-4000-8000-000000000001",
	}
	if !ctx.AddMLModelTrainingSubscriptionRoute(route) {
		t.Fatal("first training route was rejected")
	}
	duplicate := route
	duplicate.SubscriptionID = "route-b"
	if ctx.AddMLModelTrainingSubscriptionRoute(duplicate) {
		t.Fatal("duplicate notification correlation was accepted")
	}

	stored, found := ctx.GetMLModelTrainingSubscriptionRoute(route.ResourceKey())
	if !found {
		t.Fatal("training route was not found")
	}
	*stored.ExpectedRoundIndicator = 9
	stored.AcceptedRepresentation[0] = 'x'
	again, _ := ctx.GetMLModelTrainingSubscriptionRoute(route.ResourceKey())
	if *again.ExpectedRoundIndicator != 2 ||
		string(again.AcceptedRepresentation) != `{"roundInd":2}` {
		t.Fatalf("stored route was mutated through a returned value: %+v", again)
	}
	if again.OfferedSupportedFeatures != "4" ||
		again.NegotiatedSupportedFeatures != "4" ||
		!again.HierarchicalFLFeatureNegotiated ||
		again.BoundParticipantNFInstanceID != "10000000-0000-4000-8000-000000000001" {
		t.Fatalf("feature or identity state was not preserved: %+v", again)
	}
}

func TestMLModelTrainingRoutesScopePeerIDAndKeepCallbackIdentity(t *testing.T) {
	Init()
	ctx := GetSelf()
	for _, peer := range []struct{ owner, callback, correlation string }{
		{"peer-a", "callback-a", "correlation-a"},
		{"peer-b", "callback-b", "correlation-b"},
	} {
		pending := MLModelTrainingSubscriptionRoute{
			OwnerNFInstanceID:         peer.owner,
			CallbackRouteID:           peer.callback,
			NotificationCorrelationID: peer.correlation,
			PeerRoute: MLModelPeerRoute{
				Direction:      MLModelRouteDirectionOutbound,
				LifecycleState: MLModelRouteCreating,
			},
		}
		if !ctx.AddMLModelTrainingSubscriptionRoute(pending) {
			t.Fatalf("could not reserve %s", peer.owner)
		}
		if got, found := ctx.FindMLModelTrainingSubscriptionRouteByCallback(peer.callback); !found ||
			got.SubscriptionID != "" {
			t.Fatalf("pending callback %s: %+v found=%t", peer.callback, got, found)
		}
		pending.SubscriptionID = "same-peer-resource-id"
		pending.PeerRoute.LifecycleState = MLModelRouteActive
		if !ctx.ActivatePendingMLModelTrainingRoute(peer.callback, pending) {
			t.Fatalf("could not activate %s", peer.owner)
		}
	}
	if routes := ctx.GetAllMLModelTrainingSubscriptionRoutes(); len(routes) != 2 {
		t.Fatalf("active routes = %+v", routes)
	}
	keyA := MLModelTrainingResourceKey{
		Direction: MLModelRouteDirectionOutbound, OwnerNFInstanceID: "peer-a",
		SubscriptionID: "same-peer-resource-id",
	}
	keyB := MLModelTrainingResourceKey{
		Direction: MLModelRouteDirectionOutbound, OwnerNFInstanceID: "peer-b",
		SubscriptionID: "same-peer-resource-id",
	}
	if !ctx.DeleteMLModelTrainingSubscriptionRoute(keyA) {
		t.Fatal("peer-a route was not deleted")
	}
	if _, found := ctx.GetMLModelTrainingSubscriptionRoute(keyB); !found {
		t.Fatal("deleting peer-a also removed peer-b")
	}
	if route, found := ctx.FindMLModelTrainingSubscriptionRouteByCallback("callback-b"); !found ||
		route.OwnerNFInstanceID != "peer-b" {
		t.Fatalf("peer-b callback route = %+v found=%t", route, found)
	}
}
