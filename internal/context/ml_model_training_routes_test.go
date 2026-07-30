package context

import "testing"

func TestMLModelTrainingRouteCorrelationIsUniqueAndCloned(t *testing.T) {
	Init()
	ctx := GetSelf()
	round := int64(2)
	route := MLModelTrainingSubscriptionRoute{
		SubscriptionID:            "route-a",
		NotificationCorrelationID: "correlation-a",
		ExpectedRoundIndicator:    &round,
		AcceptedRepresentation:    []byte(`{"roundInd":2}`),
	}
	if !ctx.AddMLModelTrainingSubscriptionRoute(route) {
		t.Fatal("first training route was rejected")
	}
	duplicate := route
	duplicate.SubscriptionID = "route-b"
	if ctx.AddMLModelTrainingSubscriptionRoute(duplicate) {
		t.Fatal("duplicate notification correlation was accepted")
	}

	stored, found := ctx.GetMLModelTrainingSubscriptionRoute("route-a")
	if !found {
		t.Fatal("training route was not found")
	}
	*stored.ExpectedRoundIndicator = 9
	stored.AcceptedRepresentation[0] = 'x'
	again, _ := ctx.GetMLModelTrainingSubscriptionRoute("route-a")
	if *again.ExpectedRoundIndicator != 2 ||
		string(again.AcceptedRepresentation) != `{"roundInd":2}` {
		t.Fatalf("stored route was mutated through a returned value: %+v", again)
	}
}
