package context

import (
	"testing"
)

func TestMLModelRouteMirrorsCopyRawRepresentations(t *testing.T) {
	t.Parallel()

	ctx := &NWDAFContext{}
	accepted := []byte(`{"notifUri":"http://consumer.example/callback","future":1}`)
	backendRepresentation := []byte(`{"notifUri":"http://go.internal/callback","future":1}`)
	route := MLModelProvisionSubscriptionRoute{
		SubscriptionID:             "sub-1",
		PeerRoute:                  MLModelPeerRoute{OperationRevision: 42},
		AcceptedRepresentation:     accepted,
		BackendRepresentation:      backendRepresentation,
		Initiator:                  MLModelRoutePartyExternal,
		Destination:                MLModelRoutePartyExternal,
		DestinationNotificationURI: "http://consumer.example/callback",
		NotificationCorrelationID:  "corr-1",
	}
	if !ctx.AddMLModelProvisionSubscriptionRoute(route) {
		t.Fatal("AddMLModelProvisionSubscriptionRoute() = false")
	}
	accepted[0] = 'x'
	backendRepresentation[0] = 'x'
	stored, found := ctx.GetMLModelProvisionSubscriptionRoute("sub-1")
	if !found || stored.PeerRoute.OperationRevision != 42 ||
		stored.AcceptedRepresentation[0] != '{' || stored.BackendRepresentation[0] != '{' {
		t.Fatalf("stored route was aliased: %+v", stored)
	}
	stored.AcceptedRepresentation[0] = 'y'
	again, _ := ctx.GetMLModelProvisionSubscriptionRoute("sub-1")
	if again.AcceptedRepresentation[0] != '{' {
		t.Fatal("returned route aliases stored raw JSON")
	}
	if !ctx.DeleteMLModelProvisionSubscriptionRoute("sub-1") {
		t.Fatal("DeleteMLModelProvisionSubscriptionRoute() = false")
	}
}

func TestMLModelRouteMirrorsKeepResourceKindsSeparate(t *testing.T) {
	t.Parallel()

	ctx := &NWDAFContext{}
	if !ctx.AddMLModelMonitorRegistrationRoute(MLModelMonitorRegistrationRoute{
		RegistrationID:         "shared-id",
		AcceptedRepresentation: []byte(`{"modelId":1}`),
	}) {
		t.Fatal("could not add registration route")
	}
	if !ctx.AddMLModelMonitorSubscriptionRoute(MLModelMonitorSubscriptionRoute{
		SubscriptionID:         "shared-id",
		AcceptedRepresentation: []byte(`{"modelIds":[1]}`),
	}) {
		t.Fatal("could not add subscription route")
	}
	if len(ctx.GetAllMLModelMonitorRegistrationRoutes()) != 1 ||
		len(ctx.GetAllMLModelMonitorSubscriptionRoutes()) != 1 {
		t.Fatal("resource kinds collided")
	}
}
