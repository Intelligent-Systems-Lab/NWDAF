package context

import (
	"testing"

	"github.com/free5gc/nwdaf/internal/backend"
)

func TestMLModelRouteMirrorsCopyRawRepresentations(t *testing.T) {
	t.Parallel()

	ctx := &NWDAFContext{}
	accepted := []byte(`{"notifUri":"http://consumer.example/callback","future":1}`)
	backendRepresentation := []byte(`{"notifUri":"http://go.internal/callback","future":1}`)
	route := MLModelProvisionSubscriptionRoute{
		SubscriptionID:             "sub-1",
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
	if !found || stored.AcceptedRepresentation[0] != '{' || stored.BackendRepresentation[0] != '{' {
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

func TestBackendSyncRebindsOnlyLocallyOwnedResourceIDs(t *testing.T) {
	t.Parallel()

	const anlfGeneration = "anlf-generation-2"

	ctx := &NWDAFContext{}
	if !ctx.AddMLModelProvisionSubscriptionRoute(MLModelProvisionSubscriptionRoute{
		SubscriptionID: "local-provision",
		Destination:    MLModelRoutePartyMTLFBackend,
		PeerRoute: MLModelPeerRoute{
			BackendResourceID: "old-mtlf-resource",
			LifecycleState:    MLModelRouteActive,
		},
	}) {
		t.Fatal("could not add local provision route")
	}
	if !ctx.AddMLModelProvisionSubscriptionRoute(MLModelProvisionSubscriptionRoute{
		SubscriptionID: "remote-provision",
		Initiator:      MLModelRoutePartyAnLFBackend,
		Destination:    MLModelRoutePartyAnLFBackend,
		PeerRoute: MLModelPeerRoute{
			BackendResourceID: "unchanged",
			PeerLocation:      "http://peer.example/subscriptions/1",
			LifecycleState:    MLModelRouteActive,
			SelectedTarget: &backend.SelectedTarget{
				NFInstanceID: "11111111-1111-4111-8111-111111111111",
			},
		},
	}) {
		t.Fatal("could not add remote provision route")
	}
	if !ctx.AddMLModelMonitorSubscriptionRoute(MLModelMonitorSubscriptionRoute{
		SubscriptionID: "local-monitor",
		Destination:    MLModelRoutePartyAnLFBackend,
		PeerRoute: MLModelPeerRoute{
			BackendResourceID: "old-anlf-resource",
			LifecycleState:    MLModelRouteActive,
		},
	}) {
		t.Fatal("could not add local monitor route")
	}

	ctx.ReconcileMTLFMLModelRoutes("mtlf-generation-2")
	ctx.ReconcileAnLFMLModelRoutes(anlfGeneration)

	localProvision, _ := ctx.GetMLModelProvisionSubscriptionRoute("local-provision")
	remoteProvision, _ := ctx.GetMLModelProvisionSubscriptionRoute("remote-provision")
	localMonitor, _ := ctx.GetMLModelMonitorSubscriptionRoute("local-monitor")
	if localProvision.PeerRoute.BackendResourceID != "local-provision" ||
		localProvision.PeerRoute.ProcessGeneration != "mtlf-generation-2" {
		t.Fatalf("local provision route = %+v", localProvision)
	}
	if remoteProvision.PeerRoute.BackendResourceID != "unchanged" ||
		remoteProvision.PeerRoute.PeerLocation != "http://peer.example/subscriptions/1" ||
		remoteProvision.PeerRoute.ProcessGeneration != anlfGeneration {
		t.Fatalf("remote provision route = %+v", remoteProvision)
	}
	if localMonitor.PeerRoute.BackendResourceID != "local-monitor" ||
		localMonitor.PeerRoute.ProcessGeneration != anlfGeneration {
		t.Fatalf("local monitor route = %+v", localMonitor)
	}
}

func TestBackendSyncDoesNotClaimOtherBackendRoutes(t *testing.T) {
	t.Parallel()

	ctx := &NWDAFContext{}
	if !ctx.AddMLModelMonitorRegistrationRoute(MLModelMonitorRegistrationRoute{
		RegistrationID: "anlf-outbound-registration",
		Initiator:      MLModelRoutePartyAnLFBackend,
		PeerRoute: MLModelPeerRoute{
			ProcessGeneration: "anlf-generation-1",
			LifecycleState:    MLModelRouteActive,
			SelectedTarget: &backend.SelectedTarget{
				NFInstanceID: "11111111-1111-4111-8111-111111111111",
			},
		},
	}) {
		t.Fatal("could not add outbound registration route")
	}
	if !ctx.AddMLModelMonitorSubscriptionRoute(MLModelMonitorSubscriptionRoute{
		SubscriptionID: "mtlf-outbound-monitor",
		Destination:    MLModelRoutePartyMTLFBackend,
		PeerRoute: MLModelPeerRoute{
			ProcessGeneration: "mtlf-generation-1",
			LifecycleState:    MLModelRouteActive,
			SelectedTarget: &backend.SelectedTarget{
				NFInstanceID: "22222222-2222-4222-8222-222222222222",
			},
		},
	}) {
		t.Fatal("could not add outbound monitor route")
	}

	ctx.ReconcileMTLFMLModelRoutes("mtlf-generation-2")
	ctx.ReconcileAnLFMLModelRoutes("anlf-generation-2")

	registration, _ := ctx.GetMLModelMonitorRegistrationRoute("anlf-outbound-registration")
	monitor, _ := ctx.GetMLModelMonitorSubscriptionRoute("mtlf-outbound-monitor")
	if registration.PeerRoute.ProcessGeneration != "anlf-generation-2" {
		t.Fatalf("registration generation = %q", registration.PeerRoute.ProcessGeneration)
	}
	if monitor.PeerRoute.ProcessGeneration != "mtlf-generation-2" {
		t.Fatalf("monitor generation = %q", monitor.PeerRoute.ProcessGeneration)
	}
}

func TestBackendSyncPreservesResourceIDWithinSameProcessGeneration(t *testing.T) {
	t.Parallel()

	ctx := &NWDAFContext{}
	if !ctx.AddMLModelTrainingSubscriptionRoute(MLModelTrainingSubscriptionRoute{
		SubscriptionID: "go-training-route",
		PeerRoute: MLModelPeerRoute{
			BackendResourceID: "python-training-resource",
			ProcessGeneration: "mtlf-generation-1",
			LifecycleState:    MLModelRouteActive,
		},
		NotificationCorrelationID: "training-correlation",
	}) {
		t.Fatal("could not add training route")
	}

	ctx.ReconcileMTLFMLModelRoutes("mtlf-generation-1")

	route, _ := ctx.GetMLModelTrainingSubscriptionRoute("go-training-route")
	if route.PeerRoute.BackendResourceID != "python-training-resource" {
		t.Fatalf("same-generation backend resource ID = %q", route.PeerRoute.BackendResourceID)
	}
}
