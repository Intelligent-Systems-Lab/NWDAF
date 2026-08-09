package processor

import (
	"net/http"
	"testing"

	"github.com/free5gc/nwdaf/internal/backend"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

func TestResetMTLFGenerationClearsProviderAndConsumerRelationships(t *testing.T) {
	processor, ctx, _, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	mtlfGeneration := mtlfAvailability.generation
	anlfGeneration := anlfAvailability.generation

	if !ctx.AddMLModelProvisionSubscriptionRoute(nwdaf_context.MLModelProvisionSubscriptionRoute{
		SubscriptionID: "provision-local",
		PeerRoute: nwdaf_context.MLModelPeerRoute{
			Direction:         nwdaf_context.MLModelRouteDirectionInbound,
			BackendResourceID: "provision-backend",
			ProcessGeneration: mtlfGeneration,
			RelatedBackend:    backend.KindAnLF,
			RelatedGeneration: anlfGeneration,
		},
		Initiator: nwdaf_context.MLModelRoutePartyAnLFBackend,
	}) {
		t.Fatal("could not add local provision route")
	}
	if !ctx.AddMLModelMonitorRegistrationRoute(nwdaf_context.MLModelMonitorRegistrationRoute{
		RegistrationID: "registration-local",
		PeerRoute: nwdaf_context.MLModelPeerRoute{
			Direction:         nwdaf_context.MLModelRouteDirectionInbound,
			BackendResourceID: "registration-backend",
			ProcessGeneration: mtlfGeneration,
			RelatedBackend:    backend.KindAnLF,
			RelatedGeneration: anlfGeneration,
		},
		Initiator: nwdaf_context.MLModelRoutePartyAnLFBackend,
	}) {
		t.Fatal("could not add local registration route")
	}
	if !ctx.AddMLModelMonitorSubscriptionRoute(nwdaf_context.MLModelMonitorSubscriptionRoute{
		SubscriptionID: "monitor-local",
		PeerRoute: nwdaf_context.MLModelPeerRoute{
			Direction:         nwdaf_context.MLModelRouteDirectionInbound,
			BackendResourceID: "monitor-backend",
			ProcessGeneration: anlfGeneration,
			RelatedBackend:    backend.KindMTLF,
			RelatedGeneration: mtlfGeneration,
		},
		Destination: nwdaf_context.MLModelRoutePartyMTLFBackend,
	}) {
		t.Fatal("could not add local monitor subscription route")
	}
	if !ctx.AddMLModelTrainingSubscriptionRoute(nwdaf_context.MLModelTrainingSubscriptionRoute{
		SubscriptionID: "training-local",
		PeerRoute: nwdaf_context.MLModelPeerRoute{
			Direction:         nwdaf_context.MLModelRouteDirectionInbound,
			BackendResourceID: "training-backend",
			ProcessGeneration: mtlfGeneration,
		},
		NotificationCorrelationID: "training-correlation",
	}) {
		t.Fatal("could not add local training route")
	}

	processor.ResetMLModelBackendGeneration(t.Context(), backend.KindMTLF, mtlfGeneration)

	if _, found := ctx.GetMLModelProvisionSubscriptionRoute("provision-local"); found {
		t.Fatal("MTLF provider provision route remains active")
	}
	if _, found := ctx.GetMLModelMonitorRegistrationRoute("registration-local"); found {
		t.Fatal("MTLF provider registration route remains active")
	}
	if _, found := ctx.GetMLModelMonitorSubscriptionRoute("monitor-local"); found {
		t.Fatal("MTLF consumer monitor route remains active")
	}
	if _, found := ctx.GetMLModelTrainingSubscriptionRoute("training-local"); found {
		t.Fatal("MTLF training route remains active")
	}
	if anlfBackend.deleted != "monitor-backend" {
		t.Fatalf("AnLF monitor cleanup id = %q", anlfBackend.deleted)
	}
	for kind, id := range map[nwdaf_context.MLModelResourceKind]string{
		nwdaf_context.MLModelResourceProvisionSubscription: "provision-local",
		nwdaf_context.MLModelResourceMonitorRegistration:   "registration-local",
		nwdaf_context.MLModelResourceMonitorSubscription:   "monitor-local",
		nwdaf_context.MLModelResourceTrainingSubscription:  "training-local",
	} {
		if _, found := ctx.GetMLModelDeletionRecord(kind, id); !found {
			t.Fatalf("deletion record missing: kind=%s id=%s", kind, id)
		}
	}

	response, problem := processor.HandleDeleteMLModelProvision(t.Context(), "provision-local")
	if problem != nil || response == nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("late provision DELETE response=%+v problem=%+v", response, problem)
	}
	response, problem = processor.HandleDeleteMLModelProvision(t.Context(), "provision-local")
	if response != nil || problem == nil || problem.Status != http.StatusNotFound {
		t.Fatalf("second provision DELETE response=%+v problem=%+v", response, problem)
	}
}

func TestResetAnLFGenerationDeletesConsumerRelationshipsOnce(t *testing.T) {
	processor, ctx, mtlfBackend, anlfBackend, mtlfAvailability, anlfAvailability := newMLModelProcessorTestSubject()
	mtlfGeneration := mtlfAvailability.generation
	anlfGeneration := anlfAvailability.generation

	if !ctx.AddMLModelProvisionSubscriptionRoute(nwdaf_context.MLModelProvisionSubscriptionRoute{
		SubscriptionID: "provision-local",
		PeerRoute: nwdaf_context.MLModelPeerRoute{
			Direction:         nwdaf_context.MLModelRouteDirectionInbound,
			BackendResourceID: "provision-backend",
			ProcessGeneration: mtlfGeneration,
			RelatedBackend:    backend.KindAnLF,
			RelatedGeneration: anlfGeneration,
		},
		Initiator: nwdaf_context.MLModelRoutePartyAnLFBackend,
	}) {
		t.Fatal("could not add local provision route")
	}
	if !ctx.AddMLModelMonitorRegistrationRoute(nwdaf_context.MLModelMonitorRegistrationRoute{
		RegistrationID: "registration-local",
		PeerRoute: nwdaf_context.MLModelPeerRoute{
			Direction:         nwdaf_context.MLModelRouteDirectionInbound,
			BackendResourceID: "registration-backend",
			ProcessGeneration: mtlfGeneration,
			RelatedBackend:    backend.KindAnLF,
			RelatedGeneration: anlfGeneration,
		},
		Initiator: nwdaf_context.MLModelRoutePartyAnLFBackend,
	}) {
		t.Fatal("could not add local registration route")
	}
	if !ctx.AddMLModelMonitorSubscriptionRoute(nwdaf_context.MLModelMonitorSubscriptionRoute{
		SubscriptionID: "monitor-local",
		PeerRoute: nwdaf_context.MLModelPeerRoute{
			Direction:         nwdaf_context.MLModelRouteDirectionInbound,
			BackendResourceID: "monitor-backend",
			ProcessGeneration: anlfGeneration,
			RelatedBackend:    backend.KindMTLF,
			RelatedGeneration: mtlfGeneration,
		},
		Destination: nwdaf_context.MLModelRoutePartyMTLFBackend,
	}) {
		t.Fatal("could not add local monitor route")
	}
	if !ctx.AddMLModelProvisionSubscriptionRoute(nwdaf_context.MLModelProvisionSubscriptionRoute{
		SubscriptionID: "external-provision",
		PeerRoute: nwdaf_context.MLModelPeerRoute{
			Direction:         nwdaf_context.MLModelRouteDirectionInbound,
			BackendResourceID: "external-backend",
			ProcessGeneration: mtlfGeneration,
		},
		Initiator: nwdaf_context.MLModelRoutePartyExternal,
	}) {
		t.Fatal("could not add external provision route")
	}

	processor.ResetMLModelBackendGeneration(t.Context(), backend.KindAnLF, anlfGeneration)

	if mtlfBackend.deletedProvision != "provision-backend" {
		t.Fatalf("MTLF provision cleanup id = %q", mtlfBackend.deletedProvision)
	}
	if mtlfBackend.deletedRegistration != "registration-backend" {
		t.Fatalf("MTLF registration cleanup id = %q", mtlfBackend.deletedRegistration)
	}
	if anlfBackend.deleted != "" {
		t.Fatalf("failed AnLF provider was called for cleanup: %q", anlfBackend.deleted)
	}
	if _, found := ctx.GetMLModelMonitorSubscriptionRoute("monitor-local"); found {
		t.Fatal("AnLF provider monitor route remains active")
	}
	if _, found := ctx.GetMLModelProvisionSubscriptionRoute("external-provision"); !found {
		t.Fatal("unrelated external provision route was removed")
	}
}
