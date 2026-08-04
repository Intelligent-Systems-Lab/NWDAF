package processor

import (
	"encoding/json"
	"testing"

	"github.com/free5gc/nwdaf/internal/backend"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

type associationAvailabilityStub struct {
	snapshot backend.Snapshot
}

func TestReplaceTrainingDataDescriptorsRefreshesMtlfSync(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	availability := &associationAvailabilityStub{snapshot: backend.Snapshot{
		State: backend.StateUsable, ProcessInstanceID: "anlf-process",
	}}
	refresher := &associationRefreshStub{}
	processor := NewProcessor()
	processor.SetSmfAssociationRepository(ctx, availability, refresher)
	var descriptor backend.TrainingDataDescriptor
	if err := json.Unmarshal([]byte(`{
  "correlationId":"22222222-2222-4222-8222-222222222222",
  "state":"ACTIVE",
  "storedDataSpec":{"dataSpec":{"smfDataSub":{"supi":"imsi-1","notifId":"corr",
    "notifUri":"http://anlf/callback","eventSubs":[{"event":"UPF_EVENT"}]}},
    "timePeriod":{"startTime":"2026-08-04T09:00:00Z","stopTime":"2026-08-04T09:30:00Z"}},
  "mlEventSubscription":{"mLEvent":"UE_COMMUNICATION"},
  "sourceNfInstanceId":"11111111-1111-4111-8111-111111111111",
  "adrfInstanceId":"33333333-3333-4333-8333-333333333333",
  "retainUntil":"2099-08-04T10:30:00Z"
}`), &descriptor); err != nil {
		t.Fatalf("decode descriptor fixture: %v", err)
	}
	update := backend.TrainingDataDescriptorUpdate{
		ProcessInstanceID: "anlf-process", TrainingDataDescriptors: []backend.TrainingDataDescriptor{descriptor},
	}
	if err := processor.ReplaceTrainingDataDescriptors(update); err != nil {
		t.Fatalf("ReplaceTrainingDataDescriptors() error = %v", err)
	}
	if refresher.count != 1 {
		t.Fatalf("MTLF sync refresh count = %d, want 1", refresher.count)
	}
	if err := processor.ReplaceTrainingDataDescriptors(update); err != nil {
		t.Fatalf("repeat ReplaceTrainingDataDescriptors() error = %v", err)
	}
	if refresher.count != 1 {
		t.Fatalf("unchanged descriptor refreshed MTLF sync: count = %d", refresher.count)
	}
}

func (s *associationAvailabilityStub) Snapshot() backend.Snapshot {
	return s.snapshot
}

type associationRefreshStub struct {
	count int
}

func (s *associationRefreshStub) Refresh() {
	s.count++
}

func TestReplaceSmfResourceAssociationsRefreshesMtlfSync(t *testing.T) {
	nwdaf_context.Init()
	ctx := nwdaf_context.GetSelf()
	if !ctx.AddSmfPeerResourceRoute(&nwdaf_context.SmfPeerResourceRoute{
		SubscriptionID:           "peer-a",
		ResourceLocation:         "http://smf.example/nsmf-event-exposure/v1/subscriptions/peer-a",
		TargetAPIBaseURI:         "http://smf.example",
		CorrelationID:            "corr-a",
		AcceptedSubscriptionJSON: []byte(`{"notifId":"corr-a"}`),
	}) {
		t.Fatal("could not seed SMF peer resource")
	}

	availability := &associationAvailabilityStub{snapshot: backend.Snapshot{
		State:             backend.StateUsable,
		ProcessInstanceID: "anlf-process",
	}}
	refresher := &associationRefreshStub{}
	processor := NewProcessor()
	processor.SetSmfAssociationRepository(ctx, availability, refresher)

	err := processor.ReplaceSmfResourceAssociations(backend.SmfResourceAssociationUpdate{
		ProcessInstanceID: "anlf-process",
		SmfResources: []backend.SmfResourceAssociation{{
			TargetAPIBaseURI:     "http://smf.example",
			PeerSubscriptionID:   "peer-a",
			NwdafSubscriptionIDs: []string{"subscription-a"},
		}},
	})
	if err != nil {
		t.Fatalf("ReplaceSmfResourceAssociations() error = %v", err)
	}
	if refresher.count != 1 {
		t.Fatalf("MTLF sync refresh count = %d, want 1", refresher.count)
	}
	if err = processor.ReplaceSmfResourceAssociations(backend.SmfResourceAssociationUpdate{
		ProcessInstanceID: "anlf-process",
		SmfResources: []backend.SmfResourceAssociation{{
			TargetAPIBaseURI:     "http://smf.example",
			PeerSubscriptionID:   "peer-a",
			NwdafSubscriptionIDs: []string{"subscription-a"},
		}},
	}); err != nil {
		t.Fatalf("repeat ReplaceSmfResourceAssociations() error = %v", err)
	}
	if refresher.count != 1 {
		t.Fatalf("unchanged association refreshed MTLF sync: count = %d", refresher.count)
	}
	route, found := ctx.GetSmfPeerResourceRoute("http://smf.example", "peer-a")
	if !found || len(route.NwdafSubscriptionIDs) != 1 ||
		route.NwdafSubscriptionIDs[0] != "subscription-a" {
		t.Fatalf("stored route = %+v, found = %v", route, found)
	}
}
