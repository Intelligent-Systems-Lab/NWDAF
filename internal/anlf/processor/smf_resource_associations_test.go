package processor

import (
	"testing"

	"github.com/free5gc/nwdaf/internal/backend"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

type associationAvailabilityStub struct {
	snapshot backend.Snapshot
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
