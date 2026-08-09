package processor

import (
	"testing"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

func TestResetAnalyticsGenerationNegotiatesFailureNotification(t *testing.T) {
	ctx := setupTestContext()
	processor := &Processor{nwdaf: &subscriptionTestApp{ctx: t.Context()}}
	const generation = "anlf-generation"
	negotiatedID := "11111111-1111-4111-8111-111111111111"
	unnegotiatedID := "22222222-2222-4222-8222-222222222222"
	for _, route := range []nwdaf_context.AnalyticsSubscriptionRoute{
		{
			SubscriptionID:    negotiatedID,
			ProcessGeneration: generation,
			AcceptedSubscription: models.NnwdafEventsSubscription{
				NotifCorrId:       "corr-negotiated",
				SupportedFeatures: "10000000000400",
				EventSubscriptions: []models.NwdafEventsSubscriptionEventSubscription{{
					Event: models.NwdafEvent_UE_COMMUNICATION,
				}},
			},
		},
		{
			SubscriptionID:    unnegotiatedID,
			ProcessGeneration: generation,
			AcceptedSubscription: models.NnwdafEventsSubscription{
				SupportedFeatures: "400",
				EventSubscriptions: []models.NwdafEventsSubscriptionEventSubscription{{
					Event: models.NwdafEvent_UE_COMMUNICATION,
				}},
			},
		},
	} {
		if !ctx.AddAnalyticsSubscriptionRoute(route) {
			t.Fatalf("could not add route %s", route.SubscriptionID)
		}
	}

	var delivered []models.NnwdafEventsSubscriptionNotification
	removed := processor.ResetAnalyticsGeneration(
		generation,
		func(notifications []models.NnwdafEventsSubscriptionNotification, _ []byte) error {
			delivered = append(delivered, notifications...)
			return nil
		},
	)

	if len(removed) != 2 {
		t.Fatalf("removed routes = %v", removed)
	}
	if len(delivered) != 1 || delivered[0].SubscriptionId != negotiatedID {
		t.Fatalf("delivered notifications = %+v", delivered)
	}
	if got := delivered[0].EventNotifications[0].FailNotifyCode; got != models.NwdafFailureCode_UNAVAILABLE_DATA {
		t.Fatalf("failNotifyCode = %q", got)
	}
	for _, id := range []string{negotiatedID, unnegotiatedID} {
		if !ctx.IsAnalyticsSubscriptionTombstoned(id) {
			t.Fatalf("subscription %s was not tombstoned", id)
		}
		if _, found := ctx.GetAnalyticsSubscriptionRoute(id); found {
			t.Fatalf("subscription %s remains active", id)
		}
	}
}

func TestSupportsFeatureRejectsMalformedAndUsesThreeGPPBitNumbering(t *testing.T) {
	if !supportsFeature("10000000000400", 11) || !supportsFeature("10000000000400", 53) {
		t.Fatal("expected feature 11 and 53 bits to be set")
	}
	if supportsFeature("400", 53) || supportsFeature("not-hex", 11) || supportsFeature("", 11) {
		t.Fatal("unsupported or malformed feature mask was accepted")
	}
}
