package processor

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"go.uber.org/mock/gomock"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/mtlf"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/nwdaf/pkg/mockapp"
	"github.com/free5gc/openapi/models"
)

func TestTriggerTargetDataCollection_ResourceReuse(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	smfConsumer := NewMockConsumerAPI(ctrl)
	smfConsumer.EXPECT().AdrfClient().Return(nil).AnyTimes()
	smfConsumer.EXPECT().
		SubscribeToSmf(gomock.Any(), "http://smf.example", gomock.AssignableToTypeOf(consumer.SmfSubscriptionOptions{})).
		DoAndReturn(func(_ any, _ string, opts consumer.SmfSubscriptionOptions) (string, error) {
			if opts.Supi != "imsi-208930000000003" {
				t.Fatalf("SubscribeToSmf SUPI = %q, want %q", opts.Supi, "imsi-208930000000003")
			}
			return "smf-sub-1", nil
		}).
		Times(1)

	// 3. Define Targets
	targetSupi := "imsi-208930000000003"
	targets := []DataCollectionTarget{
		{
			Supi: targetSupi,
		},
	}

	// 4. Trigger First Subscription
	nwdafSubId1 := "nwdaf-sub-01"
	smfEndpoint := "http://smf.example"
	smfEndpoints := []string{smfEndpoint}
	profileKey := canonicalCollectionProfileKey(10, []string{"TOTAL_VOLUME", "UL_VOLUME", "DL_VOLUME"})

	// Access unexported method via reflection? No, I am in package processor!
	// But triggerTargetDataCollection is in data_collection.go which belongs to package processor.
	// So I can call it directly.
	if _, _, err := p.triggerTargetDataCollection(
		context.Background(),
		ctx,
		smfConsumer,
		smfEndpoints,
		targets,
		nwdafSubId1,
		"http://nwdaf/notify",
		"http://nwdaf/upf-notify",
		10,
	); err != nil {
		t.Fatalf("first triggerTargetDataCollection() error = %v", err)
	}

	// 5. Verify First Subscription
	targetKey := targetSupi + "@" + smfEndpoint
	correlationId1, found := ctx.GetSmfCorrelationIdForProfile("supi="+targetSupi, smfEndpoint, profileKey)
	if !found {
		t.Fatalf("Expected SMF mapping for key %s", targetKey)
	}

	sub1 := ctx.GetSmfSubscription(correlationId1)
	if sub1 == nil {
		t.Fatal("Expected SmfSubscription to be created")
	} else if sub1.RefCount != 1 {
		t.Errorf("Expected RefCount=1, got %d", sub1.RefCount)
	}

	// 6. Trigger Second Subscription (Same Target)
	nwdafSubId2 := "nwdaf-sub-02"
	if _, _, err := p.triggerTargetDataCollection(
		context.Background(),
		ctx,
		smfConsumer,
		smfEndpoints,
		targets,
		nwdafSubId2,
		"http://nwdaf/notify",
		"http://nwdaf/upf-notify",
		10,
	); err != nil {
		t.Fatalf("second triggerTargetDataCollection() error = %v", err)
	}

	// 7. Verify Reuse
	correlationId2, found2 := ctx.GetSmfCorrelationIdForProfile("supi="+targetSupi, smfEndpoint, profileKey)
	if !found2 {
		t.Fatal("Expected SMF mapping to exist")
	}
	if correlationId1 != correlationId2 {
		t.Errorf("Expected CorrelationId reuse. Got %s and %s", correlationId1, correlationId2)
	}

	sub2 := ctx.GetSmfSubscription(correlationId2)
	if sub2.RefCount != 2 {
		t.Errorf("Expected RefCount=2, got %d", sub2.RefCount)
	}

	// 8. Cleanup First
	shouldDelete, _ := ctx.ReleaseSmfSubscription(correlationId1, nwdafSubId1)
	if shouldDelete {
		t.Error("Should not delete subscription when RefCount=2")
	}
	if sub2.RefCount != 1 {
		t.Errorf("Expected RefCount=1 after release, got %d", sub2.RefCount)
	}

	// Mapping should still exist
	_, found3 := ctx.GetSmfCorrelationIdForProfile("supi="+targetSupi, smfEndpoint, profileKey)
	if !found3 {
		t.Error("Mapping should persist until last reference removed")
	}

	// 9. Cleanup Second (Last)
	shouldDelete2, _ := ctx.ReleaseSmfSubscription(correlationId1, nwdafSubId2)
	if !shouldDelete2 {
		t.Error("Should delete subscription when RefCount=0")
	}

	// Mapping should be gone
	_, found4 := ctx.GetSmfCorrelationIdForProfile("supi="+targetSupi, smfEndpoint, profileKey)
	if found4 {
		t.Error("Mapping should be removed after last reference removed")
	}
}

func TestTriggerTargetDataCollection_DifferentProfilesDoNotReuse(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	smfConsumer := NewMockConsumerAPI(ctrl)
	smfConsumer.EXPECT().
		SubscribeToSmf(gomock.Any(), "http://smf.example", gomock.Any()).
		Return("smf-sub", nil).
		Times(2)
	targets := []DataCollectionTarget{{Supi: "imsi-1"}}
	for _, profile := range []struct {
		subscriptionID string
		period         int32
	}{
		{subscriptionID: "sub-10", period: 10},
		{subscriptionID: "sub-20", period: 20},
	} {
		if _, _, err := p.triggerTargetDataCollection(
			context.Background(),
			ctx,
			smfConsumer,
			[]string{"http://smf.example"},
			targets,
			profile.subscriptionID,
			"http://nwdaf/notify",
			"http://nwdaf/upf-notify",
			profile.period,
		); err != nil {
			t.Fatalf("triggerTargetDataCollection() error = %v", err)
		}
	}

	profile10 := canonicalCollectionProfileKey(10, []string{"TOTAL_VOLUME", "UL_VOLUME", "DL_VOLUME"})
	profile20 := canonicalCollectionProfileKey(20, []string{"TOTAL_VOLUME", "UL_VOLUME", "DL_VOLUME"})
	corr10, ok10 := ctx.GetSmfCorrelationIdForProfile("supi=imsi-1", "http://smf.example", profile10)
	corr20, ok20 := ctx.GetSmfCorrelationIdForProfile("supi=imsi-1", "http://smf.example", profile20)
	if !ok10 || !ok20 || corr10 == corr20 {
		t.Fatalf("profile correlations: period10=%q period20=%q", corr10, corr20)
	}
}

// =============================================================================
// DataCollectionTarget Tests
// =============================================================================

func TestDataCollectionTarget_Identifier(t *testing.T) {
	tests := []struct {
		name     string
		target   DataCollectionTarget
		expected string
	}{
		{
			name:     "SUPI only",
			target:   DataCollectionTarget{Supi: "imsi-123456789012345"},
			expected: "supi=imsi-123456789012345",
		},
		{
			name: "SUPI with OriginalGroupId",
			target: DataCollectionTarget{
				Supi:            "imsi-123456789012345",
				OriginalGroupId: "group-test-001",
			},
			expected: "supi=imsi-123456789012345",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.target.Identifier()
			if got != tt.expected {
				t.Errorf("Identifier() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestDataCollectionTarget_OriginalGroupIdTracking(t *testing.T) {
	// When Group ID is resolved to SUPIs, OriginalGroupId should be preserved
	groupId := "group-enterprise-001"
	supis := []string{"imsi-001", "imsi-002", "imsi-003"}

	var targets []DataCollectionTarget
	for _, supi := range supis {
		targets = append(targets, DataCollectionTarget{
			Supi:            supi,
			OriginalGroupId: groupId,
		})
	}

	// Verify all targets have correct OriginalGroupId
	for i, target := range targets {
		if target.OriginalGroupId != groupId {
			t.Errorf("Target[%d] OriginalGroupId = %q, want %q",
				i, target.OriginalGroupId, groupId)
		}
		if target.Supi != supis[i] {
			t.Errorf("Target[%d] Supi = %q, want %q",
				i, target.Supi, supis[i])
		}
	}
}

func TestTriggerTargetDataCollection_WithOriginalGroupId(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	smfConsumer := NewMockConsumerAPI(ctrl)
	var gotSupis []string
	smfConsumer.EXPECT().AdrfClient().Return(nil).AnyTimes()
	smfConsumer.EXPECT().
		SubscribeToSmf(gomock.Any(), "http://smf.example", gomock.AssignableToTypeOf(consumer.SmfSubscriptionOptions{})).
		DoAndReturn(func(_ any, _ string, opts consumer.SmfSubscriptionOptions) (string, error) {
			gotSupis = append(gotSupis, opts.Supi)
			return "smf-sub-" + opts.Supi, nil
		}).
		Times(3)

	// Simulate Group ID resolution: group → multiple SUPIs
	groupId := "group-test-001"
	targets := []DataCollectionTarget{
		{Supi: "imsi-001", OriginalGroupId: groupId},
		{Supi: "imsi-002", OriginalGroupId: groupId},
		{Supi: "imsi-003", OriginalGroupId: groupId},
	}

	nwdafSubId := "nwdaf-sub-group-01"

	if _, _, err := p.triggerTargetDataCollection(
		context.Background(),
		ctx,
		smfConsumer,
		[]string{"http://smf.example"},
		targets,
		nwdafSubId,
		"http://nwdaf/notify",
		"http://nwdaf/upf-notify",
		10,
	); err != nil {
		t.Fatalf("triggerTargetDataCollection() error = %v", err)
	}
	profileKey := canonicalCollectionProfileKey(10, []string{"TOTAL_VOLUME", "UL_VOLUME", "DL_VOLUME"})

	// Verify: Each SUPI should have its own SMF subscription
	for _, target := range targets {
		correlationId, found := ctx.GetSmfCorrelationIdForProfile(
			"supi="+target.Supi,
			"http://smf.example",
			profileKey,
		)
		if !found {
			t.Errorf("Expected SMF mapping for supi=%s", target.Supi)
			continue
		}

		sub := ctx.GetSmfSubscription(correlationId)
		if sub == nil {
			t.Errorf("Expected SmfSubscription for supi=%s", target.Supi)
			continue
		}

		if sub.Supi != target.Supi {
			t.Errorf("SmfSubscription.Supi = %q, want %q", sub.Supi, target.Supi)
		}
	}

	// Verify: NwdafSubResource should track OriginalGroupId
	resources := ctx.GetNwdafSubResources(nwdafSubId)
	if len(resources) != 3 {
		t.Fatalf("Expected 3 resources, got %d", len(resources))
	}

	for _, resource := range resources {
		if resource.OriginalGroupId != groupId {
			t.Errorf("Resource.OriginalGroupId = %q, want %q",
				resource.OriginalGroupId, groupId)
		}
	}
	if len(gotSupis) != len(targets) {
		t.Fatalf("expected %d SMF subscription calls, got %d", len(targets), len(gotSupis))
	}
}

func TestTriggerTargetDataCollection_MixedSupiAndGroup(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	smfConsumer := NewMockConsumerAPI(ctrl)
	var subscribeCalls int
	smfConsumer.EXPECT().AdrfClient().Return(nil).AnyTimes()
	smfConsumer.EXPECT().
		SubscribeToSmf(gomock.Any(), "http://smf.example", gomock.AssignableToTypeOf(consumer.SmfSubscriptionOptions{})).
		DoAndReturn(func(_ any, _ string, _ consumer.SmfSubscriptionOptions) (string, error) {
			subscribeCalls++
			return "smf-sub", nil
		}).
		Times(4)

	// Mix of direct SUPI and Group-resolved SUPIs
	targets := []DataCollectionTarget{
		{Supi: "imsi-direct-001", OriginalGroupId: ""},       // Direct SUPI
		{Supi: "imsi-group-001", OriginalGroupId: "group-A"}, // From Group A
		{Supi: "imsi-group-002", OriginalGroupId: "group-A"}, // From Group A
		{Supi: "imsi-group-003", OriginalGroupId: "group-B"}, // From Group B
	}

	nwdafSubId := "nwdaf-sub-mixed"

	if _, _, err := p.triggerTargetDataCollection(
		context.Background(),
		ctx,
		smfConsumer,
		[]string{"http://smf.example"},
		targets,
		nwdafSubId,
		"http://nwdaf/notify",
		"http://nwdaf/upf-notify",
		10,
	); err != nil {
		t.Fatalf("triggerTargetDataCollection() error = %v", err)
	}

	// Verify all 4 subscriptions created
	resources := ctx.GetNwdafSubResources(nwdafSubId)
	if len(resources) != 4 {
		t.Fatalf("Expected 4 resources, got %d", len(resources))
	}

	// Count by OriginalGroupId
	groupCounts := make(map[string]int)
	for _, r := range resources {
		groupCounts[r.OriginalGroupId]++
	}

	if groupCounts[""] != 1 {
		t.Errorf("Expected 1 direct SUPI, got %d", groupCounts[""])
	}
	if groupCounts["group-A"] != 2 {
		t.Errorf("Expected 2 from group-A, got %d", groupCounts["group-A"])
	}
	if groupCounts["group-B"] != 1 {
		t.Errorf("Expected 1 from group-B, got %d", groupCounts["group-B"])
	}
	if subscribeCalls != len(targets) {
		t.Fatalf("expected %d SMF subscription calls, got %d", len(targets), subscribeCalls)
	}
}

func TestTriggerDataCollectionUsesOneNrfDiscoveredEndpointSet(t *testing.T) {
	ctx := setupTestContext()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	discovered := []string{"http://smf-a.example", "http://smf-b.example"}
	consumerClient := NewMockConsumerAPI(ctrl)
	consumerClient.EXPECT().AdrfClient().Return(nil).AnyTimes()
	consumerClient.EXPECT().DiscoverSmfEventExposure(gomock.Any()).Return(discovered, nil).Times(1)
	var subscribedEndpoints []string
	consumerClient.EXPECT().
		SubscribeToSmf(gomock.Any(), gomock.Any(), gomock.AssignableToTypeOf(consumer.SmfSubscriptionOptions{})).
		DoAndReturn(func(_ context.Context, endpoint string, _ consumer.SmfSubscriptionOptions) (string, error) {
			subscribedEndpoints = append(subscribedEndpoints, endpoint)
			return "sub-" + endpoint, nil
		}).
		Times(2)

	cfg := smfDataCollectionConfig(factory.SmfEndpointSourceNRF, []string{"http://configured-must-not-be-used.example"})
	app := &subscriptionTestApp{ctx: context.Background(), cfg: cfg, consumer: consumerClient}
	p := NewProcessor(
		app,
		newTestAnlfCoordinator(app, &subscriptionTestBackend{}, nil),
		mtlf.NewMtlfService(app, nil, nil),
	)
	subscriptionID := "sub-nrf-discovery"
	events := []models.NwdafEventsSubscriptionEventSubscription{
		{
			Event: models.NwdafEvent_UE_COMMUNICATION,
			TgtUe: &models.TargetUeInformation{Supis: []string{"imsi-001"}},
		},
	}
	subscription := &nwdaf_context.Subscription{ID: subscriptionID, EventSubs: events, IsActive: true}
	subscription.SetRuntime(1, nwdaf_context.CollectionRequirements{
		SamplingIntervalSeconds: 10,
		RequiredMeasurements:    []string{"UL_VOLUME", "DL_VOLUME"},
	}, nil)
	ctx.AddSubscription(subscription)

	if err := p.TriggerDataCollection(context.Background(), events, subscriptionID); err != nil {
		t.Fatalf("TriggerDataCollection() error = %v", err)
	}
	if !slices.Equal(subscribedEndpoints, discovered) {
		t.Fatalf("subscribed endpoints = %v, want %v", subscribedEndpoints, discovered)
	}
	resources := ctx.GetNwdafSubResources(subscriptionID)
	if len(resources) != 2 {
		t.Fatalf("resource count = %d, want 2", len(resources))
	}
	for _, resource := range resources {
		if !slices.Contains(discovered, resource.SmfEndpoint) {
			t.Fatalf("resource endpoint = %q, want discovered endpoint", resource.SmfEndpoint)
		}
	}
}

func TestTriggerDataCollectionReusesUnchangedDiscoveredResource(t *testing.T) {
	ctx := setupTestContext()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	endpoint := "http://smf-reuse.example"
	supi := "imsi-999990000000001"
	consumerClient := NewMockConsumerAPI(ctrl)
	consumerClient.EXPECT().AdrfClient().Return(nil).AnyTimes()
	consumerClient.EXPECT().DiscoverSmfEventExposure(gomock.Any()).Return([]string{endpoint}, nil)

	app := &subscriptionTestApp{
		ctx:      context.Background(),
		cfg:      smfDataCollectionConfig(factory.SmfEndpointSourceNRF, nil),
		consumer: consumerClient,
	}
	p := NewProcessor(
		app,
		newTestAnlfCoordinator(app, &subscriptionTestBackend{}, nil),
		mtlf.NewMtlfService(app, nil, nil),
	)
	subscriptionID := "sub-reuse-discovered"
	events := []models.NwdafEventsSubscriptionEventSubscription{{
		Event: models.NwdafEvent_UE_COMMUNICATION,
		TgtUe: &models.TargetUeInformation{Supis: []string{supi}},
	}}
	subscription := &nwdaf_context.Subscription{ID: subscriptionID, EventSubs: events, IsActive: true}
	subscription.SetRuntime(1, nwdaf_context.CollectionRequirements{
		SamplingIntervalSeconds: 10,
		RequiredMeasurements:    []string{"UL_VOLUME"},
	}, nil)
	ctx.AddSubscription(subscription)
	profileKey := canonicalCollectionProfileKey(10, []string{"UL_VOLUME"})
	correlationID := "corr-reused-discovered"
	ctx.StoreSmfCorrelationIdForProfile("supi="+supi, endpoint, profileKey, correlationID)
	smfSubscription, _ := ctx.GetOrCreateSmfSubscription(correlationID, subscriptionID)
	smfSubscription.Lock()
	smfSubscription.Supi = supi
	smfSubscription.SmfEndpoint = endpoint
	smfSubscription.ProfileKey = profileKey
	smfSubscription.SmfSubId = "smf-sub-existing"
	smfSubscription.Unlock()
	ctx.AddNwdafSubResource(subscriptionID, nwdaf_context.NwdafSubResource{
		SmfEndpoint:   endpoint,
		Supi:          supi,
		CorrelationId: correlationID,
	})

	if err := p.TriggerDataCollection(context.Background(), events, subscriptionID); err != nil {
		t.Fatalf("TriggerDataCollection() error = %v", err)
	}
	resources := ctx.GetNwdafSubResources(subscriptionID)
	if len(resources) != 1 || resources[0].CorrelationId != correlationID {
		t.Fatalf("resources after unchanged discovery = %+v", resources)
	}
	_, _, refCount := smfSubscription.GetInfo()
	if refCount != 1 {
		t.Fatalf("reused SMF subscription refCount = %d, want 1", refCount)
	}
}

func TestTriggerDataCollectionDiscoveryFailurePreservesExistingResources(t *testing.T) {
	ctx := setupTestContext()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	discoveryErr := errors.New("NRF unavailable")
	consumerClient := NewMockConsumerAPI(ctrl)
	consumerClient.EXPECT().AdrfClient().Return(nil).AnyTimes()
	consumerClient.EXPECT().DiscoverSmfEventExposure(gomock.Any()).Return(nil, discoveryErr).Times(1)

	cfg := smfDataCollectionConfig(factory.SmfEndpointSourceNRF, nil)
	app := &subscriptionTestApp{ctx: context.Background(), cfg: cfg, consumer: consumerClient}
	p := NewProcessor(
		app,
		newTestAnlfCoordinator(app, &subscriptionTestBackend{}, nil),
		mtlf.NewMtlfService(app, nil, nil),
	)
	subscriptionID := "sub-preserve-on-discovery-failure"
	events := []models.NwdafEventsSubscriptionEventSubscription{
		{
			Event: models.NwdafEvent_UE_COMMUNICATION,
			TgtUe: &models.TargetUeInformation{Supis: []string{"imsi-001"}},
		},
	}
	subscription := &nwdaf_context.Subscription{ID: subscriptionID, EventSubs: events, IsActive: true}
	subscription.SetRuntime(1, nwdaf_context.CollectionRequirements{
		SamplingIntervalSeconds: 10,
		RequiredMeasurements:    []string{"UL_VOLUME"},
	}, nil)
	ctx.AddSubscription(subscription)
	ctx.AddNwdafSubResource(subscriptionID, nwdaf_context.NwdafSubResource{
		SmfEndpoint:   "http://previous-smf.example",
		Supi:          "imsi-001",
		CorrelationId: "corr-existing",
	})
	ctx.GetOrCreateSmfSubscription("corr-existing", subscriptionID)

	err := p.TriggerDataCollection(context.Background(), events, subscriptionID)
	if !errors.Is(err, discoveryErr) {
		t.Fatalf("TriggerDataCollection() error = %v, want discovery error", err)
	}
	resources := ctx.GetNwdafSubResources(subscriptionID)
	if len(resources) != 1 || resources[0].SmfEndpoint != "http://previous-smf.example" {
		t.Fatalf("resources after discovery failure = %+v, want previous resource preserved", resources)
	}
}

func TestTriggerDataCollectionReturnsErrorWhenAllSmfCallsFail(t *testing.T) {
	ctx := setupTestContext()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	consumerClient := NewMockConsumerAPI(ctrl)
	consumerClient.EXPECT().AdrfClient().Return(nil).AnyTimes()
	consumerClient.EXPECT().
		SubscribeToSmf(gomock.Any(), "http://smf.example", gomock.Any()).
		Return("", errors.New("SMF rejected subscription"))
	cfg := smfDataCollectionConfig(factory.SmfEndpointSourceConfigured, []string{"http://smf.example"})
	app := &subscriptionTestApp{ctx: context.Background(), cfg: cfg, consumer: consumerClient}
	p := NewProcessor(
		app,
		newTestAnlfCoordinator(app, &subscriptionTestBackend{}, nil),
		mtlf.NewMtlfService(app, nil, nil),
	)
	subscriptionID := "sub-total-failure"
	events := []models.NwdafEventsSubscriptionEventSubscription{
		{
			Event: models.NwdafEvent_UE_COMMUNICATION,
			TgtUe: &models.TargetUeInformation{Supis: []string{"imsi-001"}},
		},
	}
	subscription := &nwdaf_context.Subscription{ID: subscriptionID, EventSubs: events, IsActive: true}
	subscription.SetRuntime(1, nwdaf_context.CollectionRequirements{
		SamplingIntervalSeconds: 10,
		RequiredMeasurements:    []string{"UL_VOLUME"},
	}, nil)
	ctx.AddSubscription(subscription)

	err := p.TriggerDataCollection(context.Background(), events, subscriptionID)
	if err == nil || !strings.Contains(err.Error(), "all 1 SMF") {
		t.Fatalf("TriggerDataCollection() error = %v, want total SMF failure", err)
	}
	if resources := ctx.GetNwdafSubResources(subscriptionID); len(resources) != 0 {
		t.Fatalf("resources after total failure = %+v, want none", resources)
	}
}

func TestTriggerDataCollectionKeepsPartialSmfFanOutSuccess(t *testing.T) {
	ctx := setupTestContext()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	consumerClient := NewMockConsumerAPI(ctrl)
	consumerClient.EXPECT().AdrfClient().Return(nil).AnyTimes()
	consumerClient.EXPECT().
		SubscribeToSmf(gomock.Any(), "http://smf-a.example", gomock.Any()).
		Return("", errors.New("SMF A rejected subscription"))
	consumerClient.EXPECT().
		SubscribeToSmf(gomock.Any(), "http://smf-b.example", gomock.Any()).
		Return("smf-b-sub", nil)
	cfg := smfDataCollectionConfig(
		factory.SmfEndpointSourceConfigured,
		[]string{"http://smf-a.example", "http://smf-b.example"},
	)
	app := &subscriptionTestApp{ctx: context.Background(), cfg: cfg, consumer: consumerClient}
	p := NewProcessor(
		app,
		newTestAnlfCoordinator(app, &subscriptionTestBackend{}, nil),
		mtlf.NewMtlfService(app, nil, nil),
	)
	subscriptionID := "sub-partial-fan-out"
	events := []models.NwdafEventsSubscriptionEventSubscription{
		{
			Event: models.NwdafEvent_UE_COMMUNICATION,
			TgtUe: &models.TargetUeInformation{Supis: []string{"imsi-001"}},
		},
	}
	subscription := &nwdaf_context.Subscription{ID: subscriptionID, EventSubs: events, IsActive: true}
	subscription.SetRuntime(1, nwdaf_context.CollectionRequirements{
		SamplingIntervalSeconds: 10,
		RequiredMeasurements:    []string{"UL_VOLUME"},
	}, nil)
	ctx.AddSubscription(subscription)

	if err := p.TriggerDataCollection(context.Background(), events, subscriptionID); err != nil {
		t.Fatalf("TriggerDataCollection() error = %v, want partial success", err)
	}
	resources := ctx.GetNwdafSubResources(subscriptionID)
	if len(resources) != 1 || resources[0].SmfEndpoint != "http://smf-b.example" {
		t.Fatalf("resources after partial fan-out = %+v, want SMF B only", resources)
	}
}

func TestTriggerDataCollectionPropagatesCancellationAfterPartialSmfSuccess(t *testing.T) {
	tests := []struct {
		name         string
		cancelSource func(context.CancelFunc, context.CancelFunc)
	}{
		{
			name: "caller cancellation",
			cancelSource: func(cancelRequest, _ context.CancelFunc) {
				cancelRequest()
			},
		},
		{
			name: "application cancellation",
			cancelSource: func(_ context.CancelFunc, cancelApp context.CancelFunc) {
				cancelApp()
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := setupTestContext()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			requestCtx, cancelRequest := context.WithCancel(context.Background())
			defer cancelRequest()
			appCtx, cancelApp := context.WithCancel(context.Background())
			defer cancelApp()
			firstEndpoint := "http://smf-first.example"
			secondEndpoint := "http://smf-canceled.example"
			consumerClient := NewMockConsumerAPI(ctrl)
			consumerClient.EXPECT().AdrfClient().Return(nil).AnyTimes()
			consumerClient.EXPECT().
				SubscribeToSmf(gomock.Any(), firstEndpoint, gomock.Any()).
				DoAndReturn(func(
					subscribeCtx context.Context,
					_ string,
					_ consumer.SmfSubscriptionOptions,
				) (string, error) {
					tt.cancelSource(cancelRequest, cancelApp)
					<-subscribeCtx.Done()
					return "smf-first-sub", nil
				})

			cfg := smfDataCollectionConfig(
				factory.SmfEndpointSourceConfigured,
				[]string{firstEndpoint, secondEndpoint},
			)
			app := &subscriptionTestApp{ctx: appCtx, cfg: cfg, consumer: consumerClient}
			p := NewProcessor(
				app,
				newTestAnlfCoordinator(app, &subscriptionTestBackend{}, nil),
				mtlf.NewMtlfService(app, nil, nil),
			)
			subscriptionID := "sub-partial-cancellation"
			events := []models.NwdafEventsSubscriptionEventSubscription{{
				Event: models.NwdafEvent_UE_COMMUNICATION,
				TgtUe: &models.TargetUeInformation{Supis: []string{"imsi-999990000000004"}},
			}}
			subscription := &nwdaf_context.Subscription{ID: subscriptionID, EventSubs: events, IsActive: true}
			subscription.SetRuntime(1, nwdaf_context.CollectionRequirements{
				SamplingIntervalSeconds: 10,
				RequiredMeasurements:    []string{"UL_VOLUME"},
			}, nil)
			ctx.AddSubscription(subscription)

			err := p.TriggerDataCollection(requestCtx, events, subscriptionID)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("TriggerDataCollection() error = %v, want cancellation", err)
			}
			resources := ctx.GetNwdafSubResources(subscriptionID)
			if len(resources) != 1 || resources[0].SmfEndpoint != firstEndpoint {
				t.Fatalf("resources after partial cancellation = %+v, want first SMF only", resources)
			}
		})
	}
}

func TestTriggerDataCollectionStopsDiscoveryOnApplicationCancellation(t *testing.T) {
	ctx := setupTestContext()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	discoveryStarted := make(chan struct{})
	consumerClient := NewMockConsumerAPI(ctrl)
	consumerClient.EXPECT().AdrfClient().Return(nil).AnyTimes()
	consumerClient.EXPECT().
		DiscoverSmfEventExposure(gomock.Any()).
		DoAndReturn(func(discoveryCtx context.Context) ([]string, error) {
			close(discoveryStarted)
			<-discoveryCtx.Done()
			return nil, discoveryCtx.Err()
		})
	appCtx, cancelApp := context.WithCancel(context.Background())
	cfg := smfDataCollectionConfig(factory.SmfEndpointSourceNRF, nil)
	app := &subscriptionTestApp{ctx: appCtx, cfg: cfg, consumer: consumerClient}
	p := NewProcessor(
		app,
		newTestAnlfCoordinator(app, &subscriptionTestBackend{}, nil),
		mtlf.NewMtlfService(app, nil, nil),
	)
	subscriptionID := "sub-canceled-discovery"
	events := []models.NwdafEventsSubscriptionEventSubscription{
		{
			Event: models.NwdafEvent_UE_COMMUNICATION,
			TgtUe: &models.TargetUeInformation{Supis: []string{"imsi-001"}},
		},
	}
	subscription := &nwdaf_context.Subscription{ID: subscriptionID, EventSubs: events, IsActive: true}
	subscription.SetRuntime(1, nwdaf_context.CollectionRequirements{
		SamplingIntervalSeconds: 10,
		RequiredMeasurements:    []string{"UL_VOLUME"},
	}, nil)
	ctx.AddSubscription(subscription)

	result := make(chan error, 1)
	go func() {
		result <- p.TriggerDataCollection(context.Background(), events, subscriptionID)
	}()
	<-discoveryStarted
	cancelApp()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("TriggerDataCollection() error = %v, want application cancellation", err)
	}
}

func TestTriggerDataCollectionMigratesEndpointsWithApplicationOwnedCleanup(t *testing.T) {
	ctx := setupTestContext()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	oldEndpoint := "http://smf-old.example"
	newEndpoint := "http://smf-new.example"
	supi := "imsi-999990000000002"
	consumerClient := NewMockConsumerAPI(ctrl)
	consumerClient.EXPECT().AdrfClient().Return(nil).AnyTimes()
	consumerClient.EXPECT().DiscoverSmfEventExposure(gomock.Any()).Return([]string{newEndpoint}, nil)
	consumerClient.EXPECT().
		SubscribeToSmf(gomock.Any(), newEndpoint, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ consumer.SmfSubscriptionOptions) (string, error) {
			cancelRequest()
			return "smf-sub-new", nil
		})
	consumerClient.EXPECT().
		UnsubscribeFromSmf(gomock.Any(), oldEndpoint, "smf-sub-old").
		DoAndReturn(func(cleanupCtx context.Context, _, _ string) error {
			if cleanupCtx.Err() != nil {
				t.Fatalf("stale-resource cleanup inherited canceled request context: %v", cleanupCtx.Err())
			}
			if _, ok := cleanupCtx.Deadline(); !ok {
				t.Fatal("stale-resource cleanup context has no deadline")
			}
			return nil
		})

	app := &subscriptionTestApp{
		ctx:      context.Background(),
		cfg:      smfDataCollectionConfig(factory.SmfEndpointSourceNRF, nil),
		consumer: consumerClient,
	}
	p := NewProcessor(
		app,
		newTestAnlfCoordinator(app, &subscriptionTestBackend{}, nil),
		mtlf.NewMtlfService(app, nil, nil),
	)
	subscriptionID := "sub-endpoint-migration"
	events := []models.NwdafEventsSubscriptionEventSubscription{{
		Event: models.NwdafEvent_UE_COMMUNICATION,
		TgtUe: &models.TargetUeInformation{Supis: []string{supi}},
	}}
	subscription := &nwdaf_context.Subscription{ID: subscriptionID, EventSubs: events, IsActive: true}
	subscription.SetRuntime(1, nwdaf_context.CollectionRequirements{
		SamplingIntervalSeconds: 10,
		RequiredMeasurements:    []string{"UL_VOLUME"},
	}, nil)
	ctx.AddSubscription(subscription)
	profileKey := canonicalCollectionProfileKey(10, []string{"UL_VOLUME"})
	oldCorrelationID := "corr-old-endpoint"
	ctx.StoreSmfCorrelationIdForProfile("supi="+supi, oldEndpoint, profileKey, oldCorrelationID)
	oldSmfSubscription, _ := ctx.GetOrCreateSmfSubscription(oldCorrelationID, subscriptionID)
	oldSmfSubscription.Lock()
	oldSmfSubscription.Supi = supi
	oldSmfSubscription.SmfEndpoint = oldEndpoint
	oldSmfSubscription.ProfileKey = profileKey
	oldSmfSubscription.SmfSubId = "smf-sub-old"
	oldSmfSubscription.Unlock()
	ctx.AddNwdafSubResource(subscriptionID, nwdaf_context.NwdafSubResource{
		SmfEndpoint:   oldEndpoint,
		Supi:          supi,
		CorrelationId: oldCorrelationID,
	})

	if err := p.TriggerDataCollection(requestCtx, events, subscriptionID); err != nil {
		t.Fatalf("TriggerDataCollection() error = %v", err)
	}
	resources := ctx.GetNwdafSubResources(subscriptionID)
	if len(resources) != 1 || resources[0].SmfEndpoint != newEndpoint {
		t.Fatalf("resources after endpoint migration = %+v", resources)
	}
	if ctx.GetSmfSubscription(oldCorrelationID) != nil {
		t.Fatal("old SMF subscription still exists after migration")
	}
}

func smfDataCollectionConfig(endpointSource string, endpoints []string) *factory.Config {
	return &factory.Config{
		Configuration: &factory.Configuration{
			Sbi: &factory.Sbi{Scheme: "http", BindingIPv4: "127.0.0.1", Port: 8080},
			Smf: &factory.SmfConfig{
				Enabled:        true,
				EndpointSource: endpointSource,
				Endpoints:      endpoints,
				NotifUris: &factory.NotifUris{
					Smf: "http://127.0.0.1:8080/collector/notify",
					Upf: "http://127.0.0.1:8080/collector/upf-notify",
				},
			},
		},
	}
}

// =============================================================================
// AnLF runtime registration tests
// =============================================================================

func TestTriggerMlModelProvisioningCreatesPendingRuntimeCorrelation(t *testing.T) {
	ctx := setupTestContext()

	cfg := &factory.Config{
		Configuration: &factory.Configuration{
			Mtlf: &factory.MtlfConfig{Enabled: false},
		},
	}
	p := newTestProcessorWithConfig(t, cfg)

	subId := "test-sub-pending-runtime"
	eventSub := models.NwdafEventsSubscriptionEventSubscription{
		Event: models.NwdafEvent_UE_COMMUNICATION,
	}

	p.triggerMlModelProvisioning(&eventSub, subId)

	mlInfo := ctx.GetMlModelInfo(subId)
	if mlInfo == nil {
		t.Fatal("expected pending ML runtime correlation to be created")
	}
	if mlInfo.GetStatus() != nwdaf_context.MlModelStatus_PENDING {
		t.Fatalf("status = %q, want PENDING", mlInfo.GetStatus())
	}
}

func TestTriggerMlModelProvisioning_UsesAnlfServerNotificationURI(t *testing.T) {
	setupTestContext()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := &factory.Config{
		Configuration: &factory.Configuration{
			Anlf: &factory.AnlfConfig{
				Server: &factory.AuxiliaryServerConfig{
					BindingIPv4:  "127.0.0.1",
					RegisterIPv4: "10.1.2.3",
					Port:         8090,
				},
			},
			ExternalMtlf: &factory.ExternalMtlfConfig{
				Enabled:   true,
				Endpoints: []string{"http://mtlf.example"},
			},
		},
	}

	mockConsumer := NewMockConsumerAPI(ctrl)
	mockConsumer.EXPECT().AdrfClient().Return(nil).AnyTimes()
	mockConsumer.EXPECT().
		SubscribeToMtlf(gomock.Any(), "http://mtlf.example", gomock.AssignableToTypeOf(consumer.MtlfSubscriptionOptions{})).
		DoAndReturn(func(_ context.Context, _ string, opts consumer.MtlfSubscriptionOptions) (string, error) {
			if opts.NotifUri != "http://10.1.2.3:8090/mlmodel-notify" {
				t.Fatalf("NotifUri = %q, want %q",
					opts.NotifUri, "http://10.1.2.3:8090/mlmodel-notify")
			}
			return "mtlf-sub-123", nil
		}).
		Times(1)

	mockApp := mockapp.NewMockApp(ctrl)
	mockApp.EXPECT().CancelContext().Return(context.Background()).AnyTimes()
	mockApp.EXPECT().Consumer().Return(mockConsumer).AnyTimes()
	mockApp.EXPECT().Config().Return(cfg).AnyTimes()

	anlfService := newTestAnlfCoordinator(mockApp, &provisionBindingBackend{}, nil)
	mtlfService := mtlf.NewMtlfService(mockApp, nil, nil)
	p := NewProcessor(mockApp, anlfService, mtlfService)

	subId := "test-sub-dynamic"
	nwdaf_context.GetSelf().AddSubscription(&nwdaf_context.Subscription{ID: subId, IsActive: true, RuntimeRevision: 1})
	eventSub := models.NwdafEventsSubscriptionEventSubscription{
		Event: models.NwdafEvent_UE_COMMUNICATION,
		TgtUe: &models.TargetUeInformation{
			Supis: []string{"imsi-208930000000003"},
		},
	}

	p.triggerMlModelProvisioning(&eventSub, subId)
}
