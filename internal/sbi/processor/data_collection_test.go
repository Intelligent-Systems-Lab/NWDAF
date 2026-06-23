package processor

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

type fakeSmfService struct {
	subscribeCalls []struct {
		endpoint string
		opts     consumer.SmfSubscriptionOptions
	}
	unsubscribeCalls []struct {
		endpoint       string
		subscriptionID string
	}
	nextID int
}

func (f *fakeSmfService) SubscribeToSmf(
	smfEndpoint string,
	opts consumer.SmfSubscriptionOptions,
) (string, error) {
	f.subscribeCalls = append(f.subscribeCalls, struct {
		endpoint string
		opts     consumer.SmfSubscriptionOptions
	}{
		endpoint: smfEndpoint,
		opts:     opts,
	})
	f.nextID++
	return fmt.Sprintf("smf-sub-%d", f.nextID), nil
}

func (f *fakeSmfService) UnsubscribeFromSmf(smfEndpoint string, subscriptionID string) error {
	f.unsubscribeCalls = append(f.unsubscribeCalls, struct {
		endpoint       string
		subscriptionID string
	}{
		endpoint:       smfEndpoint,
		subscriptionID: subscriptionID,
	})
	return nil
}

func (f *fakeSmfService) HTTPClient() *http.Client {
	return &http.Client{}
}

func TestTriggerTargetDataCollection_ResourceReuse(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor()
	smfService := &fakeSmfService{}
	smfConsumer := consumer.NewConsumerWithServices(smfService, nil, nil)

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

	// Access unexported method via reflection? No, I am in package processor!
	// But triggerTargetDataCollection is in data_collection.go which belongs to package processor.
	// So I can call it directly.
	p.triggerTargetDataCollection(
		ctx,
		smfConsumer,
		smfEndpoints,
		targets,
		nwdafSubId1,
		"http://nwdaf/notify",
		"http://nwdaf/upf-notify",
		10,
	)

	// 5. Verify First Subscription
	targetKey := targetSupi + "@" + smfEndpoint
	correlationId1, found := ctx.GetSmfCorrelationId("supi="+targetSupi, smfEndpoint)
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
	p.triggerTargetDataCollection(
		ctx,
		smfConsumer,
		smfEndpoints,
		targets,
		nwdafSubId2,
		"http://nwdaf/notify",
		"http://nwdaf/upf-notify",
		10,
	)

	// 7. Verify Reuse
	correlationId2, found2 := ctx.GetSmfCorrelationId("supi="+targetSupi, smfEndpoint)
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
	_, found3 := ctx.GetSmfCorrelationId("supi="+targetSupi, smfEndpoint)
	if !found3 {
		t.Error("Mapping should persist until last reference removed")
	}

	// 9. Cleanup Second (Last)
	shouldDelete2, _ := ctx.ReleaseSmfSubscription(correlationId1, nwdafSubId2)
	if !shouldDelete2 {
		t.Error("Should delete subscription when RefCount=0")
	}

	// Mapping should be gone
	_, found4 := ctx.GetSmfCorrelationId("supi="+targetSupi, smfEndpoint)
	if found4 {
		t.Error("Mapping should be removed after last reference removed")
	}
	if len(smfService.subscribeCalls) != 1 {
		t.Fatalf("expected one SMF subscription call, got %d", len(smfService.subscribeCalls))
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
	p := newTestProcessor()
	smfService := &fakeSmfService{}
	smfConsumer := consumer.NewConsumerWithServices(smfService, nil, nil)

	// Simulate Group ID resolution: group → multiple SUPIs
	groupId := "group-test-001"
	targets := []DataCollectionTarget{
		{Supi: "imsi-001", OriginalGroupId: groupId},
		{Supi: "imsi-002", OriginalGroupId: groupId},
		{Supi: "imsi-003", OriginalGroupId: groupId},
	}

	nwdafSubId := "nwdaf-sub-group-01"

	p.triggerTargetDataCollection(
		ctx,
		smfConsumer,
		[]string{"http://smf.example"},
		targets,
		nwdafSubId,
		"http://nwdaf/notify",
		"http://nwdaf/upf-notify",
		10,
	)

	// Verify: Each SUPI should have its own SMF subscription
	for _, target := range targets {
		correlationId, found := ctx.GetSmfCorrelationId("supi="+target.Supi, "http://smf.example")
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
	if len(smfService.subscribeCalls) != len(targets) {
		t.Fatalf("expected %d SMF subscription calls, got %d", len(targets), len(smfService.subscribeCalls))
	}
}

func TestTriggerTargetDataCollection_MixedSupiAndGroup(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor()
	smfService := &fakeSmfService{}
	smfConsumer := consumer.NewConsumerWithServices(smfService, nil, nil)

	// Mix of direct SUPI and Group-resolved SUPIs
	targets := []DataCollectionTarget{
		{Supi: "imsi-direct-001", OriginalGroupId: ""},       // Direct SUPI
		{Supi: "imsi-group-001", OriginalGroupId: "group-A"}, // From Group A
		{Supi: "imsi-group-002", OriginalGroupId: "group-A"}, // From Group A
		{Supi: "imsi-group-003", OriginalGroupId: "group-B"}, // From Group B
	}

	nwdafSubId := "nwdaf-sub-mixed"

	p.triggerTargetDataCollection(
		ctx,
		smfConsumer,
		[]string{"http://smf.example"},
		targets,
		nwdafSubId,
		"http://nwdaf/notify",
		"http://nwdaf/upf-notify",
		10,
	)

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
	if len(smfService.subscribeCalls) != len(targets) {
		t.Fatalf("expected %d SMF subscription calls, got %d", len(targets), len(smfService.subscribeCalls))
	}
}

// =============================================================================
// Static ML Model URL Tests
// =============================================================================

func TestTriggerMlModelProvisioning_StaticUrl(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor()

	// 1. Setup Config with Static Model URL
	// Create minimal config structure
	cfg := &factory.Config{
		Configuration: &factory.Configuration{
			Mtlf: &factory.MtlfConfig{
				Enabled:        false, // MTLF Disabled
				StaticModelUrl: "file:///test/model.pth",
			},
			MlService: &factory.MlServiceConfig{
				Enabled:  true,
				Endpoint: "http://ml-service-mock",
			},
		},
	}
	// Save current config to restore later
	oldCfg := factory.NwdafConfig
	factory.NwdafConfig = cfg
	defer func() { factory.NwdafConfig = oldCfg }()

	// 2. Setup Subscription
	subId := "test-sub-static-url"
	eventSub := models.NwdafEventsSubscriptionEventSubscription{
		Event: models.NwdafEvent_UE_COMMUNICATION,
	}

	// 3. Trigger
	p.triggerMlModelProvisioning(&eventSub, subId)

	// 4. Verify logic path
	// Check if MlModelInfo was created
	// Since triggerMlModelProvisioning is async (goroutine), we need to wait briefly
	// However, the creation of MlModelInfo happens synchronously before the goroutine starts
	// in our implementation of triggerMlModelProvisioning.

	mlInfo := ctx.GetMlModelInfo(subId)
	if mlInfo == nil {
		t.Fatal("Expected MlModelInfo to be created")
	} else if mlInfo.ModelUrl != "file:///test/model.pth" {
		t.Errorf("Expected ModelUrl to be %s, got %s", "file:///test/model.pth", mlInfo.ModelUrl)
	}
}

func TestTriggerMlModelProvisioning_MtlfDisabledNoStaticUrl(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor()

	// Setup Config: MTLF disabled, no static URL
	cfg := &factory.Config{
		Configuration: &factory.Configuration{
			Mtlf: &factory.MtlfConfig{
				Enabled:        false,
				StaticModelUrl: "", // Empty
			},
		},
	}
	oldCfg := factory.NwdafConfig
	factory.NwdafConfig = cfg
	defer func() { factory.NwdafConfig = oldCfg }()

	subId := "test-sub-no-action"
	eventSub := models.NwdafEventsSubscriptionEventSubscription{
		Event: models.NwdafEvent_UE_COMMUNICATION,
	}

	p.triggerMlModelProvisioning(&eventSub, subId)

	// Verify no MlModelInfo created
	mlInfo := ctx.GetMlModelInfo(subId)
	if mlInfo != nil {
		t.Error("Expected no MlModelInfo to be created")
	}
}
