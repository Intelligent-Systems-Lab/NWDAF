package processor

import (
	"net/http"
	"net/http/httptest"
	"testing"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
)

// MockRoundTripper for intercepting HTTP requests
type MockRoundTripper struct {
	RoundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *MockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.RoundTripFunc(req)
}

func TestTriggerTargetDataCollection_ResourceReuse(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor()

	// 1. Start Mock SMF Server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Header().Set("Location", r.URL.String()+"/sub-123")
		if _, err := w.Write([]byte(`{"notifId": "test-notif-id"}`)); err != nil {
			t.Logf("Failed to write response: %v", err)
		}
	}))
	defer ts.Close()

	// 2. Setup Consumer (real one, will talk to httptest server)
	smfConsumer, err := consumer.NewConsumer()
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	// 3. Define Targets
	targetSupi := "imsi-208930000000003"
	targets := []DataCollectionTarget{
		{
			TargetType: nwdaf_context.TargetType_SUPI,
			Supi:       targetSupi,
		},
	}

	// 4. Trigger First Subscription
	nwdafSubId1 := "nwdaf-sub-01"
	smfEndpoints := []string{ts.URL}

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
	targetKey := targetSupi + "@" + ts.URL
	correlationId1, found := ctx.GetSmfCorrelationId("supi="+targetSupi, ts.URL)
	if !found {
		t.Fatalf("Expected SMF mapping for key %s", targetKey)
	}

	sub1 := ctx.GetSmfSubscription(correlationId1)
	if sub1 == nil {
		t.Fatal("Expected SmfSubscription to be created")
	}
	if sub1.RefCount != 1 {
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
	correlationId2, found2 := ctx.GetSmfCorrelationId("supi="+targetSupi, ts.URL)
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
	_, found3 := ctx.GetSmfCorrelationId("supi="+targetSupi, ts.URL)
	if !found3 {
		t.Error("Mapping should persist until last reference removed")
	}

	// 9. Cleanup Second (Last)
	shouldDelete2, _ := ctx.ReleaseSmfSubscription(correlationId1, nwdafSubId2)
	if !shouldDelete2 {
		t.Error("Should delete subscription when RefCount=0")
	}

	// Mapping should be gone
	_, found4 := ctx.GetSmfCorrelationId("supi="+targetSupi, ts.URL)
	if found4 {
		t.Error("Mapping should be removed after last reference removed")
	}
}
