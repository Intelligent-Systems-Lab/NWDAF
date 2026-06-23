package consumer

import (
	"net/http"
	"testing"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

type testSmfService struct {
	subscribeCalled   bool
	unsubscribeCalled bool
	subscriptionID    string
	err               error
	httpClient        *http.Client
}

func (s *testSmfService) SubscribeToSmf(_ string, _ SmfSubscriptionOptions) (string, error) {
	s.subscribeCalled = true
	return s.subscriptionID, s.err
}

func (s *testSmfService) UnsubscribeFromSmf(_ string, _ string) error {
	s.unsubscribeCalled = true
	return s.err
}

func (s *testSmfService) HTTPClient() *http.Client {
	return s.httpClient
}

type testMtlfService struct {
	subscribeCalled   bool
	unsubscribeCalled bool
	subscriptionID    string
	err               error
	httpClient        *http.Client
}

func (s *testMtlfService) SubscribeToMtlf(_ string, _ MtlfSubscriptionOptions) (string, error) {
	s.subscribeCalled = true
	return s.subscriptionID, s.err
}

func (s *testMtlfService) UnsubscribeFromMtlf(_ string, _ string) error {
	s.unsubscribeCalled = true
	return s.err
}

func (s *testMtlfService) HTTPClient() *http.Client {
	return s.httpClient
}

// TestNewConsumer tests Consumer initialization
func TestNewConsumer(t *testing.T) {
	c, err := NewConsumer()
	if err != nil {
		t.Errorf("NewConsumer() error = %v", err)
	}
	if c == nil {
		t.Fatal("NewConsumer() returned nil")
	} else if c.SmfService() == nil {
		t.Error("SMF service should be initialized")
	} else if c.MtlfService() == nil {
		t.Error("MTLF service should be initialized")
	}
}

// TestConsumerContext tests Consumer.Context() method
func TestConsumerContext(t *testing.T) {
	nwdaf_context.Init()
	c, err := NewConsumer()
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}

	ctx := c.Context()
	if ctx == nil {
		t.Error("Context() returned nil")
	}
}

// TestNsmfServiceHTTPClient tests HTTP client is properly initialized
func TestNsmfServiceHTTPClient(t *testing.T) {
	c, err := NewConsumer()
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}

	// Get the HTTP client - should be a single instance
	client := c.SmfService().HTTPClient()
	if client == nil {
		t.Fatal("HTTPClient() returned nil")
	} else {
		// Same client should be returned (single instance)
		client2 := c.SmfService().HTTPClient()
		if client != client2 {
			t.Error("HTTPClient() should return the same instance")
		}

		// Verify timeout is set
		if client.Timeout == 0 {
			t.Error("HTTP client timeout should be set")
		}
	}
}

func TestConsumerDelegatesToInjectedServices(t *testing.T) {
	smfService := &testSmfService{
		subscriptionID: "smf-sub-1",
		httpClient:     &http.Client{},
	}
	mtlfService := &testMtlfService{
		subscriptionID: "mtlf-sub-1",
		httpClient:     &http.Client{},
	}
	c := NewConsumerWithServices(smfService, mtlfService, nil)

	if _, err := c.SubscribeToSmf("http://smf", SmfSubscriptionOptions{}); err != nil {
		t.Fatalf("SubscribeToSmf returned error: %v", err)
	}
	if _, err := c.SubscribeToMtlf("http://mtlf", MtlfSubscriptionOptions{}); err != nil {
		t.Fatalf("SubscribeToMtlf returned error: %v", err)
	}
	if err := c.UnsubscribeFromSmf("http://smf", "smf-sub-1"); err != nil {
		t.Fatalf("UnsubscribeFromSmf returned error: %v", err)
	}
	if err := c.UnsubscribeFromMtlf("http://mtlf", "mtlf-sub-1"); err != nil {
		t.Fatalf("UnsubscribeFromMtlf returned error: %v", err)
	}

	if !smfService.subscribeCalled || !smfService.unsubscribeCalled {
		t.Fatal("expected SMF service delegation to be invoked")
	}
	if !mtlfService.subscribeCalled || !mtlfService.unsubscribeCalled {
		t.Fatal("expected MTLF service delegation to be invoked")
	}
}

// TestExtendedEventSubscription tests UPF event subscription model
func TestExtendedEventSubscription(t *testing.T) {
	sub := ExtendedEventSubscription{
		Event: SmfEvent_UPF_EVENT,
		UpfEvents: []UpfEvent{
			{
				Type: UpfEventType_USER_DATA_USAGE_MEASURES,
				MeasurementTypes: []MeasurementType{
					MeasurementType_VOLUME_MEASUREMENT,
					MeasurementType_THROUGHPUT_MEASUREMENT,
				},
				GranularityOfMeasurement: Granularity_PER_SESSION,
			},
		},
		BundlingAllowed:       true,
		BundledEventNotifyUri: "http://localhost:8080/upf-notify",
	}

	if sub.Event != SmfEvent_UPF_EVENT {
		t.Errorf("Event = %v, want UPF_EVENT", sub.Event)
	}
	if len(sub.UpfEvents) != 1 {
		t.Errorf("UpfEvents length = %v, want 1", len(sub.UpfEvents))
	}
	if !sub.BundlingAllowed {
		t.Error("BundlingAllowed should be true")
	}
}
