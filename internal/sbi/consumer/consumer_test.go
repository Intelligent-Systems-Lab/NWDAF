package consumer

import (
	"testing"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

// TestNewConsumer tests Consumer initialization
func TestNewConsumer(t *testing.T) {
	c, err := NewConsumer()
	if err != nil {
		t.Errorf("NewConsumer() error = %v", err)
	}
	if c == nil {
		t.Error("NewConsumer() returned nil")
	}
	if c.NsmfService == nil {
		t.Error("NsmfService should be initialized")
	}
}

// TestConsumerContext tests Consumer.Context() method
func TestConsumerContext(t *testing.T) {
	nwdaf_context.Init()
	c, _ := NewConsumer()

	ctx := c.Context()
	if ctx == nil {
		t.Error("Context() returned nil")
	}
}

// TestNsmfServiceHTTPClient tests HTTP client is properly initialized
func TestNsmfServiceHTTPClient(t *testing.T) {
	c, _ := NewConsumer()

	// Get the HTTP client - should be a single instance
	client := c.NsmfService.HTTPClient()
	if client == nil {
		t.Error("HTTPClient() returned nil")
	}

	// Same client should be returned (single instance)
	client2 := c.NsmfService.HTTPClient()
	if client != client2 {
		t.Error("HTTPClient() should return the same instance")
	}

	// Verify timeout is set
	if client.Timeout == 0 {
		t.Error("HTTP client timeout should be set")
	}
}

// TestEmbeddedMethodPromotion tests that NsmfService methods are promoted to Consumer
func TestEmbeddedMethodPromotion(t *testing.T) {
	nwdaf_context.Init()
	c, _ := NewConsumer()

	// These methods should be accessible directly on Consumer via embedding
	// We can't call them without a real SMF, but we can verify they exist

	// Verify the method exists and is callable (will fail due to no SMF, but that's ok)
	// The point is to verify the method is promoted
	var _ func(string, string, []string, string) (string, error) = c.SubscribeToSmf
	var _ func(string, string) error = c.UnsubscribeFromSmf
	var _ func(string, string, string, string) (string, error) = c.SubscribeForUeCommunication
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
