package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	// Nsmf_EventExposure API path
	SmfEventExposurePath = "/nsmf-event-exposure/v1/subscriptions"
)

// NsmfService handles SMF Event Exposure API interactions
// Exported to allow method promotion via embedding in Consumer
type NsmfService struct {
	// Single HTTP client - Go's http.Client is safe for concurrent use
	// and handles connection pooling internally
	httpClient *http.Client
}

// NewNsmfService creates a new NsmfService with optimized HTTP client
func NewNsmfService() *NsmfService {
	return &NsmfService{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// SmfSubscriptionOptions configures SMF event exposure subscription
// Per TS 23.502 §4.15.4.5.2: Group IDs are resolved to SUPIs by NWDAF
// before subscribing to SMF, so subscriptions are always SUPI-based
type SmfSubscriptionOptions struct {
	Supi        string // Target SUPI for subscription
	NotifUri    string
	NotifId     string // Correlation ID for notification routing
	EventSubs   []ExtendedEventSubscription
	NotifMethod string // "PERIODIC" | "ONE_TIME"
	RepPeriod   int32  // Reporting period in seconds (for PERIODIC)
}

// SubscribeToSmf creates an event exposure subscription to SMF
// Per TS 23.502 §4.15.4.5.2: Subscriptions are always SUPI-based
// (Group IDs are resolved by NWDAF before calling this function)
func (s *NsmfService) SubscribeToSmf(
	ctx context.Context,
	smfEndpoint string,
	opts SmfSubscriptionOptions,
) (string, error) {
	request := ExtendedNsmfEventExposure{
		Supi:        opts.Supi,
		NotifUri:    opts.NotifUri,
		NotifId:     opts.NotifId,
		EventSubs:   opts.EventSubs,
		NotifMethod: opts.NotifMethod,
		RepPeriod:   opts.RepPeriod,
	}

	subscriptionId, err := s.sendRequest(ctx, smfEndpoint, &request)
	if err != nil {
		return "", err
	}

	if subscriptionId == "" {
		subscriptionId = opts.NotifId
	}

	return subscriptionId, nil
}

// BuildUpfEventSubs constructs ExtendedEventSubscription for UPF_EVENT
// upfNotifUri: notification URI for UPF events
// volume: include VOLUME_MEASUREMENT
// throughput: include THROUGHPUT_MEASUREMENT
func BuildUpfEventSubs(upfNotifUri string, volume, throughput bool) []ExtendedEventSubscription {
	measureTypes := []MeasurementType{}
	if volume {
		measureTypes = append(measureTypes, MeasurementType_VOLUME_MEASUREMENT)
	}
	if throughput {
		measureTypes = append(measureTypes, MeasurementType_THROUGHPUT_MEASUREMENT)
	}

	return []ExtendedEventSubscription{
		{
			Event: SmfEvent_UPF_EVENT,
			UpfEvents: []UpfEvent{
				{
					Type:                     UpfEventType_USER_DATA_USAGE_MEASURES,
					MeasurementTypes:         measureTypes,
					GranularityOfMeasurement: Granularity_PER_SESSION,
				},
			},
			BundlingAllowed:       true,
			BundledEventNotifyUri: upfNotifUri,
		},
	}
}

// UnsubscribeFromSmf deletes an event exposure subscription from SMF
func (s *NsmfService) UnsubscribeFromSmf(
	ctx context.Context,
	smfEndpoint string,
	subscriptionId string,
) error {
	url := smfEndpoint + SmfEventExposurePath + "/" + subscriptionId

	ctx, cancel, err := timeoutContextFromParent(ctx, 10*time.Second, "SMF unsubscription")
	if err != nil {
		return err
	}
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request to SMF: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			consumerLog.Debugf("failed to close response body (may be ignored): %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("SMF unsubscription failed: status=%d", resp.StatusCode)
	}

	// NOTE: Resource cleanup handled by cleanupDataCollection()
	// in eventssubscription.go via ReleaseSmfResource()

	return nil
}

// sendRequest sends a POST subscription request to SMF
// Uses interface{} to handle both standard and extended request types
func (s *NsmfService) sendRequest(ctx context.Context, smfEndpoint string, request interface{}) (string, error) {
	url := smfEndpoint + SmfEventExposurePath

	jsonData, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	ctx, cancel, err := timeoutContextFromParent(ctx, 10*time.Second, "SMF subscription")
	if err != nil {
		return "", err
	}
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request to SMF: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			consumerLog.Debugf("failed to close response body (may be ignored): %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("SMF subscription failed: status=%d", resp.StatusCode)
	}

	return s.parseSubscriptionId(resp), nil
}

// parseSubscriptionId extracts subscription ID from response
// Location header is the primary source per 3GPP TS 29.508
func (s *NsmfService) parseSubscriptionId(resp *http.Response) string {
	// Try Location header first (primary method per 3GPP spec)
	location := resp.Header.Get("Location")
	if location != "" {
		// Extract the last segment (subscription ID) from URI
		// e.g., "/nsmf-event-exposure/v1/subscriptions/sub-123" -> "sub-123"
		if idx := strings.LastIndex(location, "/"); idx >= 0 {
			return location[idx+1:]
		}
		return location
	}

	// Fallback: try to parse body (optional, some SMFs may include subId)
	var response struct {
		SubId string `json:"subId"`
	}
	// Body parsing is optional fallback, log but don't fail
	if decodeErr := json.NewDecoder(resp.Body).Decode(&response); decodeErr != nil {
		consumerLog.Debugf("failed to decode subscription response body (may be ignored): %v", decodeErr)
	}

	return response.SubId
}

// HTTPClient returns the underlying HTTP client for testing
func (s *NsmfService) HTTPClient() *http.Client {
	return s.httpClient
}
