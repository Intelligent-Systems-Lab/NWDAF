package consumer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

const (
	// Nsmf_EventExposure API path
	SmfEventExposurePath = "/nsmf-event-exposure/v1/subscriptions"
)

// NsmfService handles SMF Event Exposure API interactions
// Exported to allow method promotion via embedding in Consumer
type NsmfService struct {
	consumer *Consumer

	// Single HTTP client - Go's http.Client is safe for concurrent use
	// and handles connection pooling internally
	httpClient *http.Client
}

// NewNsmfService creates a new NsmfService with optimized HTTP client
func NewNsmfService(c *Consumer) *NsmfService {
	return &NsmfService{
		consumer: c,
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

// SubscribeToSmf creates a basic event exposure subscription to SMF
func (s *NsmfService) SubscribeToSmf(
	smfEndpoint string,
	supi string,
	events []string,
	notifUri string,
) (string, error) {
	consumerLog.Infof("Subscribing to SMF: endpoint=%s, supi=%s", smfEndpoint, supi)

	notifId := uuid.New().String()

	// Build subscription request
	eventSubs := make([]models.SmfEventExposureEventSubscription, 0, len(events))
	for _, event := range events {
		eventSubs = append(eventSubs, models.SmfEventExposureEventSubscription{
			Event: models.SmfEvent(event),
		})
	}

	request := models.NsmfEventExposure{
		Supi:      supi,
		NotifUri:  notifUri,
		NotifId:   notifId,
		EventSubs: eventSubs,
	}

	subscriptionId, err := s.sendRequest(smfEndpoint, &request)
	if err != nil {
		return "", err
	}

	if subscriptionId == "" {
		subscriptionId = notifId
	}

	// Store subscription in context
	s.storeSmfSubscription(subscriptionId, smfEndpoint, supi, notifId, events)

	consumerLog.Infof("SMF subscription created: id=%s", subscriptionId)
	return subscriptionId, nil
}

// UnsubscribeFromSmf deletes an event exposure subscription from SMF
func (s *NsmfService) UnsubscribeFromSmf(
	smfEndpoint string,
	subscriptionId string,
) error {
	consumerLog.Infof("Unsubscribing from SMF: endpoint=%s, subId=%s", smfEndpoint, subscriptionId)

	url := smfEndpoint + SmfEventExposurePath + "/" + subscriptionId

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request to SMF: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("SMF unsubscription failed: status=%d, body=%s", resp.StatusCode, string(body))
	}

	// Remove from context
	ctx := s.consumer.Context()
	ctx.DeleteSmfSubscription(subscriptionId)

	consumerLog.Infof("SMF subscription deleted: id=%s", subscriptionId)
	return nil
}

// SubscribeForUeCommunication subscribes to SMF for all UE communication data
// Combines UPF_EVENT for traffic volume measurements
func (s *NsmfService) SubscribeForUeCommunication(
	smfEndpoint string,
	supi string,
	smfNotifUri string,
	upfNotifUri string,
) (string, error) {
	consumerLog.Infof("Subscribing for UE Communication: endpoint=%s, supi=%s", smfEndpoint, supi)

	notifId := uuid.New().String()

	// Build subscription with UPF_EVENT for traffic volume data
	eventSubs := []ExtendedEventSubscription{
		{
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
			BundledEventNotifyUri: upfNotifUri,
		},
	}

	request := ExtendedNsmfEventExposure{
		Supi:      supi,
		NotifUri:  smfNotifUri,
		NotifId:   notifId,
		EventSubs: eventSubs,
	}

	subscriptionId, err := s.sendRequest(smfEndpoint, &request)
	if err != nil {
		return "", err
	}

	if subscriptionId == "" {
		subscriptionId = notifId
	}

	// Store subscription
	s.storeSmfSubscription(subscriptionId, smfEndpoint, supi, notifId, []string{string(SmfEvent_UPF_EVENT)})

	consumerLog.Infof("SMF UE Communication subscription created: id=%s", subscriptionId)
	return subscriptionId, nil
}

// sendRequest sends a POST subscription request to SMF
// Uses interface{} to handle both standard and extended request types
func (s *NsmfService) sendRequest(smfEndpoint string, request interface{}) (string, error) {
	url := smfEndpoint + SmfEventExposurePath

	jsonData, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	consumerLog.Debugf("SMF subscription request: %s", string(jsonData))

	resp, err := s.httpClient.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to send request to SMF: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("SMF subscription failed: status=%d, body=%s", resp.StatusCode, string(body))
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
	// Ignore decode errors - body parsing is optional fallback
	_ = json.NewDecoder(resp.Body).Decode(&response)

	return response.SubId
}

// storeSmfSubscription stores subscription info in context
func (s *NsmfService) storeSmfSubscription(subscriptionId, smfEndpoint, supi, notifId string, events []string) {
	ctx := s.consumer.Context()
	sub := &nwdaf_context.SmfSubscription{
		SubscriptionId: subscriptionId,
		SmfEndpoint:    smfEndpoint,
		TargetSupi:     supi,
		NotifId:        notifId,
		Events:         events,
		CreatedAt:      time.Now(),
	}
	ctx.StoreSmfSubscription(sub)
}

// HTTPClient returns the underlying HTTP client for testing
func (s *NsmfService) HTTPClient() *http.Client {
	return s.httpClient
}
