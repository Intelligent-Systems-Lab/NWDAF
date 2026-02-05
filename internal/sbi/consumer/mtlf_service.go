package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/free5gc/openapi/models"
)

const (
	// Nnwdaf_MLModelProvision API path
	MtlfMLModelProvisionPath = "/nnwdaf-mlmodelprovision/v1/subscriptions"
)

// NmtlfService handles MTLF ML Model Provision API interactions
// Per TS 29.520 §5.4: Nnwdaf_MLModelProvision Service API
type NmtlfService struct {
	consumer   *Consumer
	httpClient *http.Client
}

// NewNmtlfService creates a new NmtlfService with HTTP client
func NewNmtlfService(c *Consumer) *NmtlfService {
	return &NmtlfService{
		consumer: c,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        50,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// MtlfSubscriptionOptions configures ML Model Provision subscription
type MtlfSubscriptionOptions struct {
	NotifUri string                      // Callback URI for ML model notifications
	NotifId  string                      // Notification correlation ID
	Event    models.NwdafEvent           // Analytics event type
	TgtUe    *models.TargetUeInformation // Target UE information
}

// MtlfEventSubscription represents ML event subscription per TS 29.520
type MtlfEventSubscription struct {
	MLEvent string                      `json:"mLEvent"`
	TgtUe   *models.TargetUeInformation `json:"tgtUe,omitempty"`
}

// MtlfSubscriptionRequest represents NwdafMLModelProvSubsc per TS 29.520 §5.4
type MtlfSubscriptionRequest struct {
	MLEventSubscs []MtlfEventSubscription `json:"mLEventSubscs"`
	NotifUri      string                  `json:"notifUri"`
	NotifCorreId  string                  `json:"notifCorreId,omitempty"`
}

// SubscribeToMtlf creates an ML Model Provision subscription to MTLF
// Per TS 29.520 §5.4.3.2.3.1: POST to /subscriptions
func (s *NmtlfService) SubscribeToMtlf(
	mtlfEndpoint string,
	opts MtlfSubscriptionOptions,
) (string, error) {
	consumerLog.Infof("Subscribing to MTLF: endpoint=%s, event=%s, notifId=%s",
		mtlfEndpoint, opts.Event, opts.NotifId)

	// Build subscription request
	eventSubs := []MtlfEventSubscription{
		{
			MLEvent: string(opts.Event),
			TgtUe:   opts.TgtUe,
		},
	}

	request := MtlfSubscriptionRequest{
		MLEventSubscs: eventSubs,
		NotifUri:      opts.NotifUri,
		NotifCorreId:  opts.NotifId,
	}

	subscriptionId, err := s.sendSubscribeRequest(mtlfEndpoint, &request)
	if err != nil {
		return "", err
	}

	if subscriptionId == "" {
		subscriptionId = opts.NotifId
	}

	consumerLog.Infof("MTLF subscription created: id=%s", subscriptionId)
	return subscriptionId, nil
}

// UnsubscribeFromMtlf deletes an ML Model Provision subscription from MTLF
// Per TS 29.520 §5.4.3.3.3.2: DELETE /subscriptions/{subscriptionId}
func (s *NmtlfService) UnsubscribeFromMtlf(
	mtlfEndpoint string,
	subscriptionId string,
) error {
	consumerLog.Infof("Unsubscribing from MTLF: endpoint=%s, subId=%s", mtlfEndpoint, subscriptionId)

	url := mtlfEndpoint + MtlfMLModelProvisionPath + "/" + subscriptionId

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request to MTLF: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			consumerLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("MTLF unsubscription failed: status=%d", resp.StatusCode)
		}
		return fmt.Errorf("MTLF unsubscription failed: status=%d, body=%s", resp.StatusCode, string(body))
	}

	consumerLog.Infof("MTLF subscription deleted: id=%s", subscriptionId)
	return nil
}

// sendSubscribeRequest sends a POST subscription request to MTLF
func (s *NmtlfService) sendSubscribeRequest(mtlfEndpoint string, request *MtlfSubscriptionRequest) (string, error) {
	url := mtlfEndpoint + MtlfMLModelProvisionPath

	jsonData, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	consumerLog.Debugf("MTLF subscription request: %s", string(jsonData))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request to MTLF: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			consumerLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return "", fmt.Errorf("MTLF subscription failed: status=%d", resp.StatusCode)
		}
		return "", fmt.Errorf("MTLF subscription failed: status=%d, body=%s", resp.StatusCode, string(body))
	}

	return s.parseSubscriptionId(resp), nil
}

// parseSubscriptionId extracts subscription ID from response Location header
func (s *NmtlfService) parseSubscriptionId(resp *http.Response) string {
	location := resp.Header.Get("Location")
	if location != "" {
		if idx := strings.LastIndex(location, "/"); idx >= 0 {
			return location[idx+1:]
		}
		return location
	}
	return ""
}

// HTTPClient returns the underlying HTTP client for testing
func (s *NmtlfService) HTTPClient() *http.Client {
	return s.httpClient
}
