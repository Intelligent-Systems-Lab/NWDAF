// Package collector provides SMF client for Nsmf_EventExposure service
package collector

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

const (
	// Nsmf_EventExposure API path
	SmfEventExposurePath = "/nsmf-event-exposure/v1/subscriptions"
)

// SubscribeToSmf creates an event exposure subscription to SMF
func (c *CollectorContext) SubscribeToSmf(
	smfEndpoint string,
	supi string,
	events []models.SmfEvent,
	notifUri string,
) (string, error) {
	logger.CollectorLog.Infof("Subscribing to SMF: endpoint=%s, supi=%s", smfEndpoint, supi)

	notifId := uuid.New().String()

	// Build subscription request
	request := models.NsmfEventExposure{
		Supi:      supi,
		NotifUri:  notifUri,
		NotifId:   notifId,
		EventSubs: make([]models.SmfEventExposureEventSubscription, 0, len(events)),
	}

	for _, event := range events {
		request.EventSubs = append(request.EventSubs, models.SmfEventExposureEventSubscription{
			Event: event,
		})
	}

	// Send POST request to SMF
	url := smfEndpoint + SmfEventExposurePath
	jsonData, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	logger.CollectorLog.Debugf("SMF subscription request: %s", string(jsonData))

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to send request to SMF: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("SMF subscription failed: status=%d, body=%s", resp.StatusCode, string(body))
	}

	// Parse response to get subscriptionId from Location header
	subscriptionId := resp.Header.Get("Location")
	if subscriptionId == "" {
		// Try to parse from response body
		var response models.NsmfEventExposure
		if err := json.NewDecoder(resp.Body).Decode(&response); err == nil {
			subscriptionId = response.SubId
		}
	}

	if subscriptionId == "" {
		subscriptionId = notifId // Fallback to notifId
	}

	// Store subscription
	sub := &SmfSubscription{
		SubscriptionId: subscriptionId,
		SmfEndpoint:    smfEndpoint,
		TargetSupi:     supi,
		NotifId:        notifId,
		Events:         events,
		CreatedAt:      time.Now(),
	}
	c.StoreSubscription(sub)

	logger.CollectorLog.Infof("SMF subscription created: id=%s", subscriptionId)
	return subscriptionId, nil
}

// UnsubscribeFromSmf deletes an event exposure subscription from SMF
func (c *CollectorContext) UnsubscribeFromSmf(
	smfEndpoint string,
	subscriptionId string,
) error {
	logger.CollectorLog.Infof("Unsubscribing from SMF: endpoint=%s, subId=%s", smfEndpoint, subscriptionId)

	// Send DELETE request to SMF
	url := smfEndpoint + SmfEventExposurePath + "/" + subscriptionId

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request to SMF: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("SMF unsubscription failed: status=%d, body=%s", resp.StatusCode, string(body))
	}

	// Remove from local storage
	c.DeleteSubscription(subscriptionId)

	logger.CollectorLog.Infof("SMF subscription deleted: id=%s", subscriptionId)
	return nil
}

// SubscribeForUeCommunication subscribes to SMF for UE communication data
// Based on 3GPP TS 23.288 and TS 29.508 analysis:
// - PDU_SES_EST: For DNN, S-NSSAI, session lifecycle start
// - PDU_SES_REL: For session lifecycle end, duration calculation
// - UP_STATUS_INFO: For commDur calculation (ACTIVATED/DEACTIVATED transitions)
func (c *CollectorContext) SubscribeForUeCommunication(
	smfEndpoint string,
	supi string,
	notifUri string,
) (string, error) {
	// Subscribe to core events for UE Communication analytics
	events := []models.SmfEvent{
		models.SmfEvent_PDU_SES_EST,    // Session metadata (DNN, S-NSSAI)
		models.SmfEvent_PDU_SES_REL,    // Session lifecycle end
		models.SmfEvent_UP_STATUS_INFO, // User Plane status (ACTIVATED/DEACTIVATED)
	}

	return c.SubscribeToSmf(smfEndpoint, supi, events, notifUri)
}
