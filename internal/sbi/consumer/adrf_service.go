package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

const (
	AdrfDataStoreRecordsPath           = "/nadrf-datamanagement/v1/data-store-records"
	AdrfDataRetrievalSubscriptionsPath = "/nadrf-datamanagement/v1/data-retrieval-subscriptions"
	adrfStorageTimeout                 = 10 * time.Second
	adrfRetrievalTimeout               = 15 * time.Second
	adrfFetchTimeout                   = 10 * time.Second
	adrfUnsubscribeMaxRetry            = 3
	adrfUnsubscribeRetryBackoff        = 500 * time.Millisecond
)

// AdrfTimePeriod represents a time window for data retrieval (TS 29.122 TimeWindow).
type AdrfTimePeriod struct {
	StartTime string `json:"startTime"` // RFC 3339
	StopTime  string `json:"stopTime"`  // RFC 3339
}

// NadrfDataRetrievalSubscription is the request body for RetrievalSubscribe (TS 29.575).
// dataSub is a single object (not array) per the retrieval subscription schema.
type NadrfDataRetrievalSubscription struct {
	NotifCorrId     string               `json:"notifCorrId"`
	NotificationURI string               `json:"notificationURI"`
	TimePeriod      AdrfTimePeriod       `json:"timePeriod"`
	DataSub         AdrfDataSubscription `json:"dataSub"`
	ConsTrigNotif   bool                 `json:"consTrigNotif,omitempty"`
}

// AdrfDataSubscription wraps a single DataSubscription element (TS 29.575).
// Using smfDataSub path: ExtendedNsmfEventExposure maps to NsmfEventExposure (TS 29.508).
type AdrfDataSubscription struct {
	SmfDataSub *ExtendedNsmfEventExposure `json:"smfDataSub,omitempty"`
}

// AdrfDataNotification holds the upfEventNotifs array (TS 29.575 DataNotification).
// Each element is a serialized NotificationData (TS 29.564) passed as raw JSON
// to avoid importing processor types.
type AdrfDataNotification struct {
	UpfEventNotifs []json.RawMessage `json:"upfEventNotifs"`
}

// NadrfDataStoreRecord is the request body for StorageRequest (TS 29.575).
// Uses the dataSub + dataNotif path for raw UPF data.
type NadrfDataStoreRecord struct {
	DataSub   []AdrfDataSubscription `json:"dataSub"`
	DataNotif *AdrfDataNotification  `json:"dataNotif,omitempty"`
}

// AdrfClient is the HTTP client for the ADRF Nadrf_DataManagement service (TS 29.575).
type AdrfClient struct {
	endpoint   string
	httpClient *http.Client
}

// NewAdrfClient creates an AdrfClient for the given ADRF endpoint (e.g. "http://127.0.0.1:9888").
func NewAdrfClient(endpoint string) *AdrfClient {
	return &AdrfClient{
		endpoint: endpoint,
		httpClient: &http.Client{
			Timeout: adrfStorageTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        50,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// StorageRequest stores a batch of UPF notification records to ADRF.
// All upfNotifJSONs must belong to the same SMF subscription (same info).
// Returns the storeTransId extracted from the Location header on success.
func (c *AdrfClient) StorageRequest(
	info *nwdaf_context.AdrfSmfInfo,
	upfNotifJSONs []json.RawMessage,
) (string, error) {
	smfDataSub := &ExtendedNsmfEventExposure{
		Supi:        info.Supi,
		NotifId:     info.NotifId,
		NotifUri:    info.NotifUri,
		NotifMethod: info.NotifMethod,
		RepPeriod:   info.RepPeriod,
		EventSubs:   BuildUpfEventSubs(info.UpfNotifUri, true, true),
	}

	record := NadrfDataStoreRecord{
		DataSub: []AdrfDataSubscription{
			{SmfDataSub: smfDataSub},
		},
		DataNotif: &AdrfDataNotification{
			UpfEventNotifs: upfNotifJSONs,
		},
	}

	body, err := json.Marshal(record)
	if err != nil {
		return "", fmt.Errorf("marshal NadrfDataStoreRecord: %w", err)
	}

	url := c.endpoint + AdrfDataStoreRecordsPath
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("POST %s: %w", url, err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			consumerLog.Warnf("Failed to close ADRF response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("ADRF StorageRequest returned %d", resp.StatusCode)
	}

	location := resp.Header.Get("Location")
	storeTransId := path.Base(location)
	return storeTransId, nil
}

// RetrievalSubscribe creates an ADRF retrieval subscription for a specific SUPI.
// Returns subscriptionId from Location header on success (201).
// notifCorrId should be the TID of the retrain job so callbacks can be routed.
func (c *AdrfClient) RetrievalSubscribe(
	info *nwdaf_context.AdrfSmfInfo,
	notifCorrId string,
	notifURI string,
	timePeriod AdrfTimePeriod,
) (string, error) {
	smfDataSub := &ExtendedNsmfEventExposure{
		Supi:        info.Supi,
		NotifId:     info.NotifId,
		NotifUri:    info.NotifUri,
		NotifMethod: info.NotifMethod,
		RepPeriod:   info.RepPeriod,
		EventSubs:   BuildUpfEventSubs(info.UpfNotifUri, true, true),
	}

	sub := NadrfDataRetrievalSubscription{
		NotifCorrId:     notifCorrId,
		NotificationURI: notifURI,
		TimePeriod:      timePeriod,
		DataSub:         AdrfDataSubscription{SmfDataSub: smfDataSub},
		ConsTrigNotif:   true,
	}

	body, err := json.Marshal(sub)
	if err != nil {
		return "", fmt.Errorf("marshal NadrfDataRetrievalSubscription: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), adrfRetrievalTimeout)
	defer cancel()

	url := c.endpoint + AdrfDataRetrievalSubscriptionsPath
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build RetrievalSubscribe request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("POST %s: %w", url, err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			consumerLog.Warnf("Failed to close ADRF RetrievalSubscribe response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusCreated {
		bodyBytes, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return "", fmt.Errorf("ADRF RetrievalSubscribe returned %d", resp.StatusCode)
		}
		return "", fmt.Errorf("ADRF RetrievalSubscribe returned %d: %s", resp.StatusCode, string(bodyBytes))
	}

	location := resp.Header.Get("Location")
	subscriptionId := path.Base(location)
	return subscriptionId, nil
}

// RetrievalRequest fetches data store records by fetch-correlation-ids.
// Returns nil record (no error) when ADRF responds 204 (no matching data).
func (c *AdrfClient) RetrievalRequest(fetchCorrIds []string) (*NadrfDataStoreRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), adrfFetchTimeout)
	defer cancel()

	url := c.endpoint + AdrfDataStoreRecordsPath + "?fetch-correlation-ids=" + strings.Join(fetchCorrIds, ",")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build RetrievalRequest: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			consumerLog.Warnf("Failed to close ADRF RetrievalRequest response body: %v", closeErr)
		}
	}()

	switch resp.StatusCode {
	case http.StatusOK:
		var record NadrfDataStoreRecord
		if decodeErr := json.NewDecoder(resp.Body).Decode(&record); decodeErr != nil {
			return nil, fmt.Errorf("decode NadrfDataStoreRecord: %w", decodeErr)
		}
		return &record, nil
	case http.StatusNoContent:
		return nil, nil
	default:
		bodyBytes, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("ADRF RetrievalRequest %d", resp.StatusCode)
		}
		return nil, fmt.Errorf("ADRF RetrievalRequest %d: %s", resp.StatusCode, string(bodyBytes))
	}
}

// RetrievalUnsubscribe deletes an ADRF retrieval subscription.
// Treats 404 as success (subscription already cleaned up).
// Retries up to adrfUnsubscribeMaxRetry times on 5xx/timeout.
func (c *AdrfClient) RetrievalUnsubscribe(subscriptionId string) error {
	url := c.endpoint + AdrfDataRetrievalSubscriptionsPath + "/" + subscriptionId

	var lastErr error
	for attempt := 0; attempt < adrfUnsubscribeMaxRetry; attempt++ {
		if attempt > 0 {
			time.Sleep(adrfUnsubscribeRetryBackoff)
		}

		ctx, cancel := context.WithTimeout(context.Background(), adrfFetchTimeout)
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
		if err != nil {
			cancel()
			return fmt.Errorf("build RetrievalUnsubscribe request: %w", err)
		}

		resp, doErr := c.httpClient.Do(req)
		cancel()
		if doErr != nil {
			lastErr = fmt.Errorf("DELETE %s: %w", url, doErr)
			continue
		}
		if closeErr := resp.Body.Close(); closeErr != nil {
			consumerLog.Warnf("Failed to close ADRF RetrievalUnsubscribe response body: %v", closeErr)
		}

		switch resp.StatusCode {
		case http.StatusNoContent, http.StatusNotFound:
			return nil
		default:
			lastErr = fmt.Errorf("ADRF RetrievalUnsubscribe returned %d", resp.StatusCode)
			if resp.StatusCode < 500 {
				return lastErr
			}
		}
	}
	return lastErr
}
