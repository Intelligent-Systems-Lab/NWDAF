package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

const (
	AdrfDataStoreRecordsPath = "/nadrf-datamanagement/v1/data-store-records"
	adrfStorageTimeout       = 10 * time.Second
)

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
