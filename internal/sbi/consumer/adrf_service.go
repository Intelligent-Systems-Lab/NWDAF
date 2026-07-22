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
	"github.com/free5gc/openapi/models"
)

const maxAdrfStandardBodyBytes = 4 * 1024 * 1024

type StandardAdrfResponse struct {
	StatusCode  int
	Location    string
	ContentType string
	Body        []byte
}

type StandardAdrfError struct {
	StatusCode     int
	ProblemDetails models.ProblemDetails
}

func (e *StandardAdrfError) Error() string {
	return fmt.Sprintf("ADRF Data Management request failed: status=%d", e.StatusCode)
}

func (e *StandardAdrfError) HTTPStatusCode() int {
	return e.StatusCode
}

func (e *StandardAdrfError) StandardProblemDetails() *models.ProblemDetails {
	problem := e.ProblemDetails
	if problem.Status == 0 {
		problem.Status = int32(e.StatusCode)
	}
	return &problem
}

const (
	AdrfDataStoreRecordsPath           = "/nadrf-datamanagement/v1/data-store-records"
	AdrfDataRetrievalSubscriptionsPath = "/nadrf-datamanagement/v1/data-retrieval-subscriptions"
	adrfStorageTimeout                 = 10 * time.Second
	adrfRetrievalTimeout               = 15 * time.Second
	adrfFetchTimeout                   = 10 * time.Second
	adrfUnsubscribeTimeout             = 10 * time.Second
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
	SmfEventNotifs []json.RawMessage `json:"smfEventNotifs,omitempty"`
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
	ctx context.Context,
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
	ctx, cancel, err := timeoutContextFromParent(ctx, adrfStorageTimeout, "ADRF storage request")
	if err != nil {
		return "", err
	}
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
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
			consumerLog.Debugf("failed to close ADRF response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("ADRF StorageRequest returned %d", resp.StatusCode)
	}

	location := resp.Header.Get("Location")
	storeTransId := path.Base(location)
	return storeTransId, nil
}

func (c *AdrfClient) ExecuteStandardStorageRequest(
	ctx context.Context,
	body []byte,
) (*StandardAdrfResponse, error) {
	requestCtx, cancel, err := timeoutContextFromParent(ctx, adrfStorageTimeout, "ADRF storage request")
	if err != nil {
		return nil, err
	}
	defer cancel()
	requestURL := strings.TrimRight(c.endpoint, "/") + AdrfDataStoreRecordsPath
	request, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodPost,
		requestURL,
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("build ADRF storage request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("send ADRF storage request: %w", err)
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxAdrfStandardBodyBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read ADRF storage response: %w", readErr)
	}
	if len(responseBody) > maxAdrfStandardBodyBytes {
		return nil, fmt.Errorf("ADRF storage response exceeds transport limit")
	}
	if closeErr != nil {
		consumerLog.Debugf("failed to close ADRF storage response: %v", closeErr)
	}
	standardResponse := &StandardAdrfResponse{
		StatusCode:  response.StatusCode,
		Location:    response.Header.Get("Location"),
		ContentType: response.Header.Get("Content-Type"),
		Body:        responseBody,
	}
	if response.StatusCode != http.StatusCreated {
		problem := models.ProblemDetails{
			Status: int32(response.StatusCode),
			Title:  http.StatusText(response.StatusCode),
		}
		if unmarshalErr := json.Unmarshal(responseBody, &problem); unmarshalErr != nil {
			problem.Detail = strings.TrimSpace(string(responseBody))
		}
		return standardResponse, &StandardAdrfError{
			StatusCode:     response.StatusCode,
			ProblemDetails: problem,
		}
	}
	if standardResponse.Location == "" {
		return nil, fmt.Errorf("malformed ADRF storage response: Location is required")
	}
	if !isJSONMediaType(standardResponse.ContentType) {
		return nil, fmt.Errorf("malformed ADRF storage response: Content-Type must be application/json")
	}
	if len(standardResponse.Body) == 0 {
		return nil, fmt.Errorf("malformed ADRF storage response: representation is required")
	}
	var record NadrfDataStoreRecord
	if decodeErr := json.Unmarshal(standardResponse.Body, &record); decodeErr != nil {
		return nil, fmt.Errorf("decode ADRF storage representation: %w", decodeErr)
	}
	if len(record.DataSub) == 0 || record.DataNotif == nil ||
		(len(record.DataNotif.UpfEventNotifs) == 0 && len(record.DataNotif.SmfEventNotifs) == 0) {
		return nil, fmt.Errorf("malformed ADRF storage response: dataSub and a supported dataNotif array are required")
	}
	return standardResponse, nil
}

func isJSONMediaType(value string) bool {
	mediaType := strings.ToLower(strings.TrimSpace(strings.SplitN(value, ";", 2)[0]))
	return mediaType == "application/json"
}

// RetrievalSubscribe creates an ADRF retrieval subscription for a specific SUPI.
// Returns subscriptionId from Location header on success (201).
// notifCorrId should be the TID of the retrain job so callbacks can be routed.
func (c *AdrfClient) RetrievalSubscribe(
	ctx context.Context,
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

	ctx, cancel, err := timeoutContextFromParent(ctx, adrfRetrievalTimeout, "ADRF retrieval subscribe")
	if err != nil {
		return "", err
	}
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
			consumerLog.Debugf("failed to close ADRF RetrievalSubscribe response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("ADRF RetrievalSubscribe returned %d", resp.StatusCode)
	}

	location := resp.Header.Get("Location")
	subscriptionId := path.Base(location)
	return subscriptionId, nil
}

// RetrievalRequest fetches data store records by fetch-correlation-ids.
// Returns nil record (no error) when ADRF responds 204 (no matching data).
func (c *AdrfClient) RetrievalRequest(ctx context.Context, fetchCorrIds []string) (*NadrfDataStoreRecord, error) {
	ctx, cancel, err := timeoutContextFromParent(ctx, adrfFetchTimeout, "ADRF retrieval request")
	if err != nil {
		return nil, err
	}
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
			consumerLog.Debugf("failed to close ADRF RetrievalRequest response body: %v", closeErr)
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
		return nil, fmt.Errorf("ADRF RetrievalRequest %d", resp.StatusCode)
	}
}

// RetrievalUnsubscribe deletes an ADRF retrieval subscription.
// Treats 404 as success (subscription already cleaned up).
// Retries up to adrfUnsubscribeMaxRetry times on 5xx/timeout.
func (c *AdrfClient) RetrievalUnsubscribe(ctx context.Context, subscriptionId string) error {
	url := c.endpoint + AdrfDataRetrievalSubscriptionsPath + "/" + subscriptionId

	var lastErr error
	for attempt := 0; attempt < adrfUnsubscribeMaxRetry; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(adrfUnsubscribeRetryBackoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		attemptCtx, cancel, err := timeoutContextFromParent(
			ctx,
			adrfUnsubscribeTimeout,
			"ADRF retrieval unsubscribe",
		)
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(attemptCtx, http.MethodDelete, url, nil)
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
			consumerLog.Debugf("failed to close ADRF RetrievalUnsubscribe response body: %v", closeErr)
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

func (c *AdrfClient) HTTPClient() *http.Client {
	return c.httpClient
}
