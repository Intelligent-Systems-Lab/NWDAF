package mtlf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// DaisyAPI defines the local Daisy integration seam owned by MTLF.
type DaisyAPI interface {
	TriggerTrainingAsync(ctx context.Context, task map[string]any, callbackURL string, tidOverride string) (string, error)
	UploadData(ctx context.Context, tid string, groupID string, upfEventNotifs []json.RawMessage) error
	HTTPClient() *http.Client
}

const (
	// DaisyPublishTaskPath is the REST API endpoint to trigger FL training on Daisy master
	DaisyPublishTaskPath = "/publish_task"

	// DaisyUploadDataPath is the REST API endpoint to upload historical training data
	DaisyUploadDataPath = "/upload_data"

	// DaisyTIDKey is the task ID key in the task payload (per Daisy convention)
	DaisyTIDKey = "TID"

	// DaisyCallbackURLKey is the key for the callback URL in the task payload
	DaisyCallbackURLKey = "CALLBACK_URL"

	// DaisyAsyncTimeout is the timeout for the initial POST to Daisy.
	// Daisy should respond 202 immediately; no need for a long timeout.
	DaisyAsyncTimeout = 10 * time.Second

	// DaisyUploadTimeout is the timeout for uploading a data batch to Daisy.
	DaisyUploadTimeout = 10 * time.Second
)

// DaisyUploadDataRequest is the payload for POST /upload_data.
type DaisyUploadDataRequest struct {
	TID            string            `json:"TID"`
	GroupId        string            `json:"group_id"`
	UpfEventNotifs []json.RawMessage `json:"upfEventNotifs"`
}

// DaisyClient handles Daisy FL framework REST API interactions.
type DaisyClient struct {
	endpoint   string
	httpClient *http.Client
}

// NewDaisyClient creates a new Daisy client.
func NewDaisyClient(endpoint string) *DaisyClient {
	return &DaisyClient{
		endpoint: endpoint,
		httpClient: &http.Client{
			Timeout: DaisyAsyncTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        10,
				MaxIdleConnsPerHost: 5,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// TriggerTrainingAsync sends a training task to Daisy in async mode.
// Daisy should respond 202 Accepted immediately and call back NWDAF at callbackURL
// when training completes. Returns the task ID (TID) on success.
// tidOverride: if non-empty, uses this TID (ADRF path, must match the TID used for UploadData).
//
//	If empty, generates a fresh UUID.
func (c *DaisyClient) TriggerTrainingAsync(
	ctx context.Context,
	task map[string]any, callbackURL string, tidOverride string,
) (string, error) {
	var tidStr string
	if tidOverride != "" {
		tidStr = tidOverride
	} else {
		tidStr = uuid.New().String()
	}
	task[DaisyTIDKey] = tidStr

	if callbackURL != "" {
		task[DaisyCallbackURLKey] = callbackURL
	}

	jsonData, err := json.Marshal(task)
	if err != nil {
		return "", fmt.Errorf("failed to marshal task payload: %w", err)
	}

	url := c.endpoint + DaisyPublishTaskPath

	ctx, cancel, err := timeoutContextFromParent(ctx, DaisyAsyncTimeout, "Daisy async training")
	if err != nil {
		return "", err
	}
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send async training request to Daisy: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			mtlfLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusAccepted {
		return "", fmt.Errorf("daisy rejected async request: status=%d", resp.StatusCode)
	}

	return tidStr, nil
}

// UploadData sends a batch of historical UPF records to Daisy for the retrain dataset.
// Each call corresponds to one NadrfDataStoreRecord fetched from ADRF.
func (c *DaisyClient) UploadData(
	ctx context.Context,
	tid string,
	groupID string,
	upfEventNotifs []json.RawMessage,
) error {
	payload := DaisyUploadDataRequest{
		TID:            tid,
		GroupId:        groupID,
		UpfEventNotifs: upfEventNotifs,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal DaisyUploadDataRequest: %w", err)
	}

	ctx, cancel, err := timeoutContextFromParent(ctx, DaisyUploadTimeout, "Daisy data upload")
	if err != nil {
		return err
	}
	defer cancel()

	url := c.endpoint + DaisyUploadDataPath
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("build UploadData request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", url, err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			mtlfLog.Debugf("failed to close UploadData response body: %v", closeErr)
		}
	}()

	statusOK := resp.StatusCode == http.StatusOK ||
		resp.StatusCode == http.StatusCreated ||
		resp.StatusCode == http.StatusNoContent
	if !statusOK {
		return fmt.Errorf("daisy UploadData returned %d", resp.StatusCode)
	}

	return nil
}

// GetEndpoint returns the configured endpoint
func (c *DaisyClient) GetEndpoint() string {
	return c.endpoint
}

// HTTPClient returns the underlying HTTP client for testing
func (c *DaisyClient) HTTPClient() *http.Client {
	return c.httpClient
}

func timeoutContextFromParent(
	parent context.Context,
	timeout time.Duration,
	operation string,
) (context.Context, context.CancelFunc, error) {
	if parent == nil {
		return nil, nil, fmt.Errorf("%s requires parent context", operation)
	}

	ctx, cancel := context.WithTimeout(parent, timeout)
	return ctx, cancel, nil
}
