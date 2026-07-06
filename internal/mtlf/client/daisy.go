package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/mtlf"
)

const (
	// AsyncTimeout is the timeout for the initial POST to Daisy.
	AsyncTimeout = 10 * time.Second

	// UploadTimeout is the timeout for uploading a data batch to Daisy.
	UploadTimeout = 10 * time.Second
)

// Client handles Daisy FL framework REST API interactions.
type Client struct {
	endpoint   string
	httpClient *http.Client
}

// NewClient creates a new Daisy client.
func NewClient(endpoint string) *Client {
	return &Client{
		endpoint: endpoint,
		httpClient: &http.Client{
			Timeout: AsyncTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        10,
				MaxIdleConnsPerHost: 5,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// TriggerTrainingAsync sends a training task to Daisy in async mode.
func (c *Client) TriggerTrainingAsync(
	ctx context.Context,
	task map[string]any, callbackURL string, tidOverride string,
) (string, error) {
	var tidStr string
	if tidOverride != "" {
		tidStr = tidOverride
	} else {
		tidStr = uuid.New().String()
	}
	task[mtlf.DaisyTIDKey] = tidStr

	if callbackURL != "" {
		task[mtlf.DaisyCallbackURLKey] = callbackURL
	}

	jsonData, err := json.Marshal(task)
	if err != nil {
		return "", fmt.Errorf("failed to marshal task payload: %w", err)
	}

	url := c.endpoint + mtlf.DaisyPublishTaskPath

	ctx, cancel, err := timeoutContextFromParent(ctx, AsyncTimeout, "Daisy async training")
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
			logger.MtlfLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusAccepted {
		return "", fmt.Errorf("daisy rejected async request: status=%d", resp.StatusCode)
	}

	return tidStr, nil
}

// UploadData sends a batch of historical UPF records to Daisy for the retrain dataset.
func (c *Client) UploadData(
	ctx context.Context,
	tid string,
	groupID string,
	upfEventNotifs []json.RawMessage,
) error {
	payload := mtlf.DaisyUploadDataRequest{
		TID:            tid,
		GroupId:        groupID,
		UpfEventNotifs: upfEventNotifs,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal DaisyUploadDataRequest: %w", err)
	}

	ctx, cancel, err := timeoutContextFromParent(ctx, UploadTimeout, "Daisy data upload")
	if err != nil {
		return err
	}
	defer cancel()

	url := c.endpoint + mtlf.DaisyUploadDataPath
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
			logger.MtlfLog.Debugf("failed to close UploadData response body: %v", closeErr)
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

// GetEndpoint returns the configured endpoint.
func (c *Client) GetEndpoint() string {
	return c.endpoint
}

// HTTPClient returns the underlying HTTP client for testing.
func (c *Client) HTTPClient() *http.Client {
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
