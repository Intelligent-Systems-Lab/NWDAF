package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
)

const (
	// DaisyPublishTaskPath is the REST API endpoint to trigger FL training on Daisy master
	DaisyPublishTaskPath = "/publish_task"

	// DaisyTIDKey is the task ID key in the task payload (per Daisy convention)
	DaisyTIDKey = "TID"

	// DaisyDefaultTimeout is 30 minutes since POST blocks until training completes
	DaisyDefaultTimeout = 30 * time.Minute
)

// DaisyClient handles Daisy FL framework REST API interactions
// Used to trigger federated learning training tasks on the Daisy master node
type DaisyClient struct {
	endpoint   string
	httpClient *http.Client
}

// NewDaisyClient creates a new Daisy client with long timeout
// The timeout is set to 30 minutes because Daisy's /publish_task POST
// blocks until the entire training process completes before returning 200
func NewDaisyClient(endpoint string) *DaisyClient {
	return &DaisyClient{
		endpoint: endpoint,
		httpClient: &http.Client{
			Timeout: DaisyDefaultTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        10,
				MaxIdleConnsPerHost: 5,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// TriggerTraining sends a training task to the Daisy master
// The POST request blocks until training completes (HTTP 200 = training success)
// If the task payload does not contain a TID, one is automatically generated
func (c *DaisyClient) TriggerTraining(task map[string]any) error {
	// Auto-generate TID if not provided (per request.py convention)
	if _, ok := task[DaisyTIDKey]; !ok {
		task[DaisyTIDKey] = uuid.New().String()
	}

	jsonData, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task payload: %w", err)
	}

	url := c.endpoint + DaisyPublishTaskPath
	consumerLog.Debugf("Daisy training request: %s", string(jsonData))

	// Use DaisyDefaultTimeout for the request context as training may take a long time
	ctx, cancel := context.WithTimeout(context.Background(), DaisyDefaultTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send training request to Daisy: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			consumerLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("daisy training failed: status=%d", resp.StatusCode)
		}
		return fmt.Errorf("daisy training failed: status=%d, body=%s", resp.StatusCode, string(body))
	}

	consumerLog.Infof("Daisy training completed: TID=%v", task[DaisyTIDKey])
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
