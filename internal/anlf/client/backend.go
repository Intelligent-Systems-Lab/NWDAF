package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/free5gc/nwdaf/internal/anlf"
	"github.com/free5gc/nwdaf/internal/logger"
)

// Client handles local AnLF backend API interactions.
type Client struct {
	endpoint   string
	httpClient *http.Client
}

// NewClient creates a new AnLF backend client.
func NewClient(endpoint string) *Client {
	return &Client{
		endpoint: endpoint,
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

// LoadModel loads a model from the given URL and returns the model ID.
func (c *Client) LoadModel(ctx context.Context, modelURL string) (string, error) {
	request := anlf.LoadModelRequest{
		ModelUrl: modelURL,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	url := c.endpoint + "/model/load"

	ctx, cancel, err := timeoutContextFromParent(ctx, 120*time.Second, "AnLF backend model load")
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
		return "", fmt.Errorf("failed to send request to AnLF backend: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.AnlfLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("AnLF backend model load failed: status=%d", resp.StatusCode)
	}

	var response anlf.LoadModelResponse
	if decodeErr := json.NewDecoder(resp.Body).Decode(&response); decodeErr != nil {
		return "", fmt.Errorf("failed to decode response: %w", decodeErr)
	}

	return response.ModelId, nil
}

// UnloadModel unloads a model by ID.
func (c *Client) UnloadModel(ctx context.Context, modelID string) error {
	request := anlf.UnloadModelRequest{
		ModelId: modelID,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	url := c.endpoint + "/model/unload"

	ctx, cancel, err := timeoutContextFromParent(ctx, 10*time.Second, "AnLF backend model unload")
	if err != nil {
		return err
	}
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request to AnLF backend: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.AnlfLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("AnLF backend model unload failed: status=%d", resp.StatusCode)
	}

	return nil
}

// Predict calls the AnLF backend to get traffic predictions.
func (c *Client) Predict(
	ctx context.Context,
	modelID string,
	trafficData []anlf.TrafficObservation,
) (*anlf.PredictResponse, error) {
	logger.AnlfLog.Debugf("Calling AnLF backend: modelId=%s, dataPoints=%d",
		modelID, len(trafficData))

	request := anlf.PredictRequest{
		ModelId:        modelID,
		HistoricalData: trafficData,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	url := c.endpoint + "/predict"

	ctx, cancel, err := timeoutContextFromParent(ctx, 10*time.Second, "AnLF backend prediction")
	if err != nil {
		return nil, err
	}
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to AnLF backend: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.AnlfLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("AnLF backend prediction failed: status=%d", resp.StatusCode)
	}

	var response anlf.PredictResponse
	if decodeErr := json.NewDecoder(resp.Body).Decode(&response); decodeErr != nil {
		return nil, fmt.Errorf("failed to decode response: %w", decodeErr)
	}

	logger.AnlfLog.Debugf("AnLF backend returned %d predictions", len(response.PredictedData))
	return &response, nil
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
