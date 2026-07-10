package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
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

func (c *Client) ApplySubscriptionRuntime(
	ctx context.Context,
	request anlf.ApplySubscriptionRuntimeRequest,
) (*anlf.ApplySubscriptionRuntimeResponse, error) {
	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal AnLF runtime apply request: %w", err)
	}

	requestURL := c.subscriptionURL(request.Subscription.SubscriptionID, "/runtime")

	ctx, cancel, err := timeoutContextFromParent(ctx, 120*time.Second, "AnLF backend runtime apply")
	if err != nil {
		return nil, err
	}
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, requestURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("create AnLF runtime apply request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send AnLF runtime apply request: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.AnlfLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("AnLF backend runtime apply failed: status=%d", resp.StatusCode)
	}

	var response anlf.ApplySubscriptionRuntimeResponse
	if decodeErr := json.NewDecoder(resp.Body).Decode(&response); decodeErr != nil {
		return nil, fmt.Errorf("decode AnLF runtime apply response: %w", decodeErr)
	}

	return &response, nil
}

func (c *Client) SyncObservationBindings(
	ctx context.Context,
	subscriptionID string,
	request anlf.SyncObservationBindingsRequest,
) error {
	return c.sendJSON(
		ctx,
		http.MethodPut,
		c.subscriptionURL(subscriptionID, "/observation-bindings"),
		request,
		http.StatusNoContent,
		10*time.Second,
		"sync AnLF observation bindings",
	)
}

func (c *Client) SendObservations(
	ctx context.Context,
	sourceID string,
	batch anlf.ObservationBatch,
) error {
	requestURL := c.endpoint + "/observation-sources/" + url.PathEscape(sourceID) + "/observations"
	return c.sendJSON(
		ctx,
		http.MethodPost,
		requestURL,
		batch,
		http.StatusNoContent,
		10*time.Second,
		"send AnLF observations",
	)
}

func (c *Client) ReleaseSubscriptionRuntime(ctx context.Context, subscriptionID string) error {
	ctx, cancel, err := timeoutContextFromParent(ctx, 10*time.Second, "AnLF backend runtime release")
	if err != nil {
		return err
	}
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		c.subscriptionURL(subscriptionID, "/runtime"),
		nil,
	)
	if err != nil {
		return fmt.Errorf("create AnLF runtime release request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send AnLF runtime release request: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.AnlfLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("AnLF backend runtime release failed: status=%d", resp.StatusCode)
	}

	return nil
}

func (c *Client) sendJSON(
	parent context.Context,
	method string,
	requestURL string,
	body any,
	expectedStatus int,
	timeout time.Duration,
	operation string,
) error {
	jsonData, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("%s: marshal request: %w", operation, err)
	}
	ctx, cancel, err := timeoutContextFromParent(parent, timeout, operation)
	if err != nil {
		return err
	}
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, requestURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("%s: create request: %w", operation, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.AnlfLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()
	if resp.StatusCode != expectedStatus {
		return fmt.Errorf("%s: status=%d", operation, resp.StatusCode)
	}
	return nil
}

func (c *Client) subscriptionURL(subscriptionID, suffix string) string {
	return c.endpoint + "/subscriptions/" + url.PathEscape(subscriptionID) + suffix
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
