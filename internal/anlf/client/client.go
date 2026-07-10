// Package client implements the outbound AnLF Backend HTTP transport.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/free5gc/nwdaf/internal/logger"
)

type Client struct {
	endpoint   string
	httpClient *http.Client
}

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

func (c *Client) GetEndpoint() string {
	return c.endpoint
}

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
