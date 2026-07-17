// Package client implements the outbound AnLF Backend HTTP transport.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/free5gc/nwdaf/internal/logger"
)

type Client struct {
	endpoint   string
	timeout    time.Duration
	httpClient *http.Client
}

const maxBackendHealthBodyBytes = 64 * 1024

type BackendRequestError struct {
	Operation  string
	StatusCode int
	Detail     string
}

func (e *BackendRequestError) Error() string {
	if e.StatusCode == 0 {
		return fmt.Sprintf("%s: %s", e.Operation, e.Detail)
	}
	return fmt.Sprintf("%s: status=%d detail=%s", e.Operation, e.StatusCode, e.Detail)
}

func (e *BackendRequestError) HTTPStatusCode() int {
	return e.StatusCode
}

func NewClient(endpoint string, requestTimeout ...time.Duration) *Client {
	timeout := 5 * time.Second
	if len(requestTimeout) > 0 && requestTimeout[0] > 0 {
		timeout = requestTimeout[0]
	}
	return &Client{
		endpoint: strings.TrimSuffix(strings.TrimSpace(endpoint), "/"),
		timeout:  timeout,
		httpClient: &http.Client{
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
		return &BackendRequestError{
			Operation:  operation,
			StatusCode: resp.StatusCode,
			Detail:     http.StatusText(resp.StatusCode),
		}
	}
	return nil
}

func (c *Client) CheckReadiness(parent context.Context) error {
	ctx, cancel, err := timeoutContextFromParent(parent, c.timeout, "check AnLF backend readiness")
	if err != nil {
		return err
	}
	defer cancel()

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		c.endpoint+"/health/ready",
		nil,
	)
	if err != nil {
		return fmt.Errorf("create AnLF backend readiness request: %w", err)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("check AnLF backend readiness: %w", ctx.Err())
		}
		return fmt.Errorf("check AnLF backend readiness: %w", err)
	}
	body, readErr := readHealthBody(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return &BackendRequestError{
			Operation:  "check AnLF backend readiness",
			StatusCode: response.StatusCode,
			Detail:     readErr.Error(),
		}
	}
	if closeErr != nil {
		return fmt.Errorf("close AnLF backend readiness response: %w", closeErr)
	}
	if response.StatusCode != http.StatusOK {
		return &BackendRequestError{
			Operation:  "check AnLF backend readiness",
			StatusCode: response.StatusCode,
			Detail:     strings.TrimSpace(string(body)),
		}
	}
	var payload struct {
		Status string `json:"status"`
	}
	if json.Unmarshal(body, &payload) != nil || payload.Status != "ready" {
		return &BackendRequestError{
			Operation:  "check AnLF backend readiness",
			StatusCode: response.StatusCode,
			Detail:     "malformed readiness response",
		}
	}
	return nil
}

func readHealthBody(reader io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, maxBackendHealthBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxBackendHealthBodyBytes {
		return nil, errors.New("response body exceeds transport limit")
	}
	return body, nil
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
