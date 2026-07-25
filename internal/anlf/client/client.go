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
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/free5gc/nwdaf/internal/backend"
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

func (c *Client) CheckReadiness(parent context.Context) (backend.HealthResponse, error) {
	ctx, cancel, err := timeoutContextFromParent(parent, c.timeout, "check AnLF backend readiness")
	if err != nil {
		return backend.HealthResponse{}, err
	}
	defer cancel()

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		c.endpoint+"/health/ready",
		nil,
	)
	if err != nil {
		return backend.HealthResponse{}, fmt.Errorf("create AnLF backend readiness request: %w", err)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return backend.HealthResponse{}, fmt.Errorf("check AnLF backend readiness: %w", ctx.Err())
		}
		return backend.HealthResponse{}, fmt.Errorf("check AnLF backend readiness: %w", err)
	}
	body, readErr := readHealthBody(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return backend.HealthResponse{}, &BackendRequestError{
			Operation:  "check AnLF backend readiness",
			StatusCode: response.StatusCode,
			Detail:     readErr.Error(),
		}
	}
	if closeErr != nil {
		return backend.HealthResponse{}, fmt.Errorf("close AnLF backend readiness response: %w", closeErr)
	}
	if response.StatusCode != http.StatusOK {
		return backend.HealthResponse{}, &BackendRequestError{
			Operation:  "check AnLF backend readiness",
			StatusCode: response.StatusCode,
			Detail:     strings.TrimSpace(string(body)),
		}
	}
	var payload backend.HealthResponse
	if json.Unmarshal(body, &payload) != nil || payload.Status != "ready" ||
		uuid.Validate(payload.ProcessInstanceID) != nil {
		return backend.HealthResponse{}, &BackendRequestError{
			Operation:  "check AnLF backend readiness",
			StatusCode: response.StatusCode,
			Detail:     "malformed readiness response",
		}
	}
	return payload, nil
}

func (c *Client) Sync(parent context.Context, snapshot backend.SyncRequest) (*backend.SyncResponse, error) {
	body, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("sync AnLF backend: marshal request: %w", err)
	}
	ctx, cancel, err := timeoutContextFromParent(parent, c.timeout, "sync AnLF backend")
	if err != nil {
		return nil, err
	}
	defer cancel()
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		c.endpoint+"/internal/v1/sync",
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("create AnLF backend sync request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("sync AnLF backend: %w", err)
	}
	responseBody, readErr := readHealthBody(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return nil, &BackendRequestError{
			Operation: "sync AnLF backend", StatusCode: response.StatusCode, Detail: readErr.Error(),
		}
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close AnLF backend sync response: %w", closeErr)
	}
	if response.StatusCode != http.StatusOK {
		return nil, &BackendRequestError{
			Operation:  "sync AnLF backend",
			StatusCode: response.StatusCode,
			Detail:     strings.TrimSpace(string(responseBody)),
		}
	}
	var payload backend.SyncResponse
	if json.Unmarshal(responseBody, &payload) != nil ||
		uuid.Validate(payload.ProcessInstanceID) != nil || !payload.SnapshotAccepted {
		return nil, &BackendRequestError{
			Operation:  "sync AnLF backend",
			StatusCode: response.StatusCode,
			Detail:     "malformed or rejected sync response",
		}
	}
	return &payload, nil
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
