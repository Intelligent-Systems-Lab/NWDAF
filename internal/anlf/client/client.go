// Package client implements the outbound AnLF Backend HTTP transport.
package client

import (
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
	var payload backend.HealthResponse
	if json.Unmarshal(body, &payload) != nil ||
		uuid.Validate(payload.ProcessInstanceID) != nil {
		return backend.HealthResponse{}, &BackendRequestError{
			Operation:  "check AnLF backend readiness",
			StatusCode: response.StatusCode,
			Detail:     "malformed readiness response",
		}
	}
	if response.StatusCode != http.StatusOK {
		return payload, &BackendRequestError{
			Operation:  "check AnLF backend readiness",
			StatusCode: response.StatusCode,
			Detail:     strings.TrimSpace(string(body)),
		}
	}
	if payload.Status != "ready" {
		return backend.HealthResponse{}, &BackendRequestError{
			Operation:  "check AnLF backend readiness",
			StatusCode: response.StatusCode,
			Detail:     "malformed readiness response",
		}
	}
	return payload, nil
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
