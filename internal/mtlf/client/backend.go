package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const maxBackendReadinessBodyBytes = 64 * 1024

type BackendRequestError struct {
	StatusCode int
	Code       string
	Message    string
	cause      error
}

func (e *BackendRequestError) Error() string {
	if e.StatusCode == 0 {
		return fmt.Sprintf("MTLF backend readiness request failed: %s", e.Message)
	}
	return fmt.Sprintf(
		"MTLF backend readiness request failed: status=%d code=%s message=%s",
		e.StatusCode,
		e.Code,
		e.Message,
	)
}

func (e *BackendRequestError) Unwrap() error {
	return e.cause
}

type BackendClient struct {
	endpoint   string
	timeout    time.Duration
	httpClient *http.Client
}

func NewBackendClient(
	endpoint string,
	timeout time.Duration,
	httpClient *http.Client,
) (*BackendClient, error) {
	if timeout <= 0 {
		return nil, errors.New("MTLF backend timeout must be positive")
	}
	parsed, err := url.ParseRequestURI(strings.TrimSpace(endpoint))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" {
		return nil, errors.New("MTLF backend endpoint must be an absolute HTTP(S) URL")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Path != "" && parsed.Path != "/") {
		return nil, errors.New("MTLF backend endpoint must be an HTTP(S) origin")
	}
	if port := parsed.Port(); port != "" {
		portNumber, portErr := strconv.Atoi(port)
		if portErr != nil || portNumber < 1 || portNumber > 65535 {
			return nil, errors.New("MTLF backend endpoint port must be between 1 and 65535")
		}
	}
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &BackendClient{
		endpoint:   strings.ToLower(parsed.Scheme) + "://" + parsed.Host,
		timeout:    timeout,
		httpClient: httpClient,
	}, nil
}

func (c *BackendClient) CheckReadiness(parent context.Context) error {
	if parent == nil {
		return &BackendRequestError{Message: "parent context is required"}
	}
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		c.endpoint+"/health/ready",
		nil,
	)
	if err != nil {
		return fmt.Errorf("create MTLF backend readiness request: %w", err)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return &BackendRequestError{Message: ctx.Err().Error(), cause: ctx.Err()}
		}
		return &BackendRequestError{Message: err.Error(), cause: err}
	}
	body, readErr := readBackendReadinessBody(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return &BackendRequestError{
			StatusCode: response.StatusCode,
			Code:       "RESPONSE_TOO_LARGE",
			Message:    readErr.Error(),
		}
	}
	if closeErr != nil {
		return &BackendRequestError{StatusCode: response.StatusCode, Message: closeErr.Error()}
	}
	if response.StatusCode == http.StatusOK {
		return nil
	}

	var payload struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &payload) != nil || payload.Code == "" {
		payload.Code = "HTTP_ERROR"
		payload.Message = strings.TrimSpace(string(body))
	}
	return &BackendRequestError{
		StatusCode: response.StatusCode,
		Code:       payload.Code,
		Message:    payload.Message,
	}
}

func readBackendReadinessBody(reader io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, maxBackendReadinessBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxBackendReadinessBodyBytes {
		return nil, errors.New("response body exceeds transport limit")
	}
	return body, nil
}
