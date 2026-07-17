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
	"strconv"
	"strings"
	"time"
)

const maxBackendReadinessBodyBytes = 64 * 1024

type BackendRequestError struct {
	Operation  string
	StatusCode int
	Code       string
	Message    string
	cause      error
}

func (e *BackendRequestError) Error() string {
	operation := e.Operation
	if operation == "" {
		operation = "MTLF backend request"
	}
	if e.StatusCode == 0 {
		return fmt.Sprintf("%s failed: %s", operation, e.Message)
	}
	return fmt.Sprintf(
		"%s failed: status=%d code=%s message=%s",
		operation,
		e.StatusCode,
		e.Code,
		e.Message,
	)
}

func (e *BackendRequestError) HTTPStatusCode() int {
	return e.StatusCode
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
		return &BackendRequestError{Operation: "check MTLF backend readiness", Message: "parent context is required"}
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
			return &BackendRequestError{
				Operation: "check MTLF backend readiness",
				Message:   ctx.Err().Error(),
				cause:     ctx.Err(),
			}
		}
		return &BackendRequestError{
			Operation: "check MTLF backend readiness",
			Message:   err.Error(),
			cause:     err,
		}
	}
	body, readErr := readBackendResponseBody(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return &BackendRequestError{
			Operation:  "check MTLF backend readiness",
			StatusCode: response.StatusCode,
			Code:       "RESPONSE_TOO_LARGE",
			Message:    readErr.Error(),
		}
	}
	if closeErr != nil {
		return &BackendRequestError{
			Operation:  "check MTLF backend readiness",
			StatusCode: response.StatusCode,
			Message:    closeErr.Error(),
		}
	}
	if response.StatusCode == http.StatusOK {
		var payload struct {
			Status string `json:"status"`
		}
		if json.Unmarshal(body, &payload) != nil || payload.Status != "ready" {
			return &BackendRequestError{
				Operation:  "check MTLF backend readiness",
				StatusCode: response.StatusCode,
				Code:       "INVALID_RESPONSE",
				Message:    "malformed readiness response",
			}
		}
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
		Operation:  "check MTLF backend readiness",
		StatusCode: response.StatusCode,
		Code:       payload.Code,
		Message:    payload.Message,
	}
}

type DataSource string

const (
	DataSourceADRF    DataSource = "adrf"
	DataSourceMongoDB DataSource = "mongodb"
)

type StorageMode string

const (
	StorageModeADRF    StorageMode = "adrf"
	StorageModeMongoDB StorageMode = "mongodb"
	StorageModeDual    StorageMode = "dual"
)

type dataSourceSelectionRequest struct {
	AvailableDataSources []DataSource `json:"availableDataSources"`
}

type dataSourceSelectionResponse struct {
	StorageMode StorageMode `json:"storageMode"`
}

func (c *BackendClient) SelectDataSource(
	parent context.Context,
	available []DataSource,
) (StorageMode, error) {
	if parent == nil {
		return "", &BackendRequestError{
			Operation: "select MTLF backend data source",
			Message:   "parent context is required",
		}
	}
	body, err := json.Marshal(dataSourceSelectionRequest{AvailableDataSources: available})
	if err != nil {
		return "", fmt.Errorf("marshal MTLF backend data-source selection: %w", err)
	}
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		c.endpoint+"/internal/v1/data-source-selection",
		bytes.NewReader(body),
	)
	if err != nil {
		return "", fmt.Errorf("create MTLF backend data-source selection request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return "", &BackendRequestError{
				Operation: "select MTLF backend data source",
				Message:   ctx.Err().Error(),
				cause:     ctx.Err(),
			}
		}
		return "", &BackendRequestError{
			Operation: "select MTLF backend data source",
			Message:   err.Error(),
			cause:     err,
		}
	}
	responseBody, readErr := readBackendResponseBody(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return "", &BackendRequestError{
			Operation:  "select MTLF backend data source",
			StatusCode: response.StatusCode,
			Code:       "RESPONSE_TOO_LARGE",
			Message:    readErr.Error(),
		}
	}
	if closeErr != nil {
		return "", fmt.Errorf("close MTLF backend data-source selection response: %w", closeErr)
	}
	if response.StatusCode != http.StatusOK {
		var payload struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		if json.Unmarshal(responseBody, &payload) != nil || payload.Code == "" {
			payload.Code = "HTTP_ERROR"
			payload.Message = strings.TrimSpace(string(responseBody))
		}
		return "", &BackendRequestError{
			Operation:  "select MTLF backend data source",
			StatusCode: response.StatusCode,
			Code:       payload.Code,
			Message:    payload.Message,
		}
	}
	var payload dataSourceSelectionResponse
	if json.Unmarshal(responseBody, &payload) != nil || !validStorageMode(payload.StorageMode) {
		return "", &BackendRequestError{
			Operation:  "select MTLF backend data source",
			StatusCode: response.StatusCode,
			Code:       "INVALID_RESPONSE",
			Message:    "invalid storageMode",
		}
	}
	return payload.StorageMode, nil
}

func validStorageMode(mode StorageMode) bool {
	return mode == StorageModeADRF || mode == StorageModeMongoDB || mode == StorageModeDual
}

func readBackendResponseBody(reader io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, maxBackendReadinessBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxBackendReadinessBodyBytes {
		return nil, errors.New("response body exceeds transport limit")
	}
	return body, nil
}
