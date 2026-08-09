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

	"github.com/google/uuid"

	"github.com/free5gc/nwdaf/internal/backend"
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

func (c *BackendClient) CheckReadiness(parent context.Context) (backend.HealthResponse, error) {
	if parent == nil {
		return backend.HealthResponse{}, &BackendRequestError{
			Operation: "check MTLF backend readiness",
			Message:   "parent context is required",
		}
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
		return backend.HealthResponse{}, fmt.Errorf("create MTLF backend readiness request: %w", err)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return backend.HealthResponse{}, &BackendRequestError{
				Operation: "check MTLF backend readiness",
				Message:   ctx.Err().Error(),
				cause:     ctx.Err(),
			}
		}
		return backend.HealthResponse{}, &BackendRequestError{
			Operation: "check MTLF backend readiness",
			Message:   err.Error(),
			cause:     err,
		}
	}
	body, readErr := readBackendResponseBody(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return backend.HealthResponse{}, &BackendRequestError{
			Operation:  "check MTLF backend readiness",
			StatusCode: response.StatusCode,
			Code:       "RESPONSE_TOO_LARGE",
			Message:    readErr.Error(),
		}
	}
	if closeErr != nil {
		return backend.HealthResponse{}, &BackendRequestError{
			Operation:  "check MTLF backend readiness",
			StatusCode: response.StatusCode,
			Message:    closeErr.Error(),
		}
	}
	var health backend.HealthResponse
	if json.Unmarshal(body, &health) == nil &&
		uuid.Validate(health.ProcessInstanceID) == nil {
		if response.StatusCode == http.StatusOK && health.Status == "ready" {
			return health, nil
		}
		if response.StatusCode == http.StatusServiceUnavailable {
			return health, &BackendRequestError{
				Operation:  "check MTLF backend readiness",
				StatusCode: response.StatusCode,
				Code:       "NOT_READY",
				Message:    health.Status,
			}
		}
		if response.StatusCode == http.StatusOK {
			return backend.HealthResponse{}, &BackendRequestError{
				Operation:  "check MTLF backend readiness",
				StatusCode: response.StatusCode,
				Code:       "INVALID_RESPONSE",
				Message:    "malformed readiness response",
			}
		}
	}

	var payload struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &payload) != nil || payload.Code == "" {
		payload.Code = "HTTP_ERROR"
		payload.Message = strings.TrimSpace(string(body))
	}
	return health, &BackendRequestError{
		Operation:  "check MTLF backend readiness",
		StatusCode: response.StatusCode,
		Code:       payload.Code,
		Message:    payload.Message,
	}
}

func (c *BackendClient) DeliverAdrfRetrievalNotification(
	parent context.Context,
	body []byte,
) (*backend.StandardResponse, error) {
	return backend.ExecuteStandardRequest(
		parent,
		c.httpClient,
		c.timeout,
		http.MethodPost,
		c.endpoint+"/internal/v1/adrf-data-management/retrieval-notifications",
		body,
		"deliver ADRF retrieval notification",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{
				http.StatusNoContent: func(body []byte) error {
					if len(body) != 0 {
						return errors.New("204 response must not contain a body")
					}
					return nil
				},
			},
			ErrorStatuses: backend.ErrorStatuses(
				http.StatusBadRequest,
				http.StatusNotFound,
				http.StatusRequestEntityTooLarge,
				http.StatusUnsupportedMediaType,
				http.StatusTooManyRequests,
				http.StatusInternalServerError,
				http.StatusBadGateway,
				http.StatusServiceUnavailable,
			),
		},
	)
}

func (c *BackendClient) PutTrainingDataDescriptor(
	parent context.Context,
	descriptorID string,
	body []byte,
) (*backend.StandardResponse, error) {
	return backend.ExecuteStandardRequest(
		parent,
		c.httpClient,
		c.timeout,
		http.MethodPut,
		c.endpoint+"/internal/v1/anlf/training-data-descriptors/"+descriptorID,
		body,
		"put training-data descriptor",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{
				http.StatusNoContent: func(body []byte) error {
					if len(body) != 0 {
						return errors.New("204 response must not contain a body")
					}
					return nil
				},
			},
			ErrorStatuses: backend.ErrorStatuses(
				http.StatusBadRequest,
				http.StatusNotFound,
				http.StatusRequestEntityTooLarge,
				http.StatusUnsupportedMediaType,
				http.StatusInternalServerError,
				http.StatusServiceUnavailable,
			),
		},
	)
}

func (c *BackendClient) DeleteTrainingDataDescriptor(
	parent context.Context,
	descriptorID string,
) (*backend.StandardResponse, error) {
	return backend.ExecuteStandardRequest(
		parent,
		c.httpClient,
		c.timeout,
		http.MethodDelete,
		c.endpoint+"/internal/v1/anlf/training-data-descriptors/"+descriptorID,
		nil,
		"delete training-data descriptor",
		backend.StandardOperationContract{
			SuccessValidators: map[int]func([]byte) error{
				http.StatusNoContent: func(body []byte) error {
					if len(body) != 0 {
						return errors.New("204 response must not contain a body")
					}
					return nil
				},
			},
			ErrorStatuses: backend.ErrorStatuses(
				http.StatusBadRequest,
				http.StatusNotFound,
				http.StatusInternalServerError,
				http.StatusServiceUnavailable,
			),
		},
	)
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
