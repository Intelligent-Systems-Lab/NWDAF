package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/free5gc/openapi/models"
)

const MaxStandardMLModelBodyBytes = 4 * 1024 * 1024

// StandardResponse preserves an accepted standard-shaped backend response.
type StandardResponse struct {
	StatusCode  int
	Location    string
	ContentType string
	Body        json.RawMessage
}

// StandardOperationContract describes the response surface declared by one
// Release 18 operation. Redirect following is reserved for Go-owned outbound
// standard callbacks; private backend calls must leave it disabled.
type StandardOperationContract struct {
	SuccessValidators map[int]func([]byte) error
	ErrorStatuses     map[int]struct{}
	FollowRedirects   bool
}

func ErrorStatuses(statuses ...int) map[int]struct{} {
	result := make(map[int]struct{}, len(statuses))
	for _, status := range statuses {
		result[status] = struct{}{}
	}
	return result
}

func ResourceIDFromLocation(location string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(location))
	if err != nil || strings.TrimSpace(location) == "" {
		return "", errors.New("create response is missing a valid Location")
	}
	resourceID := path.Base(parsed.Path)
	parsedID, err := uuid.Parse(resourceID)
	if err != nil || parsedID.Version() != 4 {
		return "", errors.New("create Location must end in a UUIDv4 resource ID")
	}
	return resourceID, nil
}

// StandardError represents a well-formed ProblemDetails response from a
// standard-shaped private backend operation.
type StandardError struct {
	StatusCode     int
	Location       string
	ProblemDetails models.ProblemDetails
}

func (e *StandardError) Error() string {
	return fmt.Sprintf("backend request failed: status=%d", e.StatusCode)
}

func (e *StandardError) HTTPStatusCode() int {
	return e.StatusCode
}

func (e *StandardError) RedirectLocation() string {
	return e.Location
}

func (e *StandardError) StandardProblemDetails() *models.ProblemDetails {
	problem := e.ProblemDetails
	problem.Status = int32(e.StatusCode)
	return &problem
}

// TransportError identifies a request that never produced an HTTP response.
type TransportError struct {
	Operation string
	Cause     error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("%s: %v", e.Operation, e.Cause)
}

func (e *TransportError) Unwrap() error {
	return e.Cause
}

// ContractError identifies a backend response that violates the expected
// standard-shaped success or ProblemDetails contract.
type ContractError struct {
	Operation string
	Detail    string
}

func (e *ContractError) Error() string {
	return fmt.Sprintf("%s: %s", e.Operation, e.Detail)
}

// ExecuteStandardRequest is the shared transport used by the AnLF and MTLF
// backend clients for Release 18 standard-shaped resource operations.
func ExecuteStandardRequest(
	parent context.Context,
	client *http.Client,
	timeout time.Duration,
	method string,
	requestURL string,
	body []byte,
	operation string,
	contract StandardOperationContract,
) (*StandardResponse, error) {
	if parent == nil {
		return nil, &TransportError{Operation: operation, Cause: errors.New("parent context is required")}
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, requestURL, bytes.NewReader(body))
	if err != nil {
		return nil, &ContractError{Operation: operation, Detail: "could not create backend request"}
	}
	if len(body) > 0 {
		request.Header.Set("Content-Type", "application/json")
	}
	transport := *client
	transport.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if !contract.FollowRedirects {
			return http.ErrUseLastResponse
		}
		if request.Response == nil ||
			(request.Response.StatusCode != http.StatusTemporaryRedirect &&
				request.Response.StatusCode != http.StatusPermanentRedirect) {
			return http.ErrUseLastResponse
		}
		if len(via) >= 3 {
			return http.ErrUseLastResponse
		}
		return nil
	}
	response, err := transport.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return nil, &TransportError{Operation: operation, Cause: err}
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, MaxStandardMLModelBodyBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil {
		return nil, &ContractError{Operation: operation, Detail: "could not read backend response"}
	}
	if len(responseBody) > MaxStandardMLModelBodyBytes {
		return nil, &ContractError{Operation: operation, Detail: "backend response exceeds transport limit"}
	}
	if closeErr != nil {
		return nil, &ContractError{Operation: operation, Detail: "could not close backend response"}
	}
	result := &StandardResponse{
		StatusCode:  response.StatusCode,
		Location:    response.Header.Get("Location"),
		ContentType: response.Header.Get("Content-Type"),
		Body:        append(json.RawMessage(nil), responseBody...),
	}
	validator, supported := contract.SuccessValidators[response.StatusCode]
	if response.StatusCode == http.StatusTemporaryRedirect ||
		response.StatusCode == http.StatusPermanentRedirect {
		return nil, &ContractError{
			Operation: operation,
			Detail:    fmt.Sprintf("unresolved redirect status %d", response.StatusCode),
		}
	}
	if response.StatusCode >= http.StatusBadRequest {
		if _, declared := contract.ErrorStatuses[response.StatusCode]; !declared {
			return nil, &ContractError{
				Operation: operation,
				Detail:    fmt.Sprintf("undeclared backend error status %d", response.StatusCode),
			}
		}
		mediaType, _, mediaErr := mime.ParseMediaType(result.ContentType)
		if mediaErr != nil || mediaType != "application/problem+json" {
			return nil, &ContractError{
				Operation: operation,
				Detail:    "backend error response Content-Type must be application/problem+json",
			}
		}
		var problem models.ProblemDetails
		if err = json.Unmarshal(responseBody, &problem); err != nil {
			return nil, &ContractError{Operation: operation, Detail: "backend returned malformed ProblemDetails"}
		}
		return result, &StandardError{
			StatusCode:     response.StatusCode,
			Location:       result.Location,
			ProblemDetails: problem,
		}
	}
	if !supported {
		return nil, &ContractError{
			Operation: operation,
			Detail:    fmt.Sprintf("unexpected backend success status %d", response.StatusCode),
		}
	}
	if response.StatusCode == http.StatusNoContent {
		if len(bytes.TrimSpace(responseBody)) != 0 {
			return nil, &ContractError{Operation: operation, Detail: "204 response must not contain a body"}
		}
		return result, nil
	}
	mediaType, _, mediaErr := mime.ParseMediaType(result.ContentType)
	if mediaErr != nil || mediaType != "application/json" {
		return nil, &ContractError{
			Operation: operation,
			Detail:    "backend success response Content-Type must be application/json",
		}
	}
	if len(bytes.TrimSpace(responseBody)) == 0 {
		return nil, &ContractError{Operation: operation, Detail: "backend success representation is required"}
	}
	if validator != nil {
		if err = validator(responseBody); err != nil {
			return nil, &ContractError{
				Operation: operation,
				Detail:    "backend returned an invalid success representation: " + err.Error(),
			}
		}
	}
	return result, nil
}
