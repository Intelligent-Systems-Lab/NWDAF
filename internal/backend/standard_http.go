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
	StatusCode           int
	Location             string
	RequestURI           string
	EffectiveURI         string
	PermanentRedirectURI string
	ContentType          string
	Body                 json.RawMessage
}

// StandardOperationContract describes the response surface declared by one
// Release 18 operation. Redirect following is reserved for Go-owned outbound
// standard callbacks; private backend calls must leave it disabled.
type StandardOperationContract struct {
	SuccessValidators  map[int]func([]byte) error
	ErrorStatuses      map[int]struct{}
	FollowRedirects    bool
	RequestContentType string
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

func ParseSelectedTargetHeaders(
	header http.Header,
	expectedService string,
) (*SelectedTarget, error) {
	values := []string{
		strings.TrimSpace(header.Get(TargetNFInstanceIDHeader)),
		strings.TrimSpace(header.Get(TargetNFServiceInstanceIDHeader)),
		strings.TrimSpace(header.Get(TargetAPIRootHeader)),
		strings.TrimSpace(header.Get(TargetSelectionSourceHeader)),
	}
	present := 0
	for _, value := range values {
		if value != "" {
			present++
		}
	}
	if present == 0 {
		return nil, nil
	}
	if present != len(values) {
		return nil, errors.New("all selected target headers are required when routing to a peer")
	}
	nfID, err := uuid.Parse(values[0])
	if err != nil || nfID.Version() != 4 {
		return nil, errors.New("selected target NF instance ID must be a UUIDv4")
	}
	if values[1] == "" {
		return nil, errors.New("selected target NF service instance ID is required")
	}
	apiRoot, err := ValidateAbsoluteHTTPURI(values[2], false)
	if err != nil {
		return nil, fmt.Errorf("selected target API root: %w", err)
	}
	parsedAPIRoot, err := url.Parse(apiRoot)
	if err != nil || parsedAPIRoot.RawQuery != "" {
		return nil, errors.New("selected target API root must not contain a query")
	}
	if values[3] != SelectionSourceNRF && values[3] != SelectionSourceConfigured {
		return nil, errors.New("selected target source must be NRF or CONFIGURED")
	}
	if strings.TrimSpace(expectedService) == "" {
		return nil, errors.New("selected target expected service is required")
	}
	return &SelectedTarget{
		NFInstanceID:        nfID.String(),
		NFServiceInstanceID: values[1],
		ServiceName:         expectedService,
		APIRoot:             apiRoot,
		SelectionSource:     values[3],
	}, nil
}

func ValidateAbsoluteHTTPURI(value string, allowFragment bool) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("must be an absolute HTTP(S) URI")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", errors.New("must use HTTP or HTTPS")
	}
	if parsed.User != nil {
		return "", errors.New("must not contain userinfo")
	}
	if !allowFragment && parsed.Fragment != "" {
		return "", errors.New("must not contain a fragment")
	}
	return parsed.String(), nil
}

func ResolvePeerLocation(effectiveRequestURI, location string) (string, error) {
	base, err := url.Parse(strings.TrimSpace(effectiveRequestURI))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return "", errors.New("effective peer request URI is invalid")
	}
	reference, err := url.Parse(strings.TrimSpace(location))
	if err != nil || strings.TrimSpace(location) == "" {
		return "", errors.New("create response is missing a valid Location")
	}
	resolved := base.ResolveReference(reference)
	return ValidateAbsoluteHTTPURI(resolved.String(), false)
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
		contentType := strings.TrimSpace(contract.RequestContentType)
		if contentType == "" {
			contentType = "application/json"
		}
		request.Header.Set("Content-Type", contentType)
	}
	transport := *client
	permanentRedirectURI := ""
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
		if request.URL.User != nil {
			return errors.New("redirect URI must not contain userinfo")
		}
		if len(via) > 0 && via[len(via)-1].URL.Scheme == "https" && request.URL.Scheme == "http" {
			return errors.New("redirect must not downgrade HTTPS to HTTP")
		}
		if request.Response.StatusCode == http.StatusPermanentRedirect {
			permanentRedirectURI = request.URL.String()
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
		StatusCode:           response.StatusCode,
		Location:             response.Header.Get("Location"),
		RequestURI:           requestURL,
		EffectiveURI:         response.Request.URL.String(),
		PermanentRedirectURI: permanentRedirectURI,
		ContentType:          response.Header.Get("Content-Type"),
		Body:                 append(json.RawMessage(nil), responseBody...),
	}
	validator, supported := contract.SuccessValidators[response.StatusCode]
	if response.StatusCode == http.StatusTemporaryRedirect ||
		response.StatusCode == http.StatusPermanentRedirect {
		return result, &ContractError{
			Operation: operation,
			Detail:    fmt.Sprintf("unresolved redirect status %d", response.StatusCode),
		}
	}
	if response.StatusCode >= http.StatusBadRequest {
		if _, declared := contract.ErrorStatuses[response.StatusCode]; !declared {
			return result, &ContractError{
				Operation: operation,
				Detail:    fmt.Sprintf("undeclared backend error status %d", response.StatusCode),
			}
		}
		mediaType, _, mediaErr := mime.ParseMediaType(result.ContentType)
		if mediaErr != nil || mediaType != "application/problem+json" {
			return result, &ContractError{
				Operation: operation,
				Detail:    "backend error response Content-Type must be application/problem+json",
			}
		}
		var problem models.ProblemDetails
		if err = json.Unmarshal(responseBody, &problem); err != nil {
			return result, &ContractError{Operation: operation, Detail: "backend returned malformed ProblemDetails"}
		}
		return result, &StandardError{
			StatusCode:     response.StatusCode,
			Location:       result.Location,
			ProblemDetails: problem,
		}
	}
	if !supported {
		return result, &ContractError{
			Operation: operation,
			Detail:    fmt.Sprintf("unexpected backend success status %d", response.StatusCode),
		}
	}
	if response.StatusCode == http.StatusNoContent {
		if len(bytes.TrimSpace(responseBody)) != 0 {
			return result, &ContractError{Operation: operation, Detail: "204 response must not contain a body"}
		}
		return result, nil
	}
	mediaType, _, mediaErr := mime.ParseMediaType(result.ContentType)
	if mediaErr != nil || mediaType != "application/json" {
		return result, &ContractError{
			Operation: operation,
			Detail:    "backend success response Content-Type must be application/json",
		}
	}
	if len(bytes.TrimSpace(responseBody)) == 0 {
		return result, &ContractError{Operation: operation, Detail: "backend success representation is required"}
	}
	if validator != nil {
		if err = validator(responseBody); err != nil {
			return result, &ContractError{
				Operation: operation,
				Detail:    "backend returned an invalid success representation: " + err.Error(),
			}
		}
	}
	return result, nil
}
