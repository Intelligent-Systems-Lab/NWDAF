package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

const maxSmfStandardBodyBytes = 4 * 1024 * 1024

type StandardSmfResponse struct {
	StatusCode          int
	Location            string
	ContentType         string
	Body                []byte
	ProvisionalResource bool
}

type StandardSmfError struct {
	StatusCode     int
	ProblemDetails models.ProblemDetails
}

func (e *StandardSmfError) Error() string {
	return fmt.Sprintf("SMF Event Exposure request failed: status=%d", e.StatusCode)
}

func (e *StandardSmfError) HTTPStatusCode() int {
	return e.StatusCode
}

func (e *StandardSmfError) StandardProblemDetails() *models.ProblemDetails {
	problem := e.ProblemDetails
	if problem.Status == 0 {
		problem.Status = int32(e.StatusCode)
	}
	return &problem
}

func (s *NsmfService) ExecuteStandardRequest(
	ctx context.Context,
	method string,
	requestURL string,
	body []byte,
) (*StandardSmfResponse, error) {
	requestCtx, cancel, err := timeoutContextFromParent(ctx, 30*time.Second, "SMF Event Exposure request")
	if err != nil {
		return nil, err
	}
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, method, requestURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create SMF Event Exposure request: %w", err)
	}
	if len(body) > 0 {
		request.Header.Set("Content-Type", "application/json")
	}
	if bindErr := bindOAuthTokenToRequest(request, requestCtx); bindErr != nil {
		return nil, fmt.Errorf("bind OAuth2 token to SMF Event Exposure request: %w", bindErr)
	}
	client := *s.httpClient
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("send SMF Event Exposure request: %w", err)
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxSmfStandardBodyBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read SMF Event Exposure response: %w", readErr)
	}
	if len(responseBody) > maxSmfStandardBodyBytes {
		return nil, errors.New("SMF Event Exposure response exceeds transport limit")
	}
	if closeErr != nil {
		consumerLog.Debugf("failed to close SMF response body: %v", closeErr)
	}
	standardResponse := &StandardSmfResponse{
		StatusCode:  response.StatusCode,
		Location:    response.Header.Get("Location"),
		ContentType: response.Header.Get("Content-Type"),
		Body:        responseBody,
	}
	if response.StatusCode >= http.StatusBadRequest || response.StatusCode == http.StatusTemporaryRedirect ||
		response.StatusCode == http.StatusPermanentRedirect {
		problem := models.ProblemDetails{
			Status: int32(response.StatusCode),
			Title:  http.StatusText(response.StatusCode),
		}
		if unmarshalErr := json.Unmarshal(responseBody, &problem); unmarshalErr != nil {
			problem.Detail = strings.TrimSpace(string(responseBody))
		}
		return standardResponse, &StandardSmfError{
			StatusCode:     response.StatusCode,
			ProblemDetails: problem,
		}
	}
	return standardResponse, nil
}

type standardSmfService interface {
	ExecuteStandardRequest(context.Context, string, string, []byte) (*StandardSmfResponse, error)
}

func (c *Consumer) CreateSmfEventExposure(
	ctx context.Context,
	targetAPIBaseURI string,
	body []byte,
) (*StandardSmfResponse, error) {
	service, ok := c.smfService.(standardSmfService)
	if !ok {
		return nil, errors.New("standard SMF Event Exposure transport is unavailable")
	}
	baseURI, err := validateTargetAPIBaseURI(targetAPIBaseURI)
	if err != nil {
		return nil, err
	}
	requestCtx, err := c.smfRequestContext(ctx)
	if err != nil {
		return nil, err
	}
	var requestedSubscription models.NsmfEventExposure
	if decodeErr := json.Unmarshal(body, &requestedSubscription); decodeErr != nil {
		return nil, fmt.Errorf("decode SMF create request: %w", decodeErr)
	}
	response, err := service.ExecuteStandardRequest(
		requestCtx,
		http.MethodPost,
		baseURI+SmfEventExposurePath,
		body,
	)
	if err != nil {
		return response, err
	}
	if response.StatusCode != http.StatusCreated || response.Location == "" {
		return nil, errors.New("malformed SMF create response: expected 201 and Location")
	}
	resolvedLocation, resolveErr := resolvePeerLocation(baseURI, response.Location)
	if resolveErr != nil {
		return nil, resolveErr
	}
	peerSubscriptionID := path.Base(resolvedLocation)
	if peerSubscriptionID == "" || peerSubscriptionID == "." || peerSubscriptionID == "/" {
		return nil, errors.New("malformed SMF create response: subscription ID is missing")
	}
	provisionalRoute := &nwdaf_context.SmfPeerResourceRoute{
		SubscriptionID:   peerSubscriptionID,
		ResourceLocation: resolvedLocation,
		TargetAPIBaseURI: baseURI,
		CorrelationID:    requestedSubscription.NotifId,
		PendingCleanup:   true,
	}
	nwdafCtx := c.Context()
	if nwdafCtx == nil {
		return response, errors.New("NWDAF context is unavailable")
	}
	if !nwdafCtx.AddSmfPeerResourceRoute(provisionalRoute) {
		existing, found := nwdafCtx.GetSmfPeerResourceRoute(baseURI, peerSubscriptionID)
		if found && existing.ResourceLocation == resolvedLocation {
			return response, errors.New("SMF create response collides with an existing peer resource route")
		}
		return response, errors.New("could not record provisional SMF peer resource route")
	}
	response.ProvisionalResource = true
	if !isJSONMediaType(response.ContentType) {
		c.compensateOrRetainSmfPeerResource(requestCtx, provisionalRoute, "invalid response media type")
		return response, errors.New("malformed SMF create response: Content-Type must be application/json")
	}
	if len(response.Body) == 0 {
		c.compensateOrRetainSmfPeerResource(requestCtx, provisionalRoute, "missing representation")
		return response, errors.New("malformed SMF create response: representation is required")
	}
	var representation models.NsmfEventExposure
	if decodeErr := json.Unmarshal(response.Body, &representation); decodeErr != nil {
		c.compensateOrRetainSmfPeerResource(requestCtx, provisionalRoute, "malformed response")
		return response, fmt.Errorf("decode SMF create representation: %w", decodeErr)
	}
	subscriptionID := representation.SubId
	if subscriptionID == "" {
		subscriptionID = peerSubscriptionID
	}
	if subscriptionID == "" || subscriptionID == "." || subscriptionID == "/" {
		c.compensateOrRetainSmfPeerResource(requestCtx, provisionalRoute, "missing subscription ID")
		return response, errors.New("malformed SMF create response: subscription ID is missing")
	}
	if subscriptionID != peerSubscriptionID {
		c.compensateOrRetainSmfPeerResource(requestCtx, provisionalRoute, "conflicting subscription ID")
		return response, errors.New("malformed SMF create response: Location and representation subscription IDs differ")
	}
	requestedSubscription.SubId = subscriptionID
	acceptedSubscriptionJSON, mergeErr := mergeSmfSubscriptionRepresentation(
		body,
		response.Body,
		subscriptionID,
	)
	if mergeErr != nil {
		c.compensateOrRetainSmfPeerResource(requestCtx, provisionalRoute, "representation merge failure")
		return response, mergeErr
	}
	var acceptedSubscription models.NsmfEventExposure
	if decodeErr := json.Unmarshal(acceptedSubscriptionJSON, &acceptedSubscription); decodeErr != nil {
		c.compensateOrRetainSmfPeerResource(requestCtx, provisionalRoute, "invalid merged representation")
		return nil, fmt.Errorf("decode accepted SMF create representation: %w", decodeErr)
	}
	provisionalRoute.CorrelationID = acceptedSubscription.NotifId
	provisionalRoute.AcceptedSubscription = acceptedSubscription
	provisionalRoute.AcceptedSubscriptionJSON = acceptedSubscriptionJSON
	provisionalRoute.PendingCleanup = false
	if !nwdafCtx.UpdateSmfPeerResourceRoute(provisionalRoute) {
		c.compensateOrRetainSmfPeerResource(requestCtx, provisionalRoute, "local route finalization failure")
		return response, errors.New("could not record SMF peer resource route")
	}
	response.Location = resolvedLocation
	response.ProvisionalResource = false
	return response, nil
}

func (c *Consumer) compensateOrRetainSmfPeerResource(
	ctx context.Context,
	route *nwdaf_context.SmfPeerResourceRoute,
	reason string,
) {
	if route == nil {
		return
	}
	if err := c.compensateCreatedSmfResource(ctx, route.TargetAPIBaseURI, route.ResourceLocation); err != nil {
		consumerLog.Errorf(
			"Failed to compensate SMF create %s; retaining pending cleanup route: apiRoot=%s peerSubId=%s err=%v",
			reason,
			route.TargetAPIBaseURI,
			route.SubscriptionID,
			err,
		)
		return
	}
	if nwdafCtx := c.Context(); nwdafCtx != nil {
		nwdafCtx.DeleteSmfPeerResourceRoute(route.TargetAPIBaseURI, route.SubscriptionID)
	}
}

func (c *Consumer) compensateCreatedSmfResource(
	ctx context.Context,
	targetAPIBaseURI string,
	resourceLocation string,
) error {
	service, ok := c.smfService.(standardSmfService)
	if !ok {
		return errors.New("standard SMF Event Exposure transport is unavailable")
	}
	response, err := service.ExecuteStandardRequest(
		ctx,
		http.MethodDelete,
		resourceLocation,
		nil,
	)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
		return fmt.Errorf("compensating SMF delete returned status %d", response.StatusCode)
	}
	return nil
}

func mergeSmfSubscriptionRepresentation(
	requestBody []byte,
	responseBody []byte,
	subscriptionID string,
) (json.RawMessage, error) {
	requested := make(map[string]any)
	if err := json.Unmarshal(requestBody, &requested); err != nil {
		return nil, fmt.Errorf("decode SMF create request representation: %w", err)
	}
	accepted := make(map[string]any)
	if err := json.Unmarshal(responseBody, &accepted); err != nil {
		return nil, fmt.Errorf("decode SMF create response representation: %w", err)
	}
	for key, value := range accepted {
		requested[key] = value
	}
	requested["subId"] = subscriptionID
	merged, err := json.Marshal(requested)
	if err != nil {
		return nil, fmt.Errorf("encode accepted SMF subscription representation: %w", err)
	}
	return merged, nil
}

func (c *Consumer) ReplaceSmfEventExposure(
	ctx context.Context,
	targetAPIBaseURI string,
	subscriptionID string,
	body []byte,
) (*StandardSmfResponse, error) {
	return c.executeRoutedSmfRequest(ctx, http.MethodPut, targetAPIBaseURI, subscriptionID, body)
}

func (c *Consumer) ReadSmfEventExposure(
	ctx context.Context,
	targetAPIBaseURI string,
	subscriptionID string,
) (*StandardSmfResponse, error) {
	return c.executeRoutedSmfRequest(ctx, http.MethodGet, targetAPIBaseURI, subscriptionID, nil)
}

func (c *Consumer) DeleteSmfEventExposure(
	ctx context.Context,
	targetAPIBaseURI string,
	subscriptionID string,
) (*StandardSmfResponse, error) {
	response, err := c.executeRoutedSmfRequest(
		ctx, http.MethodDelete, targetAPIBaseURI, subscriptionID, nil,
	)
	if err != nil {
		var standardError *StandardSmfError
		if errors.As(err, &standardError) && standardError.StatusCode == http.StatusNotFound {
			if nwdafCtx := c.Context(); nwdafCtx != nil {
				nwdafCtx.DeleteSmfPeerResourceRoute(targetAPIBaseURI, subscriptionID)
			}
			return response, err
		}
		c.markSmfPeerCleanupPending(targetAPIBaseURI, subscriptionID)
		return response, err
	}
	if response.StatusCode != http.StatusNoContent {
		c.markSmfPeerCleanupPending(targetAPIBaseURI, subscriptionID)
		return nil, fmt.Errorf("malformed SMF delete response: status=%d", response.StatusCode)
	}
	if nwdafCtx := c.Context(); nwdafCtx != nil {
		nwdafCtx.DeleteSmfPeerResourceRoute(targetAPIBaseURI, subscriptionID)
	}
	return response, nil
}

func (c *Consumer) markSmfPeerCleanupPending(targetAPIBaseURI string, subscriptionID string) {
	nwdafCtx := c.Context()
	if nwdafCtx == nil {
		return
	}
	route, found := nwdafCtx.GetSmfPeerResourceRoute(targetAPIBaseURI, subscriptionID)
	if !found {
		return
	}
	route.PendingCleanup = true
	nwdafCtx.UpdateSmfPeerResourceRoute(&route)
}

func (c *Consumer) executeRoutedSmfRequest(
	ctx context.Context,
	method string,
	targetAPIBaseURI string,
	subscriptionID string,
	body []byte,
) (*StandardSmfResponse, error) {
	nwdafCtx := c.Context()
	if nwdafCtx == nil {
		return nil, errors.New("NWDAF context is unavailable")
	}
	normalizedTarget, targetErr := validateTargetAPIBaseURI(targetAPIBaseURI)
	if targetErr != nil {
		return nil, targetErr
	}
	route, found := nwdafCtx.GetSmfPeerResourceRoute(normalizedTarget, subscriptionID)
	if !found {
		return nil, &StandardSmfError{
			StatusCode: http.StatusNotFound,
			ProblemDetails: models.ProblemDetails{
				Status: http.StatusNotFound,
				Title:  http.StatusText(http.StatusNotFound),
				Cause:  "SUBSCRIPTION_NOT_FOUND",
			},
		}
	}
	service, ok := c.smfService.(standardSmfService)
	if !ok {
		return nil, errors.New("standard SMF Event Exposure transport is unavailable")
	}
	requestCtx, err := c.smfRequestContext(ctx)
	if err != nil {
		return nil, err
	}
	response, err := service.ExecuteStandardRequest(
		requestCtx,
		method,
		route.ResourceLocation,
		body,
	)
	if err != nil {
		return response, err
	}
	switch method {
	case http.MethodGet:
		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("malformed SMF read response: status=%d", response.StatusCode)
		}
		if !isJSONMediaType(response.ContentType) || len(response.Body) == 0 {
			return nil, errors.New("malformed SMF read response: JSON representation is required")
		}
		var representation models.NsmfEventExposure
		if decodeErr := json.Unmarshal(response.Body, &representation); decodeErr != nil {
			return nil, fmt.Errorf("decode SMF read representation: %w", decodeErr)
		}
	case http.MethodPut:
		if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
			return nil, fmt.Errorf("malformed SMF replace response: status=%d", response.StatusCode)
		}
		responseRepresentation := response.Body
		if response.StatusCode == http.StatusOK &&
			(!isJSONMediaType(response.ContentType) || len(responseRepresentation) == 0) {
			return nil, errors.New("malformed SMF replace response: 200 requires a JSON representation")
		}
		if response.StatusCode == http.StatusNoContent {
			responseRepresentation = body
		}
		acceptedJSON, mergeErr := mergeSmfSubscriptionRepresentation(
			body,
			responseRepresentation,
			subscriptionID,
		)
		if mergeErr != nil {
			return nil, mergeErr
		}
		var accepted models.NsmfEventExposure
		if decodeErr := json.Unmarshal(acceptedJSON, &accepted); decodeErr != nil {
			return nil, fmt.Errorf("decode accepted SMF replace representation: %w", decodeErr)
		}
		route.AcceptedSubscription = accepted
		route.AcceptedSubscriptionJSON = acceptedJSON
		route.CorrelationID = accepted.NotifId
		nwdafCtx.UpdateSmfPeerResourceRoute(&route)
	}
	return response, nil
}

func validateTargetAPIBaseURI(value string) (string, error) {
	normalized := strings.TrimRight(strings.TrimSpace(value), "/")
	parsed, err := url.ParseRequestURI(normalized)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", errors.New("Target-Api-Root must be an absolute HTTP(S) API root")
	}
	return normalized, nil
}

func resolvePeerLocation(targetAPIBaseURI string, location string) (string, error) {
	base, err := url.Parse(targetAPIBaseURI + "/")
	if err != nil {
		return "", err
	}
	reference, err := url.Parse(location)
	if err != nil {
		return "", errors.New("SMF create Location is invalid")
	}
	resolved := base.ResolveReference(reference)
	if resolved.Scheme != "http" && resolved.Scheme != "https" || resolved.Host == "" {
		return "", errors.New("SMF create Location is not an absolute HTTP(S) URI")
	}
	return resolved.String(), nil
}
