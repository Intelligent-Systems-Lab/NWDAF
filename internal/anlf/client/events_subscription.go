package client

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

	"github.com/google/uuid"

	"github.com/free5gc/openapi/models"
)

const maxEventsSubscriptionBodyBytes = 1024 * 1024

type EventsSubscriptionError struct {
	StatusCode     int
	ProblemDetails models.ProblemDetails
}

func (e *EventsSubscriptionError) Error() string {
	return fmt.Sprintf(
		"AnLF backend Events Subscription request failed: status=%d cause=%s detail=%s",
		e.StatusCode,
		e.ProblemDetails.Cause,
		e.ProblemDetails.Detail,
	)
}

func (e *EventsSubscriptionError) HTTPStatusCode() int {
	return e.StatusCode
}

func (e *EventsSubscriptionError) StandardProblemDetails() *models.ProblemDetails {
	problem := e.ProblemDetails
	if problem.Status == 0 {
		problem.Status = int32(e.StatusCode)
	}
	return &problem
}

func (c *Client) CreateEventsSubscription(
	parent context.Context,
	subscription *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, string, error) {
	headers, body, err := c.doEventsSubscriptionRequest(
		parent,
		http.MethodPost,
		c.endpoint+"/internal/v1/events-subscriptions",
		subscription,
		http.StatusCreated,
	)
	if err != nil {
		return nil, "", err
	}
	location := headers.Get("Location")
	parsed, parseErr := url.Parse(location)
	if parseErr != nil || location == "" {
		return nil, "", errors.New("AnLF backend create response is missing a valid Location")
	}
	subscriptionID := path.Base(parsed.Path)
	if uuid.Validate(subscriptionID) != nil {
		return nil, "", errors.New("AnLF backend create Location has an invalid subscription ID")
	}
	var representation models.NnwdafEventsSubscription
	if err = json.Unmarshal(body, &representation); err != nil {
		return nil, "", fmt.Errorf("decode AnLF backend create representation: %w", err)
	}
	return &representation, subscriptionID, nil
}

func (c *Client) ReplaceEventsSubscription(
	parent context.Context,
	subscriptionID string,
	subscription *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, error) {
	_, body, err := c.doEventsSubscriptionRequest(
		parent,
		http.MethodPut,
		c.eventsSubscriptionURL(subscriptionID),
		subscription,
		http.StatusOK,
	)
	if err != nil {
		return nil, err
	}
	var representation models.NnwdafEventsSubscription
	if err = json.Unmarshal(body, &representation); err != nil {
		return nil, fmt.Errorf("decode AnLF backend replace representation: %w", err)
	}
	return &representation, nil
}

func (c *Client) DeleteEventsSubscription(parent context.Context, subscriptionID string) error {
	_, _, err := c.doEventsSubscriptionRequest(
		parent,
		http.MethodDelete,
		c.eventsSubscriptionURL(subscriptionID),
		nil,
		http.StatusNoContent,
	)
	return err
}

func (c *Client) eventsSubscriptionURL(subscriptionID string) string {
	return c.endpoint + "/internal/v1/events-subscriptions/" + url.PathEscape(subscriptionID)
}

func (c *Client) doEventsSubscriptionRequest(
	parent context.Context,
	method string,
	requestURL string,
	body any,
	expectedStatus int,
) (http.Header, []byte, error) {
	ctx, cancel, err := timeoutContextFromParent(parent, c.timeout, "route Events Subscription")
	if err != nil {
		return nil, nil, err
	}
	defer cancel()
	var reader io.Reader
	if body != nil {
		encoded, encodeErr := json.Marshal(body)
		if encodeErr != nil {
			return nil, nil, fmt.Errorf("marshal Events Subscription request: %w", encodeErr)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, requestURL, reader)
	if err != nil {
		return nil, nil, fmt.Errorf("create Events Subscription request: %w", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, nil, fmt.Errorf("route Events Subscription: %w", err)
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxEventsSubscriptionBodyBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil {
		return nil, nil, fmt.Errorf("read Events Subscription response: %w", readErr)
	}
	if len(responseBody) > maxEventsSubscriptionBodyBytes {
		return nil, nil, errors.New("events subscription response exceeds transport limit")
	}
	if closeErr != nil {
		return nil, nil, fmt.Errorf("close Events Subscription response: %w", closeErr)
	}
	if response.StatusCode != expectedStatus {
		problem := models.ProblemDetails{
			Status: int32(response.StatusCode),
			Title:  http.StatusText(response.StatusCode),
			Detail: strings.TrimSpace(string(responseBody)),
		}
		if unmarshalErr := json.Unmarshal(responseBody, &problem); unmarshalErr != nil {
			problem.Detail = strings.TrimSpace(string(responseBody))
		}
		return nil, nil, &EventsSubscriptionError{
			StatusCode:     response.StatusCode,
			ProblemDetails: problem,
		}
	}
	if expectedStatus != http.StatusNoContent {
		mediaType, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
		if mediaErr != nil || mediaType != "application/json" {
			return nil, nil, errors.New(
				"AnLF backend success response Content-Type must be application/json",
			)
		}
		if len(responseBody) == 0 {
			return nil, nil, errors.New("AnLF backend success representation is required")
		}
	}
	return response.Header.Clone(), responseBody, nil
}
