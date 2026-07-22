package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

const maxCallbackResponseBodyBytes = 1024 * 1024

type CallbackDeliveryError struct {
	StatusCode     int
	Location       string
	ProblemDetails models.ProblemDetails
}

func (e *CallbackDeliveryError) Error() string {
	return fmt.Sprintf("consumer rejected analytics notification: status=%d", e.StatusCode)
}

func (e *CallbackDeliveryError) HTTPStatusCode() int {
	return e.StatusCode
}

func (e *CallbackDeliveryError) StandardProblemDetails() *models.ProblemDetails {
	problem := e.ProblemDetails
	if problem.Status == 0 {
		problem.Status = int32(e.StatusCode)
	}
	return &problem
}

func (e *CallbackDeliveryError) RedirectLocation() string {
	return e.Location
}

func (d *ReportDispatcher) DispatchEventsSubscriptionNotifications(
	notifications []models.NnwdafEventsSubscriptionNotification,
	rawBody []byte,
) error {
	if len(notifications) == 0 {
		return ErrInvalidAnalyticsReport
	}
	ctx := nwdaf_context.GetSelf()
	if ctx == nil {
		return fmt.Errorf("%w: context unavailable", ErrExternalDelivery)
	}

	callbackURI := ""
	for i := range notifications {
		notification := &notifications[i]
		if notification.SubscriptionId == "" || len(notification.EventNotifications) == 0 {
			return ErrInvalidAnalyticsReport
		}
		route, found := ctx.GetAnalyticsSubscriptionRoute(notification.SubscriptionId)
		if !found {
			return ErrSubscriptionNotFound
		}
		if notification.NotifCorrId != route.AcceptedSubscription.NotifCorrId {
			return ErrInvalidAnalyticsReport
		}
		if callbackURI == "" {
			callbackURI = route.ExternalNotificationURI
		} else if callbackURI != route.ExternalNotificationURI {
			return ErrInvalidAnalyticsReport
		}
	}
	if len(rawBody) == 0 {
		var err error
		rawBody, err = json.Marshal(notifications)
		if err != nil {
			return fmt.Errorf("%w: marshal notification: %v", ErrInvalidAnalyticsReport, err)
		}
	}

	requestCtx, cancel := context.WithTimeout(d.baseCtx, 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodPost,
		callbackURI,
		bytes.NewReader(rawBody),
	)
	if err != nil {
		return fmt.Errorf("%w: create request: %v", ErrExternalDelivery, err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := d.client.Do(request)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrExternalDelivery, err)
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxCallbackResponseBodyBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil {
		return fmt.Errorf("%w: read response: %v", ErrExternalDelivery, readErr)
	}
	if closeErr != nil {
		logger.NotifierLog.Debugf("Failed to close analytics delivery response: %v", closeErr)
	}
	if len(responseBody) > maxCallbackResponseBodyBytes {
		return fmt.Errorf("%w: response exceeds transport limit", ErrExternalDelivery)
	}
	if response.StatusCode != http.StatusNoContent {
		problem := models.ProblemDetails{
			Status: int32(response.StatusCode),
			Title:  http.StatusText(response.StatusCode),
		}
		if unmarshalErr := json.Unmarshal(responseBody, &problem); unmarshalErr != nil {
			problem.Detail = string(responseBody)
		}
		return &CallbackDeliveryError{
			StatusCode:     response.StatusCode,
			Location:       response.Header.Get("Location"),
			ProblemDetails: problem,
		}
	}
	return nil
}
