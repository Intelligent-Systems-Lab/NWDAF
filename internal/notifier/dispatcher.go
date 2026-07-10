package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/free5gc/nwdaf/internal/anlf"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

type predictionRecorder interface {
	RecordAnalyticsReport(subscriptionID string, report *anlf.AnalyticsReport)
}

type ReportDispatcher struct {
	baseCtx  context.Context
	client   *http.Client
	recorder predictionRecorder
}

func NewReportDispatcher(baseCtx context.Context, recorder predictionRecorder) *ReportDispatcher {
	return &ReportDispatcher{
		baseCtx:  baseCtx,
		client:   &http.Client{Timeout: 10 * time.Second},
		recorder: recorder,
	}
}

func (d *ReportDispatcher) DispatchAnalyticsReport(
	subscriptionID string,
	report *anlf.AnalyticsReport,
) error {
	ctx := nwdaf_context.GetSelf()
	if ctx == nil {
		return fmt.Errorf("%w: context unavailable", anlf.ErrExternalDelivery)
	}
	subscription := ctx.GetSubscription(subscriptionID)
	if subscription == nil {
		return anlf.ErrSubscriptionNotFound
	}
	if report == nil || report.ReportID == "" || len(report.EventNotifications) == 0 {
		return anlf.ErrInvalidAnalyticsReport
	}
	duplicate, inFlight, stale := subscription.BeginReport(
		report.ReportID,
		report.RuntimeRevision,
		report.ReportSequence,
	)
	if stale {
		return anlf.ErrStaleAnalyticsReport
	}
	if inFlight {
		return anlf.ErrAnalyticsReportInFlight
	}
	if duplicate {
		return nil
	}
	delivered := false
	defer func() {
		subscription.CompleteReport(report.ReportID, report.ReportSequence, delivered)
	}()

	notification, err := mapAnalyticsReport(subscription, report)
	if err != nil {
		return err
	}
	body, err := json.Marshal(NotificationListOutput{notification})
	if err != nil {
		return fmt.Errorf("%w: marshal notification: %v", anlf.ErrInvalidAnalyticsReport, err)
	}
	requestCtx, cancel := context.WithTimeout(d.baseCtx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodPost,
		subscription.NotificationURI,
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("%w: create request: %v", anlf.ErrExternalDelivery, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", anlf.ErrExternalDelivery, err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.NotifierLog.Debugf("Failed to close analytics delivery response: %v", closeErr)
		}
	}()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%w: status=%d", anlf.ErrExternalDelivery, resp.StatusCode)
	}
	delivered = true
	if d.recorder != nil {
		d.recorder.RecordAnalyticsReport(subscriptionID, report)
	}
	return nil
}

func mapAnalyticsReport(
	subscription *nwdaf_context.Subscription,
	report *anlf.AnalyticsReport,
) (NotificationOutput, error) {
	requestedEvents := make(map[models.NwdafEvent]struct{}, len(subscription.EventSubs))
	for i := range subscription.EventSubs {
		requestedEvents[subscription.EventSubs[i].Event] = struct{}{}
	}
	notification := models.NnwdafEventsSubscriptionNotification{
		SubscriptionId: subscription.ID,
		NotifCorrId:    subscription.NotifCorrId,
	}
	for _, event := range report.EventNotifications {
		eventType := models.NwdafEvent(event.Event)
		if eventType != models.NwdafEvent_UE_COMMUNICATION {
			return NotificationOutput{}, fmt.Errorf("%w: unsupported event %q", anlf.ErrInvalidAnalyticsReport, event.Event)
		}
		if _, ok := requestedEvents[eventType]; !ok {
			return NotificationOutput{}, fmt.Errorf("%w: event %q not requested", anlf.ErrInvalidAnalyticsReport, event.Event)
		}
		if len(event.UeCommunications) == 0 {
			return NotificationOutput{}, fmt.Errorf(
				"%w: UE communication report is empty",
				anlf.ErrInvalidAnalyticsReport,
			)
		}
		mapped := models.NwdafEventsSubscriptionEventNotification{Event: eventType}
		for _, communication := range event.UeCommunications {
			if communication.CommunicationDuration < 0 || communication.Timestamp.IsZero() ||
				communication.Confidence < 0 || communication.Confidence > 100 {
				return NotificationOutput{}, fmt.Errorf(
					"%w: invalid UE communication report",
					anlf.ErrInvalidAnalyticsReport,
				)
			}
			timestamp := communication.Timestamp
			mapped.UeComms = append(mapped.UeComms, models.UeCommunication{
				CommDur: communication.CommunicationDuration,
				Ts:      &timestamp,
				TrafChar: &models.TrafficCharacterization{
					Dnn:   communication.TrafficCharacterization.Dnn,
					UlVol: communication.TrafficCharacterization.UplinkVolume,
					DlVol: communication.TrafficCharacterization.DownlinkVolume,
				},
				Confidence: communication.Confidence,
			})
		}
		notification.EventNotifications = append(notification.EventNotifications, mapped)
	}
	return convertNotificationOutput(notification), nil
}

func convertNotificationOutput(notification models.NnwdafEventsSubscriptionNotification) NotificationOutput {
	output := NotificationOutput{NnwdafEventsSubscriptionNotification: notification}
	for _, event := range notification.EventNotifications {
		eventOutput := EventNotificationOutput{
			NwdafEventsSubscriptionEventNotification: event,
			Event:                                    string(event.Event),
		}
		for _, communication := range event.UeComms {
			mapped := UeCommunicationOutput{
				UeCommunication: communication,
				Confidence:      communication.Confidence,
			}
			if communication.TrafChar != nil {
				mapped.TrafChar = &TrafficCharacterizationOutput{
					TrafficCharacterization: *communication.TrafChar,
					UlVol:                   communication.TrafChar.UlVol,
					DlVol:                   communication.TrafChar.DlVol,
				}
			}
			eventOutput.UeComms = append(eventOutput.UeComms, mapped)
		}
		output.EventNotifications = append(output.EventNotifications, eventOutput)
	}
	return output
}
