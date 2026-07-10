package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

func (d *ReportDispatcher) DispatchAnalyticsReport(
	subscriptionID string,
	report *contract.AnalyticsReport,
) error {
	ctx := nwdaf_context.GetSelf()
	if ctx == nil {
		return fmt.Errorf("%w: context unavailable", ErrExternalDelivery)
	}
	subscription := ctx.GetSubscription(subscriptionID)
	if subscription == nil {
		return ErrSubscriptionNotFound
	}
	if report == nil || report.ReportID == "" || len(report.EventNotifications) == 0 {
		return ErrInvalidAnalyticsReport
	}
	duplicate, inFlight, stale := subscription.BeginReport(
		report.ReportID,
		report.RuntimeRevision,
		report.ReportSequence,
	)
	if stale {
		return ErrStaleAnalyticsReport
	}
	if inFlight {
		return ErrAnalyticsReportInFlight
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
		return fmt.Errorf("%w: marshal notification: %v", ErrInvalidAnalyticsReport, err)
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
		return fmt.Errorf("%w: create request: %v", ErrExternalDelivery, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrExternalDelivery, err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.NotifierLog.Debugf("Failed to close analytics delivery response: %v", closeErr)
		}
	}()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%w: status=%d", ErrExternalDelivery, resp.StatusCode)
	}
	delivered = true
	if d.recorder != nil {
		d.recorder.RecordAnalyticsReport(subscriptionID, report)
	}
	return nil
}

func mapAnalyticsReport(
	subscription *nwdaf_context.Subscription,
	report *contract.AnalyticsReport,
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
			return NotificationOutput{}, fmt.Errorf("%w: unsupported event %q", ErrInvalidAnalyticsReport, event.Event)
		}
		if _, ok := requestedEvents[eventType]; !ok {
			return NotificationOutput{}, fmt.Errorf("%w: event %q not requested", ErrInvalidAnalyticsReport, event.Event)
		}
		if len(event.UeCommunications) == 0 {
			return NotificationOutput{}, fmt.Errorf(
				"%w: UE communication report is empty",
				ErrInvalidAnalyticsReport,
			)
		}
		mapped := models.NwdafEventsSubscriptionEventNotification{Event: eventType}
		for _, communication := range event.UeCommunications {
			if communication.CommunicationDuration < 0 || communication.Timestamp.IsZero() ||
				communication.Confidence < 0 || communication.Confidence > 100 {
				return NotificationOutput{}, fmt.Errorf(
					"%w: invalid UE communication report",
					ErrInvalidAnalyticsReport,
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
