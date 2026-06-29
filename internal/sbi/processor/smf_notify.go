package processor

import (
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

// HandleSmfNotification processes incoming SMF event exposure notifications
// Note: SMF events provide session lifecycle info (establish/release)
// Traffic volume data comes from UPF notifications
func (p *Processor) HandleSmfNotification(notification *models.NsmfEventExposureNotification) error {
	ctx := nwdaf_context.GetSelf()
	correlationId := notification.NotifId
	failures := 0

	for i := range notification.EventNotifs {
		event := &notification.EventNotifs[i]
		if err := p.processSmfEvent(ctx, correlationId, event); err != nil {
			failures++
			logger.ProcLog.Errorf("SmfNotification: event failed notifId=%s err=%v", correlationId, err)
			continue
		}
	}

	logger.ProcLog.Infof("SmfNotification: processed notifId=%s events=%d failures=%d",
		correlationId, len(notification.EventNotifs), failures)
	return nil
}

// processSmfEvent handles a single SMF event
// Updates TrafficData with session metadata from SMF events
func (p *Processor) processSmfEvent(
	ctx *nwdaf_context.NWDAFContext,
	correlationId string,
	event *models.SmfEventExposureEventNotification,
) error {
	supi := event.Supi
	if supi == "" {
		logger.ProcLog.Warnf("SmfNotification: missing supi notifId=%s pduSessId=%d", correlationId, event.PduSeId)
		return nil
	}

	logger.ProcLog.Debugf("SmfNotificationEvent: notifId=%s event=%s pduSessId=%d",
		correlationId, event.Event, event.PduSeId)

	// Handle specific event types
	switch event.Event {
	case models.SmfEvent_PDU_SES_EST:
		p.handlePduSessionEstablished(ctx, correlationId, event)
	case models.SmfEvent_PDU_SES_REL:
		p.handlePduSessionReleased(ctx, correlationId, event)
	case models.SmfEvent_UP_STATUS_INFO:
		p.handleUpStatusInfo(ctx, correlationId, event)
	default:
		logger.ProcLog.Debugf("SmfNotificationEvent: notifId=%s unhandled=%s", correlationId, event.Event)
	}

	return nil
}

// handlePduSessionEstablished handles PDU session establishment events
// Enriches TrafficData if available, creates connection metadata
func (p *Processor) handlePduSessionEstablished(
	ctx *nwdaf_context.NWDAFContext,
	correlationId string,
	event *models.SmfEventExposureEventNotification,
) {
	supi := event.Supi

	// If we have a correlation ID, try to enrich associated TrafficData
	if correlationId != "" {
		if bucket := ctx.GetTrafficBucket(correlationId); bucket != nil {
			for _, data := range bucket.GetAll() {
				data.Lock()
				// Enrich with session metadata
				if event.Dnn != "" {
					data.Dnn = event.Dnn
				}
				if event.Snssai != nil {
					data.Snssai = event.Snssai
				}
				if event.RatType != "" {
					data.RatType = event.RatType
				}
				// Associate SUPI with this traffic data
				if data.Supi == "" && supi != "" {
					data.Supi = supi
				}
				data.Unlock()
			}
		}
	}

	var startTime time.Time
	if event.TimeStamp != nil {
		startTime = *event.TimeStamp
	} else {
		startTime = time.Now()
	}

	logger.ProcLog.Debugf("SmfNotificationEvent: notifId=%s event=%s pduSessId=%d time=%s",
		correlationId, event.Event, event.PduSeId, startTime.UTC().Format(time.RFC3339))
}

// handlePduSessionReleased handles PDU session release events
func (p *Processor) handlePduSessionReleased(
	ctx *nwdaf_context.NWDAFContext,
	correlationId string,
	event *models.SmfEventExposureEventNotification,
) {
	var endTime time.Time
	if event.TimeStamp != nil {
		endTime = *event.TimeStamp
	} else {
		endTime = time.Now()
	}

	logger.ProcLog.Debugf("SmfNotificationEvent: notifId=%s event=%s pduSessId=%d time=%s",
		correlationId, event.Event, event.PduSeId, endTime.UTC().Format(time.RFC3339))
}

// handleUpStatusInfo handles User Plane status information events
func (p *Processor) handleUpStatusInfo(
	ctx *nwdaf_context.NWDAFContext,
	correlationId string,
	event *models.SmfEventExposureEventNotification,
) {
	var eventTime time.Time
	if event.TimeStamp != nil {
		eventTime = *event.TimeStamp
	} else {
		eventTime = time.Now()
	}

	// Process UP status from pduSessInfos
	for _, pduInfo := range event.PduSessInfos {
		if pduInfo.SessInfo != nil {
			status := pduInfo.SessInfo.PduSessStatus
			switch status {
			case models.SmfEventExposurePduSessionStatus_ACTIVATED:
				logger.ProcLog.Debugf("SmfNotificationEvent: notifId=%s status=%s time=%s",
					correlationId, status, eventTime.UTC().Format(time.RFC3339))
			case models.SmfEventExposurePduSessionStatus_DEACTIVATED:
				logger.ProcLog.Debugf("SmfNotificationEvent: notifId=%s status=%s time=%s",
					correlationId, status, eventTime.UTC().Format(time.RFC3339))
			}
		}
	}
}
