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
	logger.ProcLog.Infof("Received SMF notification, notifId: %s, events: %d",
		notification.NotifId, len(notification.EventNotifs))

	ctx := nwdaf_context.GetSelf()
	correlationId := notification.NotifId

	for _, event := range notification.EventNotifs {
		if err := p.processSmfEvent(ctx, correlationId, &event); err != nil {
			logger.ProcLog.Errorf("Failed to process event: %v", err)
			continue
		}
	}

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
		logger.ProcLog.Warnf("Event missing SUPI, skipping")
		return nil
	}

	logger.ProcLog.Debugf("Processing SMF event: type=%s, supi=%s, pduSessId=%d",
		event.Event, supi, event.PduSeId)

	// Handle specific event types
	switch event.Event {
	case models.SmfEvent_PDU_SES_EST:
		p.handlePduSessionEstablished(ctx, correlationId, event)
	case models.SmfEvent_PDU_SES_REL:
		p.handlePduSessionReleased(ctx, correlationId, event)
	case models.SmfEvent_UP_STATUS_INFO:
		p.handleUpStatusInfo(ctx, correlationId, event)
	default:
		logger.ProcLog.Debugf("Unhandled event type: %s", event.Event)
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

	logger.ProcLog.Infof("PDU Session Established: supi=%s, pduSessId=%d, dnn=%s, time=%v",
		supi, event.PduSeId, event.Dnn, startTime)
}

// handlePduSessionReleased handles PDU session release events
func (p *Processor) handlePduSessionReleased(
	ctx *nwdaf_context.NWDAFContext,
	correlationId string,
	event *models.SmfEventExposureEventNotification,
) {
	supi := event.Supi

	var endTime time.Time
	if event.TimeStamp != nil {
		endTime = *event.TimeStamp
	} else {
		endTime = time.Now()
	}

	logger.ProcLog.Infof("PDU Session Released: supi=%s, pduSessId=%d, time=%v",
		supi, event.PduSeId, endTime)
}

// handleUpStatusInfo handles User Plane status information events
func (p *Processor) handleUpStatusInfo(
	ctx *nwdaf_context.NWDAFContext,
	correlationId string,
	event *models.SmfEventExposureEventNotification,
) {
	supi := event.Supi

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
				logger.ProcLog.Infof("UP Status ACTIVATED: supi=%s, time=%v", supi, eventTime)
			case models.SmfEventExposurePduSessionStatus_DEACTIVATED:
				logger.ProcLog.Infof("UP Status DEACTIVATED: supi=%s, time=%v", supi, eventTime)
			}
		}
	}
}
