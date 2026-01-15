package processor

import (
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

// HandleSmfNotification processes incoming SMF event exposure notifications
func (p *Processor) HandleSmfNotification(notification *models.NsmfEventExposureNotification) error {
	logger.ProcLog.Infof("Received SMF notification, notifId: %s, events: %d",
		notification.NotifId, len(notification.EventNotifs))

	ctx := nwdaf_context.GetSelf()

	for _, event := range notification.EventNotifs {
		if err := p.processSmfEvent(ctx, &event); err != nil {
			logger.ProcLog.Errorf("Failed to process event: %v", err)
			continue
		}
	}

	return nil
}

// processSmfEvent handles a single SMF event
func (p *Processor) processSmfEvent(ctx *nwdaf_context.NWDAFContext, event *models.SmfEventExposureEventNotification) error {
	supi := event.Supi
	if supi == "" {
		logger.ProcLog.Warnf("Event missing SUPI, skipping")
		return nil
	}

	logger.ProcLog.Debugf("Processing event: type=%s, supi=%s, pduSessId=%d",
		event.Event, supi, event.PduSeId)

	// Store the event
	ctx.AppendEvent(supi, *event)

	// Handle specific event types
	switch event.Event {
	case models.SmfEvent_PDU_SES_EST:
		p.handlePduSessionEstablished(ctx, event)
	case models.SmfEvent_PDU_SES_REL:
		p.handlePduSessionReleased(ctx, event)
	case models.SmfEvent_UP_STATUS_INFO:
		p.handleUpStatusInfo(ctx, event)
	default:
		logger.ProcLog.Debugf("Unhandled event type: %s", event.Event)
	}

	return nil
}

// handlePduSessionEstablished handles PDU session establishment events
func (p *Processor) handlePduSessionEstablished(ctx *nwdaf_context.NWDAFContext, event *models.SmfEventExposureEventNotification) {
	data := ctx.GetOrCreateUeData(event.Supi)
	data.Lock()
	defer data.Unlock()

	data.SessionCount++

	// Record session start time
	if event.TimeStamp != nil {
		data.CommStartTime = *event.TimeStamp
	} else {
		data.CommStartTime = time.Now()
	}
	data.IsActive = true

	// Store session metadata
	if event.Dnn != "" {
		data.Dnn = event.Dnn
	}
	if event.Snssai != nil {
		data.Snssai = event.Snssai
	}
	if event.RatType != "" {
		data.RatType = event.RatType
	}

	logger.ProcLog.Infof("PDU Session Established: supi=%s, pduSessId=%d, dnn=%s",
		event.Supi, event.PduSeId, event.Dnn)
}

// handlePduSessionReleased handles PDU session release events
func (p *Processor) handlePduSessionReleased(ctx *nwdaf_context.NWDAFContext, event *models.SmfEventExposureEventNotification) {
	data, ok := ctx.GetUeData(event.Supi)
	if !ok {
		logger.ProcLog.Warnf("PDU Session Released but no data found: supi=%s", event.Supi)
		return
	}

	data.Lock()
	defer data.Unlock()

	// Calculate session duration
	var endTime time.Time
	if event.TimeStamp != nil {
		endTime = *event.TimeStamp
	} else {
		endTime = time.Now()
	}

	if !data.CommStartTime.IsZero() {
		duration := endTime.Sub(data.CommStartTime)
		data.TotalCommDuration += duration
		logger.ProcLog.Infof("PDU Session Released: supi=%s, duration=%v", event.Supi, duration)
	}

	data.IsActive = false
}

// handleUpStatusInfo handles User Plane status information events
func (p *Processor) handleUpStatusInfo(ctx *nwdaf_context.NWDAFContext, event *models.SmfEventExposureEventNotification) {
	data := ctx.GetOrCreateUeData(event.Supi)
	data.Lock()
	defer data.Unlock()

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
				data.LastActivationTime = eventTime
				data.IsActive = true
				logger.ProcLog.Infof("UP Status ACTIVATED: supi=%s", event.Supi)
			case models.SmfEventExposurePduSessionStatus_DEACTIVATED:
				if !data.LastActivationTime.IsZero() {
					activeDuration := eventTime.Sub(data.LastActivationTime)
					data.TotalCommDuration += activeDuration
					logger.ProcLog.Infof("UP Status DEACTIVATED: supi=%s, duration=%v", event.Supi, activeDuration)
				}
				data.IsActive = false
			}
		}
	}
}
