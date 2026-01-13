// Package collector provides notification handling for SMF event exposure
package collector

import (
	"time"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

var collectorLog = logger.CollectorLog

// HandleNotification processes incoming SMF event exposure notifications
// This is called when SMF sends a POST to /collector/notify
func (c *CollectorContext) HandleNotification(notification *models.NsmfEventExposureNotification) error {
	collectorLog.Infof("Received SMF notification, notifId: %s, events: %d",
		notification.NotifId, len(notification.EventNotifs))

	for _, event := range notification.EventNotifs {
		if err := c.processEvent(&event); err != nil {
			collectorLog.Errorf("Failed to process event: %v", err)
			continue
		}
	}

	return nil
}

// processEvent handles a single SMF event
func (c *CollectorContext) processEvent(event *models.SmfEventExposureEventNotification) error {
	supi := event.Supi
	if supi == "" {
		collectorLog.Warnf("Event missing SUPI, skipping")
		return nil
	}

	collectorLog.Debugf("Processing event: type=%s, supi=%s, pduSessId=%d",
		event.Event, supi, event.PduSeId)

	// Store the event
	c.AppendEvent(supi, *event)

	// Handle specific event types
	switch event.Event {
	case models.SmfEvent_PDU_SES_EST:
		// Session lifecycle - get DNN, S-NSSAI, start time
		c.handlePduSessionEstablished(event)
	case models.SmfEvent_PDU_SES_REL:
		// Session lifecycle - calculate session duration
		c.handlePduSessionReleased(event)
	case models.SmfEvent_UP_STATUS_INFO:
		// User Plane status - track ACTIVATED/DEACTIVATED for commDur
		c.handleUpStatusInfo(event)
	default:
		collectorLog.Debugf("Unhandled event type: %s", event.Event)
	}

	return nil
}

// handlePduSessionEstablished handles PDU session establishment events
// Provides: DNN, S-NSSAI, RAT Type, UE IP Address
func (c *CollectorContext) handlePduSessionEstablished(event *models.SmfEventExposureEventNotification) {
	data := c.GetOrCreateUeData(event.Supi)
	data.SessionCount++

	// Record session start time from event timestamp
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

	c.StoreUeData(data)

	collectorLog.Infof("PDU Session Established: supi=%s, pduSessId=%d, dnn=%s, snssai=%v, ratType=%s",
		event.Supi, event.PduSeId, event.Dnn, event.Snssai, event.RatType)
}

// handlePduSessionReleased handles PDU session release events
// Calculate communication duration from session lifecycle
func (c *CollectorContext) handlePduSessionReleased(event *models.SmfEventExposureEventNotification) {
	data, ok := c.GetUeData(event.Supi)
	if !ok {
		collectorLog.Warnf("PDU Session Released but no data found: supi=%s", event.Supi)
		return
	}

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
		collectorLog.Infof("PDU Session Released: supi=%s, duration=%v", event.Supi, duration)
	}

	data.IsActive = false
	c.StoreUeData(data)
}

// handleUpStatusInfo handles User Plane status information events
// Key for calculating communication duration (commDur)
// Tracks ACTIVATED/DEACTIVATED transitions to measure actual communication time
func (c *CollectorContext) handleUpStatusInfo(event *models.SmfEventExposureEventNotification) {
	data := c.GetOrCreateUeData(event.Supi)

	// Track timestamp for duration calculation
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
				// User Plane activated - record activation time
				data.LastActivationTime = eventTime
				data.IsActive = true
				collectorLog.Infof("UP Status ACTIVATED: supi=%s, pduSessId=%d", event.Supi, pduInfo.PduSessId)
			case models.SmfEventExposurePduSessionStatus_DEACTIVATED:
				// User Plane deactivated - calculate active duration
				if !data.LastActivationTime.IsZero() {
					activeDuration := eventTime.Sub(data.LastActivationTime)
					data.TotalCommDuration += activeDuration
					collectorLog.Infof("UP Status DEACTIVATED: supi=%s, pduSessId=%d, duration=%v",
						event.Supi, pduInfo.PduSessId, activeDuration)
				}
				data.IsActive = false
			}
		}
	}

	c.StoreUeData(data)
}
