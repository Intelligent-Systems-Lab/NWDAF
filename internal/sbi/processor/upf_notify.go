package processor

import (
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

// UPF Event Types based on TS 29.564
type UpfEventType string

const (
	UpfEventType_QOS_MONITORING           UpfEventType = "QOS_MONITORING"
	UpfEventType_USER_DATA_USAGE_MEASURES UpfEventType = "USER_DATA_USAGE_MEASURES"
	UpfEventType_USER_DATA_USAGE_TRENDS   UpfEventType = "USER_DATA_USAGE_TRENDS"
)

// UpfNotificationData wraps UPF notification items (TS 29.564)
type UpfNotificationData struct {
	NotificationItems []UpfNotificationItem `json:"notificationItems"`
	CorrelationId     string                `json:"correlationId,omitempty"`
	EventNotifyUri    string                `json:"eventNotifyUri,omitempty"`
}

// UpfNotificationItem represents a single UPF event report
type UpfNotificationItem struct {
	EventType                 UpfEventType                `json:"eventType"`
	UeIpv4Addr                string                      `json:"ueIpv4Addr,omitempty"`
	Supi                      string                      `json:"supi,omitempty"`
	Dnn                       string                      `json:"dnn,omitempty"`
	Snssai                    *models.Snssai              `json:"snssai,omitempty"`
	TimeStamp                 time.Time                   `json:"timeStamp"`
	RatType                   models.RatType              `json:"ratType,omitempty"`
	UserDataUsageMeasurements []UserDataUsageMeasurements `json:"userDataUsageMeasurements,omitempty"`
}

// UserDataUsageMeasurements contains traffic volume data
type UserDataUsageMeasurements struct {
	VolumeMeasurement     *VolumeMeasurement     `json:"volumeMeasurement,omitempty"`
	ThroughputMeasurement *ThroughputMeasurement `json:"throughputMeasurement,omitempty"`
}

// VolumeMeasurement contains UL/DL volume information
type VolumeMeasurement struct {
	TotalVolume int64 `json:"totalVolume,omitempty"`
	UlVolume    int64 `json:"ulVolume,omitempty"`
	DlVolume    int64 `json:"dlVolume,omitempty"`
}

// ThroughputMeasurement contains throughput information
type ThroughputMeasurement struct {
	UlThroughput string `json:"ulThroughput,omitempty"`
	DlThroughput string `json:"dlThroughput,omitempty"`
}

// HandleUpfNotification processes UPF event exposure notifications
func (p *Processor) HandleUpfNotification(notif *UpfNotificationData) error {
	logger.ProcLog.Infof("Processing UPF notification, items: %d, correlationId: %s",
		len(notif.NotificationItems), notif.CorrelationId)

	ctx := nwdaf_context.GetSelf()

	for i := range notif.NotificationItems {
		item := &notif.NotificationItems[i]
		if err := p.processUpfNotificationItem(ctx, item); err != nil {
			logger.ProcLog.Warnf("Failed to process UPF notification item: %v", err)
		}
	}

	return nil
}

// processUpfNotificationItem handles a single UPF notification item
func (p *Processor) processUpfNotificationItem(ctx *nwdaf_context.NWDAFContext, item *UpfNotificationItem) error {
	switch item.EventType {
	case UpfEventType_USER_DATA_USAGE_MEASURES:
		p.handleUserDataUsageMeasures(ctx, item)
	case UpfEventType_USER_DATA_USAGE_TRENDS:
		logger.ProcLog.Debugf("USER_DATA_USAGE_TRENDS: supi=%s (not implemented)", item.Supi)
	default:
		logger.ProcLog.Debugf("Unhandled UPF event type: %s", item.EventType)
	}

	return nil
}

// handleUserDataUsageMeasures processes USER_DATA_USAGE_MEASURES events
func (p *Processor) handleUserDataUsageMeasures(ctx *nwdaf_context.NWDAFContext, item *UpfNotificationItem) {
	supi := item.Supi
	if supi == "" {
		logger.ProcLog.Warnf("USER_DATA_USAGE_MEASURES without SUPI, ueIpv4=%s", item.UeIpv4Addr)
		return
	}

	data := ctx.GetOrCreateUeData(supi)
	data.Lock()
	defer data.Unlock()

	// Process measurements
	for _, usage := range item.UserDataUsageMeasurements {
		// Volume measurement - aggregate totals
		if usage.VolumeMeasurement != nil {
			data.TotalUlVolume += usage.VolumeMeasurement.UlVolume
			data.TotalDlVolume += usage.VolumeMeasurement.DlVolume
			logger.ProcLog.Infof("UPF VOLUME: supi=%s, ulVol=%d, dlVol=%d",
				supi, usage.VolumeMeasurement.UlVolume, usage.VolumeMeasurement.DlVolume)
		}

		// Throughput measurement - store latest values
		if usage.ThroughputMeasurement != nil {
			if usage.ThroughputMeasurement.UlThroughput != "" {
				data.LastUlThroughput = usage.ThroughputMeasurement.UlThroughput
			}
			if usage.ThroughputMeasurement.DlThroughput != "" {
				data.LastDlThroughput = usage.ThroughputMeasurement.DlThroughput
			}
			logger.ProcLog.Infof("UPF THROUGHPUT: supi=%s, ulTput=%s, dlTput=%s",
				supi, usage.ThroughputMeasurement.UlThroughput, usage.ThroughputMeasurement.DlThroughput)
		}
	}

	// Update session metadata
	if item.Dnn != "" {
		data.Dnn = item.Dnn
	}
	if item.Snssai != nil {
		data.Snssai = item.Snssai
	}
	if item.RatType != "" {
		data.RatType = item.RatType
	}
}
