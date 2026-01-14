package collector

import (
	"github.com/free5gc/nwdaf/internal/logger"
)

// HandleUpfNotification processes UPF event exposure notifications
// Per TS 29.564: UPF sends Nupf_EventExposure_Notify directly to NWDAF
func (c *CollectorContext) HandleUpfNotification(notif *UpfNotificationData) error {
	logger.CollectorLog.Infof("Processing UPF notification, items: %d, correlationId: %s",
		len(notif.NotificationItems), notif.CorrelationId)

	for i := range notif.NotificationItems {
		item := &notif.NotificationItems[i]
		if err := c.processUpfNotificationItem(item); err != nil {
			logger.CollectorLog.Warnf("Failed to process UPF notification item: %v", err)
		}
	}

	return nil
}

// processUpfNotificationItem handles a single UPF notification item
func (c *CollectorContext) processUpfNotificationItem(item *UpfNotificationItem) error {
	switch item.EventType {
	case UpfEventType_USER_DATA_USAGE_MEASURES:
		c.handleUserDataUsageMeasures(item)
	case UpfEventType_USER_DATA_USAGE_TRENDS:
		c.handleUserDataUsageTrends(item)
	case UpfEventType_QOS_MONITORING:
		logger.CollectorLog.Debugf("QOS_MONITORING event received (not implemented)")
	default:
		logger.CollectorLog.Debugf("Unhandled UPF event type: %s", item.EventType)
	}

	return nil
}

// handleUserDataUsageMeasures processes USER_DATA_USAGE_MEASURES events
// Extracts traffic volume and throughput data and updates UE communication data
func (c *CollectorContext) handleUserDataUsageMeasures(item *UpfNotificationItem) {
	supi := item.Supi
	if supi == "" {
		// SUPI not provided - cannot correlate with UE data
		logger.CollectorLog.Warnf("USER_DATA_USAGE_MEASURES without SUPI, ueIpv4=%s", item.UeIpv4Addr)
		return
	}

	data := c.GetOrCreateUeData(supi)

	// Process measurements
	for _, usage := range item.UserDataUsageMeasurements {
		// Volume measurement - aggregate totals
		if usage.VolumeMeasurement != nil {
			data.TotalUlVolume += usage.VolumeMeasurement.UlVolume
			data.TotalDlVolume += usage.VolumeMeasurement.DlVolume
			logger.CollectorLog.Infof("UPF VOLUME: supi=%s, ulVol=%d, dlVol=%d",
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
			logger.CollectorLog.Infof("UPF THROUGHPUT: supi=%s, ulTput=%s, dlTput=%s",
				supi, usage.ThroughputMeasurement.UlThroughput, usage.ThroughputMeasurement.DlThroughput)
		}
	}

	// Update session metadata if provided
	if item.Dnn != "" {
		data.Dnn = item.Dnn
	}
	if item.Snssai != nil {
		data.Snssai = item.Snssai
	}
	if item.RatType != "" {
		data.RatType = item.RatType
	}

	c.StoreUeData(data)
}

// handleUserDataUsageTrends processes USER_DATA_USAGE_TRENDS events
// Contains throughput statistics (not fully implemented)
func (c *CollectorContext) handleUserDataUsageTrends(item *UpfNotificationItem) {
	logger.CollectorLog.Debugf("USER_DATA_USAGE_TRENDS: supi=%s (throughput tracking not implemented)",
		item.Supi)
}
