package processor

import (
	"context"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
	"github.com/free5gc/util/mongoapi"
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
	UeIpv6Prefix              string                      `json:"ueIpv6Prefix,omitempty"`
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
// Unified handler for both SUPI-based and Group ID subscriptions
// Uses two-layer bucket storage: correlationId → TrafficDataBucket → ipAddress → TrafficData
func (p *Processor) HandleUpfNotification(notif *UpfNotificationData) error {
	logger.ProcLog.Infof("Processing UPF notification, items: %d, correlationId: %s",
		len(notif.NotificationItems), notif.CorrelationId)

	if notif.CorrelationId == "" {
		logger.ProcLog.Warnf("UPF notification without correlationId, cannot route")
		return nil
	}

	ctx := nwdaf_context.GetSelf()
	correlationId := notif.CorrelationId

	// Get or create bucket for this correlation ID
	bucket := ctx.GetOrCreateTrafficBucket(correlationId)

	// Update SMF subscription's last seen time if available
	if sub := ctx.GetSmfSubscription(correlationId); sub != nil {
		sub.UpdateLastSeen()
	}

	// Process each notification item into unified storage
	for i := range notif.NotificationItems {
		item := &notif.NotificationItems[i]
		p.processUpfNotificationItemUnified(ctx, bucket, item)
	}

	return nil
}

// processUpfNotificationItemUnified handles a single UPF notification item
// using the unified bucket-based storage
func (p *Processor) processUpfNotificationItemUnified(
	ctx *nwdaf_context.NWDAFContext,
	bucket *nwdaf_context.TrafficDataBucket,
	item *UpfNotificationItem,
) {
	// Get IP address (required field per TS 29.564)
	ipAddr := item.UeIpv4Addr
	if ipAddr == "" {
		ipAddr = item.UeIpv6Prefix
	}
	if ipAddr == "" {
		logger.ProcLog.Warnf("UPF notification item without IP address, skipping")
		return
	}

	// Get or create TrafficData for this IP
	data := bucket.GetOrCreate(ipAddr)

	data.Lock()
	defer data.Unlock()

	// Enrich with SUPI if available
	if item.Supi != "" && data.Supi == "" {
		data.Supi = item.Supi
		logger.ProcLog.Debugf("Enriched IP %s with SUPI %s", ipAddr, item.Supi)
	}

	// Enrich session metadata
	if item.Dnn != "" {
		data.Dnn = item.Dnn
	}
	if item.Snssai != nil {
		data.Snssai = item.Snssai
	}
	if item.RatType != "" {
		data.RatType = item.RatType
	}

	// Get GroupId if available from the original subscription resource tracking
	groupId := ctx.GetGroupIdByCorrelationId(bucket.CorrelationId)

	// Process Measurements and save to MongoDB
	for _, usage := range item.UserDataUsageMeasurements {
		dataPoint := nwdaf_context.UpfDataPoint{
			Timestamp: item.TimeStamp,
		}

		record := nwdaf_context.UpfTrafficRecord{
			Metadata: nwdaf_context.UpfTrafficMetaData{
				IpAddr:        ipAddr,
				CorrelationId: bucket.CorrelationId,
				Supi:          item.Supi,
				GroupId:       groupId,
				Dnn:           item.Dnn,
			},
			Timestamp: item.TimeStamp,
		}

		if usage.VolumeMeasurement != nil {
			dataPoint.UlVolume = usage.VolumeMeasurement.UlVolume
			dataPoint.DlVolume = usage.VolumeMeasurement.DlVolume
			record.UlVolume = usage.VolumeMeasurement.UlVolume
			record.DlVolume = usage.VolumeMeasurement.DlVolume
			logger.ProcLog.Infof("UPF VOLUME: ip=%s, ulVol=%d, dlVol=%d",
				ipAddr, usage.VolumeMeasurement.UlVolume, usage.VolumeMeasurement.DlVolume)
		}

		if usage.ThroughputMeasurement != nil {
			dataPoint.UlThroughput = usage.ThroughputMeasurement.UlThroughput
			dataPoint.DlThroughput = usage.ThroughputMeasurement.DlThroughput
			record.UlThroughput = usage.ThroughputMeasurement.UlThroughput
			record.DlThroughput = usage.ThroughputMeasurement.DlThroughput
			logger.ProcLog.Infof("UPF THROUGHPUT: ip=%s, ulTput=%s, dlTput=%s",
				ipAddr, usage.ThroughputMeasurement.UlThroughput, usage.ThroughputMeasurement.DlThroughput)
		}

		data.RawUpfData = append(data.RawUpfData, dataPoint)

		// Save to MongoDB natively using mongo-driver (if configured and connected)
		if factory.NwdafConfig != nil && factory.NwdafConfig.Configuration != nil && factory.NwdafConfig.Configuration.Mongodb != nil && mongoapi.Client != nil {
			dbName := factory.NwdafConfig.Configuration.Mongodb.Name
			coll := mongoapi.Client.Database(dbName).Collection(nwdaf_context.UpfTrafficDataColl)
			if _, err := coll.InsertOne(context.Background(), record); err != nil {
				logger.ProcLog.Errorf("Failed to save UPF TimeSeries data: %v", err)
			}
		}
	}

	data.LastUpdate = item.TimeStamp
}
