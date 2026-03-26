package processor

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
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
	StartTime                 time.Time                   `json:"startTime"`
	RatType                   models.RatType              `json:"ratType,omitempty"`
	UserDataUsageMeasurements []UserDataUsageMeasurements `json:"userDataUsageMeasurements,omitempty"`
}

// UserDataUsageMeasurements contains traffic volume data
type UserDataUsageMeasurements struct {
	VolumeMeasurement     *VolumeMeasurement     `json:"volumeMeasurement,omitempty"`
	ThroughputMeasurement *ThroughputMeasurement `json:"throughputMeasurement,omitempty"`
}

// VolumeMeasurement contains volume information per TS 29.564
type VolumeMeasurement struct {
	TotalVolume      int64  `json:"totalVolume,omitempty"`
	UlVolume         int64  `json:"ulVolume,omitempty"`
	DlVolume         int64  `json:"dlVolume,omitempty"`
	TotalNbOfPackets uint64 `json:"totalNbOfPackets,omitempty"`
	UlNbOfPackets    uint64 `json:"ulNbOfPackets,omitempty"`
	DlNbOfPackets    uint64 `json:"dlNbOfPackets,omitempty"`
}

// ThroughputMeasurement contains throughput information per TS 29.564
// BitRate and PacketRate are both string types per TS29571 CommonData
type ThroughputMeasurement struct {
	UlThroughput       string `json:"ulThroughput,omitempty"`
	DlThroughput       string `json:"dlThroughput,omitempty"`
	UlPacketThroughput string `json:"ulPacketThroughput,omitempty"`
	DlPacketThroughput string `json:"dlPacketThroughput,omitempty"`
}

// parsePacketRate parses a TS29571 PacketRate string into pps (base unit).
// Pattern: '<number> (pps|kpps|Mpps|Gpps|Tpps)'
// k = ×1000, M = ×1000², G = ×1000³, T = ×1000⁴
// Returns 0 on empty input or parse error.
func parsePacketRate(s string) float64 {
	if s == "" {
		return 0
	}
	parts := strings.SplitN(s, " ", 2)
	v, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		logger.ProcLog.Warnf("Failed to parse PacketRate value %q: %v", s, err)
		return 0
	}
	if len(parts) < 2 {
		return v
	}
	switch parts[1] {
	case "pps":
		return v
	case "kpps":
		return v * 1e3
	case "Mpps":
		return v * 1e6
	case "Gpps":
		return v * 1e9
	case "Tpps":
		return v * 1e12
	default:
		logger.ProcLog.Warnf("Unknown PacketRate unit in %q, treating as pps", s)
		return v
	}
}

// parseBitRate parses a TS29571 BitRate string into bps (base unit).
// Pattern: '<number> (bps|Kbps|Mbps|Gbps|Tbps)'
// K = ×1000 (note: spec uses uppercase K, unlike SI kilo)
// Returns 0 on empty input or parse error.
func parseBitRate(s string) float64 {
	if s == "" {
		return 0
	}
	parts := strings.SplitN(s, " ", 2)
	v, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		logger.ProcLog.Warnf("Failed to parse BitRate value %q: %v", s, err)
		return 0
	}
	if len(parts) < 2 {
		return v
	}
	switch parts[1] {
	case "bps":
		return v
	case "Kbps":
		return v * 1e3
	case "Mbps":
		return v * 1e6
	case "Gbps":
		return v * 1e9
	case "Tbps":
		return v * 1e12
	default:
		logger.ProcLog.Warnf("Unknown BitRate unit in %q, treating as bps", s)
		return v
	}
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

	// Forward to ADRF buffer if configured.
	// Each NotificationItem is stored as a separate ADRF record so that
	// ADRF time-window filtering can match individual startTime values precisely.
	if p.adrfBuffer != nil {
		if info := ctx.GetAdrfSmfInfo(correlationId); info != nil {
			for i := range notif.NotificationItems {
				singleNotif := UpfNotificationData{
					CorrelationId:     notif.CorrelationId,
					EventNotifyUri:    notif.EventNotifyUri,
					NotificationItems: []UpfNotificationItem{notif.NotificationItems[i]},
				}
				if notifJSON, err := json.Marshal(singleNotif); err == nil {
					p.adrfBuffer.add(info, notifJSON)
				} else {
					logger.ProcLog.Warnf("Failed to marshal UPF notification item for ADRF: %v", err)
				}
			}
		}
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

	// Use startTime as the measurement timestamp (per TS 29.564: startTime is the
	// beginning of the measurement period). Fall back to timeStamp if startTime is absent.
	measurementTs := item.StartTime
	if measurementTs.IsZero() {
		logger.ProcLog.Debugf("UPF notification missing startTime, falling back to timeStamp (ip=%s)", ipAddr)
		measurementTs = item.TimeStamp
	}

	// Process Measurements and save to MongoDB
	for _, usage := range item.UserDataUsageMeasurements {
		dataPoint := nwdaf_context.UpfDataPoint{
			Timestamp: measurementTs,
		}

		record := nwdaf_context.UpfTrafficRecord{
			Metadata: nwdaf_context.UpfTrafficMetaData{
				IpAddr:        ipAddr,
				CorrelationId: bucket.CorrelationId,
				Supi:          item.Supi,
				GroupId:       groupId,
				Dnn:           item.Dnn,
			},
			Timestamp: measurementTs,
		}

		if usage.VolumeMeasurement != nil {
			v := usage.VolumeMeasurement
			dataPoint.TotalVolume = v.TotalVolume
			dataPoint.UlVolume = v.UlVolume
			dataPoint.DlVolume = v.DlVolume
			dataPoint.TotalNbOfPackets = v.TotalNbOfPackets
			dataPoint.UlNbOfPackets = v.UlNbOfPackets
			dataPoint.DlNbOfPackets = v.DlNbOfPackets
			record.TotalVolume = v.TotalVolume
			record.UlVolume = v.UlVolume
			record.DlVolume = v.DlVolume
			record.TotalNbOfPackets = v.TotalNbOfPackets
			record.UlNbOfPackets = v.UlNbOfPackets
			record.DlNbOfPackets = v.DlNbOfPackets
			logger.ProcLog.Infof("UPF VOLUME: ip=%s, startTime=%s, total=%d, ul=%d, dl=%d, totalPkts=%d, ulPkts=%d, dlPkts=%d",
				ipAddr, measurementTs.UTC().Format(time.RFC3339),
				v.TotalVolume, v.UlVolume, v.DlVolume,
				v.TotalNbOfPackets, v.UlNbOfPackets, v.DlNbOfPackets)
		}

		if usage.ThroughputMeasurement != nil {
			t := usage.ThroughputMeasurement
			dataPoint.UlThroughput = parseBitRate(t.UlThroughput)
			dataPoint.DlThroughput = parseBitRate(t.DlThroughput)
			dataPoint.UlPacketThroughput = parsePacketRate(t.UlPacketThroughput)
			dataPoint.DlPacketThroughput = parsePacketRate(t.DlPacketThroughput)
			record.UlThroughput = parseBitRate(t.UlThroughput)
			record.DlThroughput = parseBitRate(t.DlThroughput)
			record.UlPacketThroughput = parsePacketRate(t.UlPacketThroughput)
			record.DlPacketThroughput = parsePacketRate(t.DlPacketThroughput)
			logger.ProcLog.Infof("UPF THROUGHPUT: ip=%s, ul=%s(%.0fbps), dl=%s(%.0fbps)",
				ipAddr, t.UlThroughput, dataPoint.UlThroughput, t.DlThroughput, dataPoint.DlThroughput)
			logger.ProcLog.Infof("UPF THROUGHPUT: ip=%s, ulPktRate=%s(%.2fpps), dlPktRate=%s(%.2fpps)",
				ipAddr, t.UlPacketThroughput, dataPoint.UlPacketThroughput,
				t.DlPacketThroughput, dataPoint.DlPacketThroughput)
		}

		// Save to MongoDB for ground truth lookup by the accuracy monitor.
		if nwdaf_context.IsMongoAvailable() &&
			factory.NwdafConfig != nil && factory.NwdafConfig.Configuration != nil &&
			factory.NwdafConfig.Configuration.Mongodb != nil {
			dbName := factory.NwdafConfig.Configuration.Mongodb.Name
			coll := mongoapi.Client.Database(dbName).Collection(nwdaf_context.UpfTrafficDataColl)
			if _, err := coll.InsertOne(context.Background(), record); err != nil {
				logger.ProcLog.Errorf("Failed to save UPF TimeSeries data: %v", err)
			}
		}

		data.RawUpfData = append(data.RawUpfData, dataPoint)
		ringBufferSize := factory.NwdafConfig.GetRingBufferSize()
		if len(data.RawUpfData) > ringBufferSize {
			drop := len(data.RawUpfData) - ringBufferSize
			data.RawUpfData = data.RawUpfData[drop:]
		}
	}

	data.LastUpdate = measurementTs
}
