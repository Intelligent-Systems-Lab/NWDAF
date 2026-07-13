package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
	"github.com/free5gc/util/mongoapi"
)

// UPF Event Types based on TS 29.564
type UpfEventType string

const (
	UpfEventType_QOS_MONITORING           UpfEventType = "QOS_MONITORING"
	UpfEventType_USER_DATA_USAGE_MEASURES UpfEventType = "USER_DATA_USAGE_MEASURES"
	UpfEventType_USER_DATA_USAGE_TRENDS   UpfEventType = "USER_DATA_USAGE_TRENDS"
	upfMongoWriteTimeout                               = 5 * time.Second
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
// Returns 0 on empty input.
func parsePacketRate(s string) (float64, error) {
	if s == "" {
		return 0, nil
	}
	parts := strings.SplitN(s, " ", 2)
	v, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return 0, fmt.Errorf("parse packet rate %q: %w", s, err)
	}
	if len(parts) < 2 {
		return v, nil
	}
	switch parts[1] {
	case "pps":
		return v, nil
	case "kpps":
		return v * 1e3, nil
	case "Mpps":
		return v * 1e6, nil
	case "Gpps":
		return v * 1e9, nil
	case "Tpps":
		return v * 1e12, nil
	default:
		return v, fmt.Errorf("unknown packet rate unit in %q", s)
	}
}

// parseBitRate parses a TS29571 BitRate string into bps (base unit).
// Pattern: '<number> (bps|Kbps|Mbps|Gbps|Tbps)'
// K = ×1000 (note: spec uses uppercase K, unlike SI kilo)
// Returns 0 on empty input.
func parseBitRate(s string) (float64, error) {
	if s == "" {
		return 0, nil
	}
	parts := strings.SplitN(s, " ", 2)
	v, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return 0, fmt.Errorf("parse bit rate %q: %w", s, err)
	}
	if len(parts) < 2 {
		return v, nil
	}
	switch parts[1] {
	case "bps":
		return v, nil
	case "Kbps":
		return v * 1e3, nil
	case "Mbps":
		return v * 1e6, nil
	case "Gbps":
		return v * 1e9, nil
	case "Tbps":
		return v * 1e12, nil
	default:
		return v, fmt.Errorf("unknown bit rate unit in %q", s)
	}
}

// HandleUpfNotification processes UPF event exposure notifications
// Unified handler for both SUPI-based and Group ID subscriptions
// Uses two-layer bucket storage: correlationId → TrafficDataBucket → ipAddress → TrafficData
func (p *Processor) HandleUpfNotification(notif *UpfNotificationData) error {
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

	processed := 0
	malformed := 0
	observations := make([]contract.SourceObservation, 0)

	// Process each notification item into unified storage
	for i := range notif.NotificationItems {
		item := &notif.NotificationItems[i]
		itemProcessed, itemMalformed, itemObservations := p.processUpfNotificationItemUnified(ctx, bucket, item)
		if itemProcessed {
			processed++
		}
		malformed += itemMalformed
		observations = append(observations, itemObservations...)
	}
	if len(observations) > 0 {
		if !ctx.HasActiveNwdafSubscriber(correlationId) {
			logger.ProcLog.Debugf(
				"UpfNotification: skip AnLF enqueue without active subscriber corr=%s",
				correlationId,
			)
		} else if !p.anlf.EnqueueObservations(correlationId, observations) {
			logger.ProcLog.Warnf("UpfNotification: observation enqueue failed corr=%s", correlationId)
		}
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

	logger.ProcLog.Infof("UpfNotification: processed corr=%s items=%d stored=%d malformed=%d",
		correlationId, len(notif.NotificationItems), processed, malformed)
	return nil
}

// processUpfNotificationItemUnified handles a single UPF notification item
// using the unified bucket-based storage
func (p *Processor) processUpfNotificationItemUnified(
	ctx *nwdaf_context.NWDAFContext,
	bucket *nwdaf_context.TrafficDataBucket,
	item *UpfNotificationItem,
) (bool, int, []contract.SourceObservation) {
	cfg := p.config()

	// Get IP address (required field per TS 29.564)
	ipAddr := item.UeIpv4Addr
	if ipAddr == "" {
		ipAddr = item.UeIpv6Prefix
	}
	if ipAddr == "" {
		logger.ProcLog.Warnf("UpfNotificationItem: missing ip corr=%s", bucket.CorrelationId)
		return false, 0, nil
	}

	// Get or create TrafficData for this IP
	data := bucket.GetOrCreate(ipAddr)

	data.Lock()
	defer data.Unlock()

	// Enrich with SUPI if available
	if item.Supi != "" && data.Supi == "" {
		data.Supi = item.Supi
		logger.ProcLog.Debugf("UpfNotificationItem: enriched corr=%s", bucket.CorrelationId)
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
		logger.ProcLog.Debugf("UpfNotificationItem: missing startTime corr=%s", bucket.CorrelationId)
		measurementTs = item.TimeStamp
	}

	// Process Measurements and save to MongoDB
	malformed := 0
	observations := make([]contract.SourceObservation, 0, len(item.UserDataUsageMeasurements))
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
			logger.ProcLog.Debugf("UpfVolume: corr=%s start=%s total=%d ul=%d dl=%d",
				bucket.CorrelationId, measurementTs.UTC().Format(time.RFC3339),
				v.TotalVolume, v.UlVolume, v.DlVolume)
		}

		if usage.ThroughputMeasurement != nil {
			t := usage.ThroughputMeasurement
			var parseErrs []error
			ulThroughput, parseErr := parseBitRate(t.UlThroughput)
			if parseErr != nil {
				parseErrs = append(parseErrs, fmt.Errorf("ulThroughput: %w", parseErr))
			}
			dlThroughput, parseErr := parseBitRate(t.DlThroughput)
			if parseErr != nil {
				parseErrs = append(parseErrs, fmt.Errorf("dlThroughput: %w", parseErr))
			}
			ulPacketThroughput, parseErr := parsePacketRate(t.UlPacketThroughput)
			if parseErr != nil {
				parseErrs = append(parseErrs, fmt.Errorf("ulPacketThroughput: %w", parseErr))
			}
			dlPacketThroughput, parseErr := parsePacketRate(t.DlPacketThroughput)
			if parseErr != nil {
				parseErrs = append(parseErrs, fmt.Errorf("dlPacketThroughput: %w", parseErr))
			}
			dataPoint.UlThroughput = ulThroughput
			dataPoint.DlThroughput = dlThroughput
			dataPoint.UlPacketThroughput = ulPacketThroughput
			dataPoint.DlPacketThroughput = dlPacketThroughput
			record.UlThroughput = dataPoint.UlThroughput
			record.DlThroughput = dataPoint.DlThroughput
			record.UlPacketThroughput = dataPoint.UlPacketThroughput
			record.DlPacketThroughput = dataPoint.DlPacketThroughput
			if len(parseErrs) > 0 {
				malformed += len(parseErrs)
				logger.ProcLog.Warnf("UpfNotificationItem: malformed throughput corr=%s count=%d first=%v",
					bucket.CorrelationId, len(parseErrs), parseErrs[0])
			}
			logger.ProcLog.Debugf("UpfThroughput: corr=%s ul=%.0f dl=%.0f ulPps=%.2f dlPps=%.2f",
				bucket.CorrelationId, dataPoint.UlThroughput, dataPoint.DlThroughput,
				dataPoint.UlPacketThroughput, dataPoint.DlPacketThroughput)
		}

		// Persist traffic records for ADRF retrieval and offline analysis.
		if nwdaf_context.IsMongoAvailable() &&
			cfg != nil && cfg.Configuration != nil &&
			cfg.Configuration.Mongodb != nil {
			dbName := cfg.Configuration.Mongodb.Name
			coll := mongoapi.Client.Database(dbName).Collection(nwdaf_context.UpfTrafficDataColl)
			if err := p.insertUpfTrafficRecord(coll, record); err != nil {
				logger.ProcLog.Errorf("UpfNotification: persist failed corr=%s err=%v",
					bucket.CorrelationId, err)
			}
		}

		observation := contract.SourceObservation{
			ObservedAt:               measurementTs,
			IPv4Address:              ipAddr,
			Supi:                     item.Supi,
			Dnn:                      item.Dnn,
			TotalVolume:              float64(dataPoint.TotalVolume),
			UplinkVolume:             float64(dataPoint.UlVolume),
			DownlinkVolume:           float64(dataPoint.DlVolume),
			TotalPacketCount:         float64(dataPoint.TotalNbOfPackets),
			UplinkPacketCount:        float64(dataPoint.UlNbOfPackets),
			DownlinkPacketCount:      float64(dataPoint.DlNbOfPackets),
			UplinkThroughput:         dataPoint.UlThroughput,
			DownlinkThroughput:       dataPoint.DlThroughput,
			UplinkPacketThroughput:   dataPoint.UlPacketThroughput,
			DownlinkPacketThroughput: dataPoint.DlPacketThroughput,
		}
		if item.Snssai != nil {
			observation.Snssai = &contract.ObservationSnssai{Sst: item.Snssai.Sst, Sd: item.Snssai.Sd}
		}
		observations = append(observations, observation)
	}

	data.LastUpdate = measurementTs
	return true, malformed, observations
}

func (p *Processor) insertUpfTrafficRecord(
	coll *mongo.Collection,
	record nwdaf_context.UpfTrafficRecord,
) error {
	if coll == nil {
		return nil
	}

	parentCtx := p.nwdaf.CancelContext()
	if parentCtx == nil {
		return context.Canceled
	}

	ctx, cancel := context.WithTimeout(parentCtx, upfMongoWriteTimeout)
	defer cancel()

	_, err := coll.InsertOne(ctx, record)
	return err
}
