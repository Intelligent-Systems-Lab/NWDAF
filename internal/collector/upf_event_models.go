package collector

import (
	"time"

	"github.com/free5gc/openapi/models"
)

// UPF Event Types based on TS 29.564 EventType enum
type UpfEventType string

const (
	UpfEventType_QOS_MONITORING           UpfEventType = "QOS_MONITORING"
	UpfEventType_USER_DATA_USAGE_MEASURES UpfEventType = "USER_DATA_USAGE_MEASURES"
	UpfEventType_USER_DATA_USAGE_TRENDS   UpfEventType = "USER_DATA_USAGE_TRENDS"
	UpfEventType_TSC_MNGT_INFO            UpfEventType = "TSC_MNGT_INFO"
	UpfEventType_UE_NAT_MAPPING_INFO      UpfEventType = "UE_NAT_MAPPING_INFO"
	UpfEventType_SUBSCRIPTION_TERMINATION UpfEventType = "SUBSCRIPTION_TERMINATION"
)

// UpfNotificationData wraps UPF notification items
// Based on TS 29.564 NotificationData schema
type UpfNotificationData struct {
	NotificationItems []UpfNotificationItem `json:"notificationItems"`
	CorrelationId     string                `json:"correlationId,omitempty"`
	EventNotifyUri    string                `json:"eventNotifyUri,omitempty"`
}

// UpfNotificationItem represents a single UPF event report
// Based on TS 29.564 NotificationItem schema
type UpfNotificationItem struct {
	EventType                 UpfEventType                `json:"eventType"`
	UeIpv4Addr                string                      `json:"ueIpv4Addr,omitempty"`
	UeIpv6Prefix              string                      `json:"ueIpv6Prefix,omitempty"`
	Supi                      string                      `json:"supi,omitempty"`
	Gpsi                      string                      `json:"gpsi,omitempty"`
	Dnn                       string                      `json:"dnn,omitempty"`
	Snssai                    *models.Snssai              `json:"snssai,omitempty"`
	TimeStamp                 time.Time                   `json:"timeStamp"`
	StartTime                 *time.Time                  `json:"startTime,omitempty"`
	RatType                   models.RatType              `json:"ratType,omitempty"`
	UserDataUsageMeasurements []UserDataUsageMeasurements `json:"userDataUsageMeasurements,omitempty"`
}

// UserDataUsageMeasurements contains traffic volume data
// Based on TS 29.564 UserDataUsageMeasurements schema
// Note: AppId is optional and not implemented per user request
type UserDataUsageMeasurements struct {
	VolumeMeasurement     *VolumeMeasurement     `json:"volumeMeasurement,omitempty"`
	ThroughputMeasurement *ThroughputMeasurement `json:"throughputMeasurement,omitempty"`
}

// VolumeMeasurement contains UL/DL volume information
// Based on TS 29.564 VolumeMeasurement schema
type VolumeMeasurement struct {
	TotalVolume      int64 `json:"totalVolume,omitempty"`
	UlVolume         int64 `json:"ulVolume,omitempty"`
	DlVolume         int64 `json:"dlVolume,omitempty"`
	TotalNbOfPackets int64 `json:"totalNbOfPackets,omitempty"`
	UlNbOfPackets    int64 `json:"ulNbOfPackets,omitempty"`
	DlNbOfPackets    int64 `json:"dlNbOfPackets,omitempty"`
}

// ThroughputMeasurement contains throughput information
// Based on TS 29.564 ThroughputMeasurement schema
type ThroughputMeasurement struct {
	UlThroughput       string `json:"ulThroughput,omitempty"`
	DlThroughput       string `json:"dlThroughput,omitempty"`
	UlPacketThroughput string `json:"ulPacketThroughput,omitempty"`
	DlPacketThroughput string `json:"dlPacketThroughput,omitempty"`
}
