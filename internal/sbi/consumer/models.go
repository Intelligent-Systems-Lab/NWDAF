package consumer

import "github.com/free5gc/openapi/models"

// Extended SMF Event types (TS 29.508)
const (
	SmfEvent_UPF_EVENT models.SmfEvent = "UPF_EVENT"
)

// Extended NsmfEventExposure with UPF_EVENT support
type ExtendedNsmfEventExposure struct {
	Supi      string                      `json:"supi,omitempty"`
	NotifUri  string                      `json:"notifUri"`
	NotifId   string                      `json:"notifId"`
	SubId     string                      `json:"subId,omitempty"`
	EventSubs []ExtendedEventSubscription `json:"eventSubs"`
}

// ExtendedEventSubscription supports both standard SMF events and UPF_EVENT
type ExtendedEventSubscription struct {
	Event models.SmfEvent `json:"event"`

	// UPF_EVENT specific fields (TS 29.508)
	UpfEvents             []UpfEvent `json:"upfEvents,omitempty"`
	BundlingAllowed       bool       `json:"bundlingAllowed,omitempty"`
	BundledEventNotifyUri string     `json:"bundledEventNotifyUri,omitempty"`
}

// UpfEvent defines UPF event subscription (TS 29.564)
type UpfEvent struct {
	Type                     UpfEventType      `json:"type"`
	MeasurementTypes         []MeasurementType `json:"measurementTypes,omitempty"`
	GranularityOfMeasurement Granularity       `json:"granularityOfMeasurement,omitempty"`
}

// UpfEventType enum (TS 29.564)
type UpfEventType string

const (
	UpfEventType_USER_DATA_USAGE_MEASURES UpfEventType = "USER_DATA_USAGE_MEASURES"
)

// MeasurementType enum (TS 29.564)
type MeasurementType string

const (
	MeasurementType_VOLUME_MEASUREMENT     MeasurementType = "VOLUME_MEASUREMENT"
	MeasurementType_THROUGHPUT_MEASUREMENT MeasurementType = "THROUGHPUT_MEASUREMENT"
)

// Granularity enum (TS 29.564)
type Granularity string

const (
	Granularity_PER_SESSION Granularity = "PER_SESSION"
	Granularity_PER_FLOW    Granularity = "PER_FLOW"
)

// UpfEventNotification represents UPF notification (TS 29.564)
type UpfEventNotification struct {
	Event           UpfEventType      `json:"event"`
	NotificationUri string            `json:"notificationUri,omitempty"`
	CorrelationId   string            `json:"correlationId,omitempty"`
	NotifItems      []UpfNotifItem    `json:"notifItems,omitempty"`
	ReportingTime   string            `json:"reportingTime,omitempty"`
	UsageReport     []UsageReportItem `json:"usageReport,omitempty"`
}

// UpfNotifItem contains individual UPF notification data
type UpfNotifItem struct {
	Supi        string            `json:"supi,omitempty"`
	UpfId       string            `json:"upfId,omitempty"`
	UsageReport []UsageReportItem `json:"usageReport,omitempty"`
}

// UsageReportItem contains volume and throughput measurements
type UsageReportItem struct {
	// Volume measurements
	VolumeMeasurement *VolumeMeasurement `json:"volumeMeasurement,omitempty"`

	// Throughput measurements
	ThroughputMeasurement *ThroughputMeasurement `json:"throughputMeasurement,omitempty"`
}

// VolumeMeasurement contains UL/DL volume data
type VolumeMeasurement struct {
	TotalVolume int64 `json:"totalVolume,omitempty"`
	UlVolume    int64 `json:"ulVolume,omitempty"`
	DlVolume    int64 `json:"dlVolume,omitempty"`
}

// ThroughputMeasurement contains UL/DL throughput data
type ThroughputMeasurement struct {
	UlThroughput string `json:"ulThroughput,omitempty"`
	DlThroughput string `json:"dlThroughput,omitempty"`
}
