package collector

import (
	"github.com/free5gc/openapi/models"
)

// SmfEvent_UPF_EVENT is not defined in OpenAPI models, define locally
// Based on TS 29.508 SmfEvent enum
const SmfEvent_UPF_EVENT models.SmfEvent = "UPF_EVENT"

// UpfEvent represents a UPF event subscription
// Based on TS 29.564 UpfEvent schema
type UpfEvent struct {
	Type                     UpfEventType      `json:"type"`
	ImmediateFlag            bool              `json:"immediateFlag,omitempty"`
	MeasurementTypes         []MeasurementType `json:"measurementTypes,omitempty"`
	AppIds                   []string          `json:"appIds,omitempty"`
	GranularityOfMeasurement GranularityType   `json:"granularityOfMeasurement,omitempty"`
}

// MeasurementType represents the type of measurement requested
// Based on TS 29.564 MeasurementType enum
type MeasurementType string

const (
	MeasurementType_VOLUME_MEASUREMENT       MeasurementType = "VOLUME_MEASUREMENT"
	MeasurementType_THROUGHPUT_MEASUREMENT   MeasurementType = "THROUGHPUT_MEASUREMENT"
	MeasurementType_APPLICATION_RELATED_INFO MeasurementType = "APPLICATION_RELATED_INFO"
)

// GranularityType represents the granularity of measurement
// Based on TS 29.564 GranularityOfMeasurement enum
type GranularityType string

const (
	Granularity_PER_SESSION     GranularityType = "PER_SESSION"
	Granularity_PER_APPLICATION GranularityType = "PER_APPLICATION"
	Granularity_PER_FLOW        GranularityType = "PER_FLOW"
)

// ExtendedEventSubscription extends SmfEventExposureEventSubscription with UPF fields
// Based on TS 29.508 EventSubscription schema with upfEvents support
// OpenAPI models lack upfEvents, bundledEventNotifyUri fields, so we define our own
type ExtendedEventSubscription struct {
	Event                 models.SmfEvent `json:"event"`
	UpfEvents             []UpfEvent      `json:"upfEvents,omitempty"`
	BundlingAllowed       bool            `json:"bundlingAllowed,omitempty"`
	BundledEventNotifyUri string          `json:"bundledEventNotifyUri,omitempty"`
	AppIds                []string        `json:"appIds,omitempty"`
}

// ExtendedNsmfEventExposure extends NsmfEventExposure with extended event subscriptions
// Uses ExtendedEventSubscription instead of SmfEventExposureEventSubscription
type ExtendedNsmfEventExposure struct {
	Supi      string                      `json:"supi,omitempty"`
	Gpsi      string                      `json:"gpsi,omitempty"`
	NotifUri  string                      `json:"notifUri"`
	NotifId   string                      `json:"notifId"`
	EventSubs []ExtendedEventSubscription `json:"eventSubs"`
	SubId     string                      `json:"subId,omitempty"`
}
