package anlf

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/free5gc/openapi/models"
)

var (
	ErrSubscriptionNotFound    = errors.New("subscription not found")
	ErrStaleAnalyticsReport    = errors.New("stale analytics report")
	ErrInvalidAnalyticsReport  = errors.New("invalid analytics report")
	ErrExternalDelivery        = errors.New("external analytics delivery failed")
	ErrAnalyticsReportInFlight = errors.New("analytics report delivery in progress")
)

// AnlfBackendAPI defines the downstream AnLF backend integration seam owned by AnLF.
type AnlfBackendAPI interface {
	ApplySubscriptionRuntime(
		ctx context.Context,
		request ApplySubscriptionRuntimeRequest,
	) (*ApplySubscriptionRuntimeResponse, error)
	ReleaseSubscriptionRuntime(ctx context.Context, subscriptionID string) error
	SyncObservationBindings(
		ctx context.Context,
		subscriptionID string,
		request SyncObservationBindingsRequest,
	) error
	SendObservations(ctx context.Context, sourceID string, batch ObservationBatch) error
	HTTPClient() *http.Client
}

type SubscriptionRuntimeContext struct {
	SubscriptionID     string                                            `json:"subscription_id"`
	NotifCorrID        string                                            `json:"notif_corr_id,omitempty"`
	EvtReq             *models.ReportingInformation                      `json:"evt_req,omitempty"`
	EventSubscriptions []models.NwdafEventsSubscriptionEventSubscription `json:"event_subscriptions"`
}

type ProvisionContext struct {
	Source              string              `json:"source"`
	MtlfSubscriptionID  string              `json:"mtlf_subscription_id,omitempty"`
	NotifSubscriptionID string              `json:"notif_subscription_id,omitempty"`
	MLEventNotification MLEventNotification `json:"ml_event_notif"`
}

// MLEventNotification extends the generated Release 17 model with lifecycle
// fields present in the newer local TS 29.520 OpenAPI definition.
type MLEventNotification struct {
	models.MlEventNotif
	ModelUpdateInd bool `json:"modelUpdateInd,omitempty"`
}

type ApplySubscriptionRuntimeRequest struct {
	Subscription      SubscriptionRuntimeContext `json:"subscription"`
	ProvisionContext  *ProvisionContext          `json:"provision_context,omitempty"`
	ReportCallbackURI string                     `json:"report_callback_uri"`
}

type ApplyResult string

const (
	ApplyResultPendingProvision    ApplyResult = "PENDING_PROVISION"
	ApplyResultActivated           ApplyResult = "ACTIVATED"
	ApplyResultReused              ApplyResult = "REUSED"
	ApplyResultReplaced            ApplyResult = "REPLACED"
	ApplyResultFailedUsingPrevious ApplyResult = "FAILED_USING_PREVIOUS"
	ApplyResultFailedNoPrevious    ApplyResult = "FAILED_NO_PREVIOUS"
)

type ApplySubscriptionRuntimeResponse struct {
	SubscriptionID         string                 `json:"subscription_id"`
	RuntimeState           string                 `json:"runtime_state"`
	Result                 ApplyResult            `json:"result"`
	FallbackApplied        bool                   `json:"fallback_applied"`
	ActiveModelReference   string                 `json:"active_model_reference,omitempty"`
	Message                string                 `json:"message,omitempty"`
	RuntimeRevision        int64                  `json:"runtime_revision"`
	CollectionRequirements CollectionRequirements `json:"collection_requirements"`
}

type CollectionRequirements struct {
	SamplingIntervalSeconds int      `json:"sampling_interval_seconds"`
	RequiredMeasurements    []string `json:"required_measurements"`
}

type ObservationSource struct {
	SourceType string `json:"source_type"`
	Supi       string `json:"supi,omitempty"`
}

type SubscriptionScope struct {
	OriginalGroupID string `json:"original_group_id,omitempty"`
}

type ObservationBinding struct {
	ObservationSourceID string                 `json:"observation_source_id"`
	Source              ObservationSource      `json:"source"`
	SubscriptionScope   SubscriptionScope      `json:"subscription_scope"`
	CollectionProfile   CollectionRequirements `json:"collection_profile"`
}

type SyncObservationBindingsRequest struct {
	RuntimeRevision int64                `json:"runtime_revision"`
	Bindings        []ObservationBinding `json:"bindings"`
}

type ObservationSnssai struct {
	Sst int32  `json:"sst"`
	Sd  string `json:"sd,omitempty"`
}

type SourceObservation struct {
	ObservedAt               time.Time          `json:"observed_at"`
	IPv4Address              string             `json:"ipv4_address,omitempty"`
	Supi                     string             `json:"supi,omitempty"`
	Dnn                      string             `json:"dnn,omitempty"`
	Snssai                   *ObservationSnssai `json:"snssai,omitempty"`
	TotalVolume              float64            `json:"total_volume"`
	UplinkVolume             float64            `json:"uplink_volume"`
	DownlinkVolume           float64            `json:"downlink_volume"`
	TotalPacketCount         float64            `json:"total_packet_count"`
	UplinkPacketCount        float64            `json:"uplink_packet_count"`
	DownlinkPacketCount      float64            `json:"downlink_packet_count"`
	UplinkThroughput         float64            `json:"uplink_throughput"`
	DownlinkThroughput       float64            `json:"downlink_throughput"`
	UplinkPacketThroughput   float64            `json:"uplink_packet_throughput"`
	DownlinkPacketThroughput float64            `json:"downlink_packet_throughput"`
}

type ObservationBatch struct {
	BatchID      string              `json:"batch_id"`
	Observations []SourceObservation `json:"observations"`
}

type AnalyticsTrafficCharacterization struct {
	Dnn            string `json:"dnn,omitempty"`
	UplinkVolume   int64  `json:"uplink_volume"`
	DownlinkVolume int64  `json:"downlink_volume"`
}

type AnalyticsUeCommunication struct {
	CommunicationDuration   int32                            `json:"communication_duration"`
	Timestamp               time.Time                        `json:"timestamp"`
	TrafficCharacterization AnalyticsTrafficCharacterization `json:"traffic_characterization"`
	Confidence              int32                            `json:"confidence"`
}

type AnalyticsEventNotification struct {
	Event            string                     `json:"event"`
	UeCommunications []AnalyticsUeCommunication `json:"ue_communications"`
}

type AnalyticsReport struct {
	ReportID           string                       `json:"report_id"`
	ReportSequence     int64                        `json:"report_sequence"`
	RuntimeRevision    int64                        `json:"runtime_revision"`
	GeneratedAt        time.Time                    `json:"generated_at"`
	EventNotifications []AnalyticsEventNotification `json:"event_notifications"`
}
