package contract

import "time"

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
