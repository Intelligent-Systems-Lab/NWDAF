package contract

import "time"

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
