package contract

import (
	"errors"
	"time"
)

var ErrStaleModelGeneration = errors.New("stale model generation")

type MonitoringContext struct {
	AnalyticsEvent string         `json:"analytics_event"`
	TargetUE       map[string]any `json:"target_ue"`
	EventFilter    map[string]any `json:"event_filter"`
	ScopeID        string         `json:"scope_id"`
}

type AccuracyInformation struct {
	Metrics               map[string]float64 `json:"metrics"`
	Deviation             float64            `json:"deviation"`
	SampleCount           int                `json:"sample_count"`
	InferenceCount        int                `json:"inference_count"`
	ActualTrafficScale    float64            `json:"actual_traffic_scale"`
	PredictedTrafficScale float64            `json:"predicted_traffic_scale"`
	WindowStart           time.Time          `json:"window_start"`
	WindowEnd             time.Time          `json:"window_end"`
}

type RetrainContext struct {
	SubscriptionIDs      []string `json:"subscription_ids"`
	ObservationSourceIDs []string `json:"observation_source_ids"`
}

type ModelAccuracyReport struct {
	ReportID            string              `json:"report_id"`
	ReportSequence      int64               `json:"report_sequence"`
	GeneratedAt         time.Time           `json:"generated_at"`
	ModelIdentity       ModelIdentity       `json:"model_identity"`
	Generation          int64               `json:"generation"`
	MonitoringContext   MonitoringContext   `json:"monitoring_context"`
	AccuracyInformation AccuracyInformation `json:"accuracy_information"`
	RetrainContext      RetrainContext      `json:"retrain_context"`
}
