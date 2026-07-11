package contract

import "github.com/free5gc/openapi/models"

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
	ModelUpdateInd  bool   `json:"modelUpdateInd,omitempty"`
	ModelUniqueID   *int64 `json:"modelUniqueId,omitempty"`
	ModelProviderID string `json:"modelProviderId,omitempty"`
}

type ModelProvisionNotification struct {
	EventNotifications []MLEventNotification `json:"eventNotifs"`
	SubscriptionID     string                `json:"subscriptionId"`
}

type ModelProvisionBinding struct {
	RuntimeRevision           int64  `json:"runtime_revision"`
	NotificationCorrelationID string `json:"notification_correlation_id,omitempty"`
	MtlfSubscriptionID        string `json:"mtlf_subscription_id,omitempty"`
	ProviderID                string `json:"provider_id,omitempty"`
}

type ModelArtifact struct {
	MLModelURL string `json:"mLModelUrl"`
}

type ProvisionNotificationCorrelation struct {
	NotificationCorrelationID string `json:"notification_correlation_id,omitempty"`
	MtlfSubscriptionID        string `json:"mtlf_subscription_id,omitempty"`
	ProvisionSubscriptionID   string `json:"provision_subscription_id,omitempty"`
}

type ModelProvisionEvent struct {
	Source                  string                           `json:"source"`
	ModelIdentity           ModelIdentity                    `json:"model_identity"`
	ModelUpdateInd          bool                             `json:"model_update_ind"`
	Artifact                ModelArtifact                    `json:"artifact"`
	AnalyticsEvent          string                           `json:"analytics_event"`
	NotificationCorrelation ProvisionNotificationCorrelation `json:"notification_correlation"`
	TrainingTaskID          string                           `json:"training_task_id,omitempty"`
}

type ModelProvisionEventResponse struct {
	Status               string `json:"status"`
	AffectedRuntimeCount int    `json:"affected_runtime_count"`
	Generation           int64  `json:"generation"`
}
