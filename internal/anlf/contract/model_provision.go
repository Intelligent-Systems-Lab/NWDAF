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
	ModelUpdateInd bool `json:"modelUpdateInd,omitempty"`
}
