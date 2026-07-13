package contract

import "github.com/free5gc/openapi/models"

type SubscriptionRuntimeContext struct {
	SubscriptionID     string                                            `json:"subscription_id"`
	NotifCorrID        string                                            `json:"notif_corr_id,omitempty"`
	EvtReq             *models.ReportingInformation                      `json:"evt_req,omitempty"`
	EventSubscriptions []models.NwdafEventsSubscriptionEventSubscription `json:"event_subscriptions"`
}

type ApplySubscriptionRuntimeRequest struct {
	Subscription                 SubscriptionRuntimeContext `json:"subscription"`
	ProvisionContext             *ProvisionContext          `json:"provision_context,omitempty"`
	ReportCallbackURI            string                     `json:"report_callback_uri"`
	RuntimeCompletionCallbackURI string                     `json:"runtime_completion_callback_uri"`
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
