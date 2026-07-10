package anlf

import (
	"context"
	"net/http"

	"github.com/free5gc/openapi/models"
)

// AnlfBackendAPI defines the downstream AnLF backend integration seam owned by AnLF.
type AnlfBackendAPI interface {
	ApplySubscriptionRuntime(
		ctx context.Context,
		request ApplySubscriptionRuntimeRequest,
	) (*ApplySubscriptionRuntimeResponse, error)
	ReleaseSubscriptionRuntime(ctx context.Context, subscriptionID string) error
	Predict(ctx context.Context, subscriptionID string, trafficData []TrafficObservation) (*PredictResponse, error)
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
	Subscription     SubscriptionRuntimeContext `json:"subscription"`
	ProvisionContext *ProvisionContext          `json:"provision_context,omitempty"`
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
	SubscriptionID       string      `json:"subscription_id"`
	RuntimeState         string      `json:"runtime_state"`
	Result               ApplyResult `json:"result"`
	FallbackApplied      bool        `json:"fallback_applied"`
	ActiveModelReference string      `json:"active_model_reference,omitempty"`
	Message              string      `json:"message,omitempty"`
}

// TrafficCharacterization represents predicted traffic volume data.
type TrafficCharacterization struct {
	UlVol int64 `json:"ul_vol"`
	DlVol int64 `json:"dl_vol"`
}

// TrafficObservation represents a single traffic observation point for ML prediction.
// Fields match the backend feature extraction order (10 features).
type TrafficObservation struct {
	Ts          string  `json:"ts"`
	TotalVol    float64 `json:"total_vol"`
	UlVol       float64 `json:"ul_vol"`
	DlVol       float64 `json:"dl_vol"`
	TotalNbPkts float64 `json:"total_nb_pkts"`
	UlNbPkts    float64 `json:"ul_nb_pkts"`
	DlNbPkts    float64 `json:"dl_nb_pkts"`
	UlThr       float64 `json:"ul_thr"`
	DlThr       float64 `json:"dl_thr"`
	UlPktThr    float64 `json:"ul_pkt_thr"`
	DlPktThr    float64 `json:"dl_pkt_thr"`
}

// PredictRequest represents the prediction request.
type PredictRequest struct {
	HistoricalData []TrafficObservation `json:"historical_data"`
}

// UeCommunicationPrediction represents predicted UE communication data.
type UeCommunicationPrediction struct {
	Ts         string                  `json:"ts"`
	TrafChar   TrafficCharacterization `json:"traf_char"`
	Confidence int32                   `json:"confidence"`
}

// PredictResponse represents the prediction response.
type PredictResponse struct {
	PredictedData []UeCommunicationPrediction `json:"predicted_data"`
}
