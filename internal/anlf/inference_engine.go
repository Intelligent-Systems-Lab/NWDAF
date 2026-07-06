package anlf

import (
	"context"
	"net/http"
)

// InferenceEngineAPI defines the local inference-engine integration seam owned by AnLF.
type InferenceEngineAPI interface {
	InitializeModel(ctx context.Context, modelURL string) (string, error)
	UnloadModel(ctx context.Context, modelID string) error
	Predict(ctx context.Context, modelID string, trafficData []TrafficObservation) (*PredictResponse, error)
	HTTPClient() *http.Client
}

// LoadModelRequest represents the request to load a model.
type LoadModelRequest struct {
	ModelUrl string `json:"model_url"`
}

// LoadModelResponse represents the response from model loading.
type LoadModelResponse struct {
	ModelId string `json:"model_id"`
}

// UnloadModelRequest represents the request to unload a model.
type UnloadModelRequest struct {
	ModelId string `json:"model_id"`
}

// UnloadModelResponse represents the response from model unloading.
type UnloadModelResponse struct {
	ModelId string `json:"model_id"`
	Status  string `json:"status"`
}

// TrafficCharacterization represents predicted traffic volume data.
type TrafficCharacterization struct {
	UlVol int64 `json:"ul_vol"`
	DlVol int64 `json:"dl_vol"`
}

// TrafficObservation represents a single traffic observation point for ML prediction.
// Fields match the inference-engine feature extraction order (10 features).
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
	ModelId        string               `json:"model_id"`
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
