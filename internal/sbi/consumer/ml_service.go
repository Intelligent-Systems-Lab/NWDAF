package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// MlServiceClient handles ML inference service API interactions
// Used to communicate with the external ML inference engine for model loading and prediction
type MlServiceClient struct {
	endpoint   string
	httpClient *http.Client
}

// NewMlServiceClient creates a new ML service client
func NewMlServiceClient(endpoint string) *MlServiceClient {
	return &MlServiceClient{
		endpoint: endpoint,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        50,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// ============================================================================
// Request/Response Models
// ============================================================================

// LoadModelRequest represents the request to load a model
type LoadModelRequest struct {
	ModelUrl string `json:"model_url"`
}

// LoadModelResponse represents the response from model loading
type LoadModelResponse struct {
	ModelId string `json:"model_id"`
}

// TrafficCharacterization represents predicted traffic volume data (used in response)
type TrafficCharacterization struct {
	UlVol int64 `json:"ul_vol"`
	DlVol int64 `json:"dl_vol"`
}

// TrafficObservation represents a single traffic observation point for ML prediction.
// Fields match the ML service feature extraction order (10 features).
type TrafficObservation struct {
	Ts          string  `json:"ts"`
	TotalVol    float64 `json:"total_vol"`
	UlVol       float64 `json:"ul_vol"`
	DlVol       float64 `json:"dl_vol"`
	TotalNbPkts float64 `json:"total_nb_pkts"`
	UlNbPkts    float64 `json:"ul_nb_pkts"`
	DlNbPkts    float64 `json:"dl_nb_pkts"`
	UlThr       float64 `json:"ul_thr"`     // uplink throughput (bps)
	DlThr       float64 `json:"dl_thr"`     // downlink throughput (bps)
	UlPktThr    float64 `json:"ul_pkt_thr"` // uplink packet throughput (pps)
	DlPktThr    float64 `json:"dl_pkt_thr"` // downlink packet throughput (pps)
}

// PredictRequest represents the prediction request
type PredictRequest struct {
	ModelId        string               `json:"model_id"`
	HistoricalData []TrafficObservation `json:"historical_data"`
}

// UeCommunicationPrediction represents predicted UE communication data
type UeCommunicationPrediction struct {
	Ts         string                  `json:"ts"`
	TrafChar   TrafficCharacterization `json:"traf_char"`
	Confidence int32                   `json:"confidence"`
}

// PredictResponse represents the prediction response
type PredictResponse struct {
	PredictedData []UeCommunicationPrediction `json:"predicted_data"`
}

// ============================================================================
// API Methods
// ============================================================================

// InitializeModel loads a model from the given URL and returns the model ID
// Calls POST /model/load on the ML service
func (c *MlServiceClient) InitializeModel(modelUrl string) (string, error) {
	consumerLog.Infof("Initializing ML model from URL: %s", modelUrl)

	request := LoadModelRequest{
		ModelUrl: modelUrl,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	url := c.endpoint + "/model/load"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request to ML service: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			consumerLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return "", fmt.Errorf("ML model load failed: status=%d", resp.StatusCode)
		}
		return "", fmt.Errorf("ML model load failed: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var response LoadModelResponse
	if decodeErr := json.NewDecoder(resp.Body).Decode(&response); decodeErr != nil {
		return "", fmt.Errorf("failed to decode response: %w", decodeErr)
	}

	consumerLog.Infof("ML model initialized: modelId=%s", response.ModelId)
	return response.ModelId, nil
}

// Predict calls the ML service to get traffic predictions
// Calls POST /predict on the ML service
func (c *MlServiceClient) Predict(
	modelId string, trafficData []TrafficObservation,
) (*PredictResponse, error) {
	consumerLog.Debugf("Calling ML prediction: modelId=%s, dataPoints=%d",
		modelId, len(trafficData))

	request := PredictRequest{
		ModelId:        modelId,
		HistoricalData: trafficData,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	url := c.endpoint + "/predict"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to ML service: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			consumerLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("ML prediction failed: status=%d", resp.StatusCode)
		}
		return nil, fmt.Errorf("ML prediction failed: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var response PredictResponse
	if decodeErr := json.NewDecoder(resp.Body).Decode(&response); decodeErr != nil {
		return nil, fmt.Errorf("failed to decode response: %w", decodeErr)
	}

	consumerLog.Debugf("ML prediction received: %d predictions", len(response.PredictedData))
	return &response, nil
}

// GetEndpoint returns the configured endpoint
func (c *MlServiceClient) GetEndpoint() string {
	return c.endpoint
}

// HTTPClient returns the underlying HTTP client for testing
func (c *MlServiceClient) HTTPClient() *http.Client {
	return c.httpClient
}
