package anlf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// InferenceEngineAPI defines the local inference-engine integration seam owned by AnLF.
type InferenceEngineAPI interface {
	InitializeModel(ctx context.Context, modelURL string) (string, error)
	UnloadModel(ctx context.Context, modelID string) error
	Predict(ctx context.Context, modelID string, trafficData []TrafficObservation) (*PredictResponse, error)
	HTTPClient() *http.Client
}

// InferenceEngineClient handles local inference-engine API interactions.
type InferenceEngineClient struct {
	endpoint   string
	httpClient *http.Client
}

// NewInferenceEngineClient creates a new inference-engine client.
func NewInferenceEngineClient(endpoint string) *InferenceEngineClient {
	return &InferenceEngineClient{
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

// UnloadModelRequest represents the request to unload a model
type UnloadModelRequest struct {
	ModelId string `json:"model_id"`
}

// UnloadModelResponse represents the response from model unloading
type UnloadModelResponse struct {
	ModelId string `json:"model_id"`
	Status  string `json:"status"`
}

// TrafficCharacterization represents predicted traffic volume data (used in response)
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

// InitializeModel loads a model from the given URL and returns the model ID.
func (c *InferenceEngineClient) InitializeModel(ctx context.Context, modelURL string) (string, error) {
	request := LoadModelRequest{
		ModelUrl: modelURL,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	url := c.endpoint + "/model/load"

	ctx, cancel, err := timeoutContextFromParent(ctx, 120*time.Second, "inference engine model initialization")
	if err != nil {
		return "", err
	}
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request to inference engine: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			anlfLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("inference engine model load failed: status=%d", resp.StatusCode)
	}

	var response LoadModelResponse
	if decodeErr := json.NewDecoder(resp.Body).Decode(&response); decodeErr != nil {
		return "", fmt.Errorf("failed to decode response: %w", decodeErr)
	}

	return response.ModelId, nil
}

// UnloadModel unloads a model by ID.
func (c *InferenceEngineClient) UnloadModel(ctx context.Context, modelID string) error {
	request := UnloadModelRequest{
		ModelId: modelID,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	url := c.endpoint + "/model/unload"

	ctx, cancel, err := timeoutContextFromParent(ctx, 10*time.Second, "inference engine model unload")
	if err != nil {
		return err
	}
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request to inference engine: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			anlfLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("inference engine model unload failed: status=%d", resp.StatusCode)
	}

	return nil
}

// Predict calls the inference engine to get traffic predictions.
func (c *InferenceEngineClient) Predict(
	ctx context.Context,
	modelID string,
	trafficData []TrafficObservation,
) (*PredictResponse, error) {
	anlfLog.Debugf("Calling inference engine: modelId=%s, dataPoints=%d",
		modelID, len(trafficData))

	request := PredictRequest{
		ModelId:        modelID,
		HistoricalData: trafficData,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	url := c.endpoint + "/predict"

	ctx, cancel, err := timeoutContextFromParent(ctx, 10*time.Second, "inference engine prediction")
	if err != nil {
		return nil, err
	}
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to inference engine: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			anlfLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("inference engine prediction failed: status=%d", resp.StatusCode)
	}

	var response PredictResponse
	if decodeErr := json.NewDecoder(resp.Body).Decode(&response); decodeErr != nil {
		return nil, fmt.Errorf("failed to decode response: %w", decodeErr)
	}

	anlfLog.Debugf("Inference engine returned %d predictions", len(response.PredictedData))
	return &response, nil
}

// GetEndpoint returns the configured endpoint.
func (c *InferenceEngineClient) GetEndpoint() string {
	return c.endpoint
}

// HTTPClient returns the underlying HTTP client for testing.
func (c *InferenceEngineClient) HTTPClient() *http.Client {
	return c.httpClient
}

func timeoutContextFromParent(
	parent context.Context,
	timeout time.Duration,
	operation string,
) (context.Context, context.CancelFunc, error) {
	if parent == nil {
		return nil, nil, fmt.Errorf("%s requires parent context", operation)
	}

	ctx, cancel := context.WithTimeout(parent, timeout)
	return ctx, cancel, nil
}
