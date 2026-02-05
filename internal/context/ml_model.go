package context

import (
	"sync"

	"github.com/free5gc/openapi/models"
)

// MlModelStatus represents the status of an ML model for a subscription
type MlModelStatus string

const (
	MlModelStatus_PENDING MlModelStatus = "PENDING" // Waiting for MTLF notification
	MlModelStatus_LOADING MlModelStatus = "LOADING" // Model URL received, initializing
	MlModelStatus_READY   MlModelStatus = "READY"   // Model loaded, ready for inference
	MlModelStatus_FAILED  MlModelStatus = "FAILED"  // Failed to load
)

// MlModelInfo tracks ML model state for a subscription
// One MlModelInfo per NWDAF subscription that requires ML-based analytics
type MlModelInfo struct {
	sync.RWMutex

	// MTLF subscription details
	MtlfSubId    string // Subscription ID from MTLF
	MtlfEndpoint string // MTLF endpoint URL

	// Model details (populated after MTLF notification)
	ModelUrl string            // ML model file URL from MTLF
	ModelId  string            // Model ID from ML inference service
	Status   MlModelStatus     // Current status
	Event    models.NwdafEvent // Analytics event type

	// Error tracking
	LastError error
}

// NewMlModelInfo creates a new MlModelInfo in PENDING state
func NewMlModelInfo(event models.NwdafEvent, mtlfEndpoint string) *MlModelInfo {
	return &MlModelInfo{
		Event:        event,
		MtlfEndpoint: mtlfEndpoint,
		Status:       MlModelStatus_PENDING,
	}
}

// SetMtlfSubscription updates MTLF subscription details
func (m *MlModelInfo) SetMtlfSubscription(mtlfSubId string) {
	m.Lock()
	defer m.Unlock()
	m.MtlfSubId = mtlfSubId
}

// SetModelUrl updates the model URL and transitions to LOADING state
func (m *MlModelInfo) SetModelUrl(modelUrl string) {
	m.Lock()
	defer m.Unlock()
	m.ModelUrl = modelUrl
	m.Status = MlModelStatus_LOADING
}

// SetModelReady marks the model as ready for inference
func (m *MlModelInfo) SetModelReady(modelId string) {
	m.Lock()
	defer m.Unlock()
	m.ModelId = modelId
	m.Status = MlModelStatus_READY
	m.LastError = nil
}

// SetModelFailed marks the model as failed
func (m *MlModelInfo) SetModelFailed(err error) {
	m.Lock()
	defer m.Unlock()
	m.Status = MlModelStatus_FAILED
	m.LastError = err
}

// IsReady returns true if the model is ready for inference
func (m *MlModelInfo) IsReady() bool {
	m.RLock()
	defer m.RUnlock()
	return m.Status == MlModelStatus_READY
}

// GetModelId returns the model ID (for inference calls)
func (m *MlModelInfo) GetModelId() string {
	m.RLock()
	defer m.RUnlock()
	return m.ModelId
}

// GetStatus returns the current status
func (m *MlModelInfo) GetStatus() MlModelStatus {
	m.RLock()
	defer m.RUnlock()
	return m.Status
}
