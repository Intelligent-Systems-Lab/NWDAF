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

// SetModelReady marks the subscription runtime as ready for inference.
func (m *MlModelInfo) SetModelReady() {
	m.Lock()
	defer m.Unlock()
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

// IsReady returns true if the model is ready for backend prediction.
func (m *MlModelInfo) IsReady() bool {
	m.RLock()
	defer m.RUnlock()
	return m.Status == MlModelStatus_READY
}

func (m *MlModelInfo) GetModelURL() string {
	m.RLock()
	defer m.RUnlock()
	return m.ModelUrl
}

// GetStatus returns the current status
func (m *MlModelInfo) GetStatus() MlModelStatus {
	m.RLock()
	defer m.RUnlock()
	return m.Status
}

// ============================================================================
// SharedModelInfo — per-modelUrl shared model state
// ============================================================================

// SharedModelInfo retains model-reference correlation for the Go-owned
// accuracy workflow. PyAnLF owns model loading and runtime usage tracking.
type SharedModelInfo struct {
	sync.RWMutex
	ModelUrl    string
	Event       models.NwdafEvent   // Analytics event type
	Subscribers map[string]struct{} // nwdafSubId set
}

// NewSharedModelInfo creates a new SharedModelInfo
func NewSharedModelInfo(modelUrl string, event models.NwdafEvent) *SharedModelInfo {
	return &SharedModelInfo{
		ModelUrl:    modelUrl,
		Event:       event,
		Subscribers: make(map[string]struct{}),
	}
}

// AddSubscriber adds a subscriber, returns current count
func (s *SharedModelInfo) AddSubscriber(nwdafSubId string) int {
	s.Lock()
	defer s.Unlock()
	s.Subscribers[nwdafSubId] = struct{}{}
	return len(s.Subscribers)
}

// RemoveSubscriber removes a subscriber, returns remaining count
func (s *SharedModelInfo) RemoveSubscriber(nwdafSubId string) int {
	s.Lock()
	defer s.Unlock()
	delete(s.Subscribers, nwdafSubId)
	return len(s.Subscribers)
}

// SubscriberCount returns the number of active subscribers
func (s *SharedModelInfo) SubscriberCount() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.Subscribers)
}

// GetSubscriberIDs returns a snapshot of all subscriber IDs for this model.
func (s *SharedModelInfo) GetSubscriberIDs() []string {
	s.RLock()
	defer s.RUnlock()
	ids := make([]string, 0, len(s.Subscribers))
	for id := range s.Subscribers {
		ids = append(ids, id)
	}
	return ids
}
