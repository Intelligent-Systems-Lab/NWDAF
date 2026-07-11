package context

import (
	"sync"

	"github.com/free5gc/openapi/models"
)

type MlModelStatus string

const (
	MlModelStatus_PENDING MlModelStatus = "PENDING"
	MlModelStatus_LOADING MlModelStatus = "LOADING"
	MlModelStatus_READY   MlModelStatus = "READY"
	MlModelStatus_FAILED  MlModelStatus = "FAILED"
)

// MlModelInfo retains only Go-owned MTLF procedure correlation and status.
type MlModelInfo struct {
	sync.RWMutex
	MtlfSubId    string
	MtlfEndpoint string
	ModelUrl     string
	Status       MlModelStatus
	Event        models.NwdafEvent
	LastError    error
}

func NewMlModelInfo(event models.NwdafEvent, mtlfEndpoint string) *MlModelInfo {
	return &MlModelInfo{Event: event, MtlfEndpoint: mtlfEndpoint, Status: MlModelStatus_PENDING}
}

func (m *MlModelInfo) SetMtlfSubscription(mtlfSubId string) {
	m.Lock()
	defer m.Unlock()
	m.MtlfSubId = mtlfSubId
}

func (m *MlModelInfo) SetModelUrl(modelUrl string) {
	m.Lock()
	defer m.Unlock()
	m.ModelUrl = modelUrl
	m.Status = MlModelStatus_LOADING
}

func (m *MlModelInfo) SetModelReady() {
	m.Lock()
	defer m.Unlock()
	m.Status = MlModelStatus_READY
	m.LastError = nil
}

func (m *MlModelInfo) SetModelFailed(err error) {
	m.Lock()
	defer m.Unlock()
	m.Status = MlModelStatus_FAILED
	m.LastError = err
}

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

func (m *MlModelInfo) GetStatus() MlModelStatus {
	m.RLock()
	defer m.RUnlock()
	return m.Status
}
