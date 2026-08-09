package backend

import (
	"time"

	adrfcompat "github.com/free5gc/nwdaf/internal/compat/adrf"
	"github.com/free5gc/openapi/models"
)

type Kind string

const (
	KindAnLF Kind = "ANLF"
	KindMTLF Kind = "MTLF"

	MonitorRegistrationIDHeader     = "X-NWDAF-Monitor-Registration-Id"
	TargetNFInstanceIDHeader        = "X-NWDAF-Target-Nf-Instance-Id"
	TargetNFServiceInstanceIDHeader = "X-NWDAF-Target-Nf-Service-Instance-Id"
	TargetAPIRootHeader             = "X-NWDAF-Target-Api-Root"
	TargetSelectionSourceHeader     = "X-NWDAF-Target-Selection-Source"
	SelectionSourceNRF              = "NRF"
	SelectionSourceConfigured       = "CONFIGURED"
)

type HealthResponse struct {
	Status            string `json:"status"`
	ProcessInstanceID string `json:"processInstanceId"`
}

type NwdafContextResponse struct {
	NFInstanceID    string `json:"nfInstanceId"`
	APIRoot         string `json:"apiRoot"`
	InternalAPIRoot string `json:"internalApiRoot"`
}

type StoredDataSpec struct {
	DataSpec   adrfcompat.DataSubscription `json:"dataSpec"`
	TimePeriod models.TimeWindow           `json:"timePeriod"`
}

type TrainingDataDescriptor struct {
	CorrelationID       string                     `json:"correlationId"`
	State               string                     `json:"state"`
	StoredDataSpec      StoredDataSpec             `json:"storedDataSpec"`
	MLEventSubscription models.MlEventSubscription `json:"mlEventSubscription"`
	SourceNFInstanceID  string                     `json:"sourceNfInstanceId"`
	ADRFInstanceID      string                     `json:"adrfInstanceId"`
	RetainUntil         time.Time                  `json:"retainUntil"`
}

// SelectedTarget is private routing metadata selected from NRF discovery or
// explicit experiment configuration. It is never serialized into a 3GPP
// request body.
type SelectedTarget struct {
	NFInstanceID        string `json:"nfInstanceId"`
	NFServiceInstanceID string `json:"nfServiceInstanceId"`
	ServiceName         string `json:"serviceName"`
	APIRoot             string `json:"apiRoot"`
	SelectionSource     string `json:"selectionSource"`
}
