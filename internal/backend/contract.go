package backend

import (
	"encoding/json"

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

type DataSource string

const (
	DataSourceADRF        DataSource = "adrf"
	DataSourceMongoDB     DataSource = "mongodb"
	DataSourceUnavailable DataSource = "unavailable"
)

type HealthResponse struct {
	Status            string `json:"status"`
	ProcessInstanceID string `json:"processInstanceId"`
}

type NwdafIdentity struct {
	NFInstanceID            string `json:"nfInstanceId"`
	APIBaseURI              string `json:"apiBaseUri"`
	InternalCallbackBaseURI string `json:"internalCallbackBaseUri"`
}

type EventsSubscriptionSnapshot struct {
	SubscriptionID          string                          `json:"subscriptionId"`
	Subscription            models.NnwdafEventsSubscription `json:"subscription"`
	ExternalNotificationURI string                          `json:"externalNotificationUri"`
}

type SmfResourceSnapshot struct {
	CorrelationID        string          `json:"correlationId"`
	ResourceLocation     string          `json:"resourceLocation"`
	TargetAPIBaseURI     string          `json:"targetApiRoot"`
	NwdafSubscriptionIDs []string        `json:"nwdafSubscriptionIds"`
	PendingCleanup       bool            `json:"pendingCleanup"`
	Subscription         json.RawMessage `json:"subscription,omitempty"`
}

type SmfResourceAssociation struct {
	TargetAPIBaseURI     string   `json:"targetApiRoot"`
	PeerSubscriptionID   string   `json:"peerSubscriptionId"`
	NwdafSubscriptionIDs []string `json:"nwdafSubscriptionIds"`
}

type SmfResourceAssociationUpdate struct {
	ProcessInstanceID string                   `json:"processInstanceId"`
	SmfResources      []SmfResourceAssociation `json:"smfResources"`
}

type MLModelProvisionSubscriptionSnapshot struct {
	SubscriptionID    string          `json:"subscriptionId"`
	Representation    json.RawMessage `json:"representation"`
	Initiator         string          `json:"initiator"`
	Destination       string          `json:"destination"`
	Direction         string          `json:"direction,omitempty"`
	SelectedTarget    *SelectedTarget `json:"selectedTarget,omitempty"`
	PeerLocation      string          `json:"peerLocation,omitempty"`
	LifecycleState    string          `json:"lifecycleState,omitempty"`
	ProcessGeneration string          `json:"processGeneration,omitempty"`
}

type MLModelMonitorRegistrationSnapshot struct {
	RegistrationID    string          `json:"registrationId"`
	Representation    json.RawMessage `json:"representation"`
	Initiator         string          `json:"initiator"`
	Direction         string          `json:"direction,omitempty"`
	SelectedTarget    *SelectedTarget `json:"selectedTarget,omitempty"`
	PeerLocation      string          `json:"peerLocation,omitempty"`
	LifecycleState    string          `json:"lifecycleState,omitempty"`
	ProcessGeneration string          `json:"processGeneration,omitempty"`
}

type MLModelMonitorSubscriptionSnapshot struct {
	SubscriptionID    string          `json:"subscriptionId"`
	Representation    json.RawMessage `json:"representation"`
	Destination       string          `json:"destination"`
	OwnerRegistration string          `json:"ownerRegistrationId,omitempty"`
	Direction         string          `json:"direction,omitempty"`
	SelectedTarget    *SelectedTarget `json:"selectedTarget,omitempty"`
	PeerLocation      string          `json:"peerLocation,omitempty"`
	LifecycleState    string          `json:"lifecycleState,omitempty"`
	ProcessGeneration string          `json:"processGeneration,omitempty"`
}

type MLModelTrainingSubscriptionSnapshot struct {
	SubscriptionID    string          `json:"subscriptionId"`
	Representation    json.RawMessage `json:"representation"`
	Direction         string          `json:"direction,omitempty"`
	SelectedTarget    *SelectedTarget `json:"selectedTarget,omitempty"`
	PeerLocation      string          `json:"peerLocation,omitempty"`
	LifecycleState    string          `json:"lifecycleState,omitempty"`
	ProcessGeneration string          `json:"processGeneration,omitempty"`
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

type SyncRequest struct {
	ContainingNwdaf               NwdafIdentity                          `json:"containingNwdaf"`
	EventsSubscriptions           []EventsSubscriptionSnapshot           `json:"eventsSubscriptions"`
	SmfResources                  []SmfResourceSnapshot                  `json:"smfResources"`
	TrainingDataSource            DataSource                             `json:"trainingDataSource,omitempty"`
	MLModelProvisionSubscriptions []MLModelProvisionSubscriptionSnapshot `json:"mlModelProvisionSubscriptions"`
	MLModelMonitorRegistrations   []MLModelMonitorRegistrationSnapshot   `json:"mlModelMonitorRegistrations"`
	MLModelMonitorSubscriptions   []MLModelMonitorSubscriptionSnapshot   `json:"mlModelMonitorSubscriptions"`
	MLModelTrainingSubscriptions  []MLModelTrainingSubscriptionSnapshot  `json:"mlModelTrainingSubscriptions,omitempty"`
}

type SyncResponse struct {
	ProcessInstanceID  string     `json:"processInstanceId"`
	SnapshotAccepted   bool       `json:"snapshotAccepted"`
	TrainingDataSource DataSource `json:"trainingDataSource,omitempty"`
}
