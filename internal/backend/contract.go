package backend

import (
	"encoding/json"

	"github.com/free5gc/openapi/models"
)

type Kind string

const (
	KindAnLF Kind = "ANLF"
	KindMTLF Kind = "MTLF"

	MonitorRegistrationIDHeader = "X-NWDAF-Monitor-Registration-Id"
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
	SubscriptionID string          `json:"subscriptionId"`
	Representation json.RawMessage `json:"representation"`
	Initiator      string          `json:"initiator"`
	Destination    string          `json:"destination"`
}

type MLModelMonitorRegistrationSnapshot struct {
	RegistrationID string          `json:"registrationId"`
	Representation json.RawMessage `json:"representation"`
	Initiator      string          `json:"initiator"`
}

type MLModelMonitorSubscriptionSnapshot struct {
	SubscriptionID    string          `json:"subscriptionId"`
	Representation    json.RawMessage `json:"representation"`
	Destination       string          `json:"destination"`
	OwnerRegistration string          `json:"ownerRegistrationId,omitempty"`
}

type SyncRequest struct {
	ContainingNwdaf               NwdafIdentity                          `json:"containingNwdaf"`
	EventsSubscriptions           []EventsSubscriptionSnapshot           `json:"eventsSubscriptions"`
	SmfResources                  []SmfResourceSnapshot                  `json:"smfResources"`
	TrainingDataSource            DataSource                             `json:"trainingDataSource,omitempty"`
	MLModelProvisionSubscriptions []MLModelProvisionSubscriptionSnapshot `json:"mlModelProvisionSubscriptions"`
	MLModelMonitorRegistrations   []MLModelMonitorRegistrationSnapshot   `json:"mlModelMonitorRegistrations"`
	MLModelMonitorSubscriptions   []MLModelMonitorSubscriptionSnapshot   `json:"mlModelMonitorSubscriptions"`
}

type SyncResponse struct {
	ProcessInstanceID  string     `json:"processInstanceId"`
	SnapshotAccepted   bool       `json:"snapshotAccepted"`
	TrainingDataSource DataSource `json:"trainingDataSource,omitempty"`
}
