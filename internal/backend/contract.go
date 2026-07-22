package backend

import (
	"encoding/json"

	"github.com/free5gc/openapi/models"
)

type Kind string

const (
	KindAnLF Kind = "ANLF"
	KindMTLF Kind = "MTLF"
)

type DataSource string

const (
	DataSourceADRF    DataSource = "adrf"
	DataSourceMongoDB DataSource = "mongodb"
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

type DataSourceAvailability struct {
	ADRF    bool `json:"adrf"`
	MongoDB bool `json:"mongodb"`
}

type DataSourceSelection struct {
	PreferredSource DataSource `json:"preferredSource,omitempty"`
	EffectiveSource DataSource `json:"effectiveSource,omitempty"`
}

type SyncRequest struct {
	ContainingNwdaf        NwdafIdentity                `json:"containingNwdaf"`
	EventsSubscriptions    []EventsSubscriptionSnapshot `json:"eventsSubscriptions"`
	SmfResources           []SmfResourceSnapshot        `json:"smfResources"`
	DataSourceAvailability DataSourceAvailability       `json:"dataSourceAvailability"`
	MtlfSourceSelection    DataSourceSelection          `json:"mtlfSourceSelection"`
}

type SyncResponse struct {
	ProcessInstanceID string              `json:"processInstanceId"`
	SnapshotAccepted  bool                `json:"snapshotAccepted"`
	MongoDBAvailable  bool                `json:"mongodbAvailable"`
	SourceSelection   DataSourceSelection `json:"sourceSelection"`
}
