// Package adrf contains the Release 18 Nadrf Data Management and ML Model
// Management wire models that are absent from the pinned free5GC OpenAPI
// dependency.
//
// Source: 3GPP TS 29.575 V18.8.0, TS29575_Nadrf_DataManagement.yaml and
// TS29575_Nadrf_MLModelManagement.yaml.
package adrf

import (
	"encoding/json"

	"github.com/free5gc/nwdaf/internal/compat/nsmf"
	"github.com/free5gc/openapi/models"
)

type TimePeriod struct {
	StartTime string `json:"startTime"`
	StopTime  string `json:"stopTime"`
}

type DataRetrievalSubscription struct {
	NotifCorrId     string           `json:"notifCorrId"`
	NotificationURI string           `json:"notificationURI"`
	TimePeriod      TimePeriod       `json:"timePeriod"`
	DataSub         DataSubscription `json:"dataSub"`
	ConsTrigNotif   bool             `json:"consTrigNotif,omitempty"`
}

type DataSubscription struct {
	SmfDataSub *nsmf.EventExposure `json:"smfDataSub,omitempty"`
}

type DataNotification struct {
	UpfEventNotifs []json.RawMessage `json:"upfEventNotifs"`
	SmfEventNotifs []json.RawMessage `json:"smfEventNotifs,omitempty"`
}

type DataStoreRecord struct {
	DataSub   []DataSubscription `json:"dataSub"`
	DataNotif *DataNotification  `json:"dataNotif,omitempty"`
}

type DataRetrievalNotification struct {
	NotifCorrId      string                   `json:"notifCorrId"`
	TimeStamp        string                   `json:"timeStamp"`
	FetchInstruct    *models.FetchInstruction `json:"fetchInstruct,omitempty"`
	DataNotif        json.RawMessage          `json:"dataNotif,omitempty"`
	AnaNotifications []json.RawMessage        `json:"anaNotifications,omitempty"`
	TerminationReq   bool                     `json:"terminationReq,omitempty"`
}

type MLModelAddress struct {
	MLModelURL string `json:"mLModelUrl,omitempty"`
	MLFileFQDN string `json:"mlFileFqdn,omitempty"`
}

type AllowedConsumer struct {
	NFInstanceID string `json:"nfInstanceId,omitempty"`
	NFSetID      string `json:"nfSetId,omitempty"`
}

type MLModelInfo struct {
	ModelUniqueID     *int64            `json:"modelUniqueId"`
	MLFileAddr        MLModelAddress    `json:"mlFileAddr"`
	MLStorageSize     *int64            `json:"mlStorageSize"`
	AllowConsumerList []AllowedConsumer `json:"allowConsumerList,omitempty"`
}

type ModelStoreResult struct {
	ModelUniqueID *int64 `json:"modelUniqueId"`
	StoreResult   string `json:"storeResult"`
}

type MLModelStoreRecord struct {
	NFInstanceID     string            `json:"nfInstanceId,omitempty"`
	NFSetID          string            `json:"nfSetId,omitempty"`
	MLModelInfo      []MLModelInfo     `json:"mlModelInfo,omitempty"`
	ModelStoreResult *ModelStoreResult `json:"modelStoreResult,omitempty"`
	SuppFeat         string            `json:"suppFeat,omitempty"`
}
