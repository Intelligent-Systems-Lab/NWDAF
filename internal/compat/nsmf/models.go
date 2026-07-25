// Package nsmf contains the Release 18 Nsmf Event Exposure wire fields that
// are absent from the pinned free5GC OpenAPI dependency.
//
// Source: 3GPP TS 29.508 V18.11.0, TS29508_Nsmf_EventExposure.yaml.
package nsmf

import "encoding/json"

const EventUPFEvent = "UPF_EVENT"

type EventSubscription struct {
	Event     string            `json:"event"`
	UPFEvents []json.RawMessage `json:"upfEvents,omitempty"`
}

type EventExposure struct {
	SUPI        string              `json:"supi,omitempty"`
	AnyUEInd    bool                `json:"anyUeInd,omitempty"`
	GroupID     string              `json:"groupId,omitempty"`
	NFID        string              `json:"nfId,omitempty"`
	SubID       string              `json:"subId,omitempty"`
	NotifID     string              `json:"notifId"`
	NotifURI    string              `json:"notifUri"`
	EventSubs   []EventSubscription `json:"eventSubs"`
	NotifMethod string              `json:"notifMethod,omitempty"`
	RepPeriod   int32               `json:"repPeriod,omitempty"`
}
