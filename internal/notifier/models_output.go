package notifier

import (
	"github.com/free5gc/openapi/models"
)

// Output models using struct embedding with field override
// Per TS 23.288 §6.2: "shall return a zero confidence" when data is insufficient
//
// Go's JSON marshaling behavior:
// - When a struct embeds another struct and also defines a field with the same JSON tag,
//   the outer field shadows the embedded field during marshaling
// - This allows us to override just the omitempty behavior for specific fields

// TrafficCharacterizationOutput embeds original and overrides UlVol/DlVol
type TrafficCharacterizationOutput struct {
	models.TrafficCharacterization       // Embed original (most fields retained)
	UlVol                          int64 `json:"ulVol"` // Override: no omitempty
	DlVol                          int64 `json:"dlVol"` // Override: no omitempty
}

// UeCommunicationOutput embeds original and overrides Confidence and TrafChar
type UeCommunicationOutput struct {
	models.UeCommunication                                // Embed original (most fields retained)
	TrafChar               *TrafficCharacterizationOutput `json:"trafChar,omitempty"` // Override type
	Confidence             int32                          `json:"confidence"`         // Override: no omitempty
}

// EventNotificationOutput embeds original and overrides UeComms type
type EventNotificationOutput struct {
	models.NwdafEventsSubscriptionEventNotification                         // Embed original
	Event                                           string                  `json:"event"`             // Simplified output
	UeComms                                         []UeCommunicationOutput `json:"ueComms,omitempty"` // Override type
}

// NotificationOutput embeds original and overrides EventNotifications type
type NotificationOutput struct {
	models.NnwdafEventsSubscriptionNotification                           // Embed original
	EventNotifications                          []EventNotificationOutput `json:"eventNotifications"` // Override type
}
