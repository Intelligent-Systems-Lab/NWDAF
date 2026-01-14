// Package collector provides data collection functionality for NWDAF
// Collects UE communication data from SMF via Nsmf_EventExposure service
package collector

import (
	"sync"
	"time"

	"github.com/free5gc/openapi/models"
)

var collectorContext CollectorContext

// CollectorContext stores SMF subscriptions and collected UE data
type CollectorContext struct {
	// Subscriptions to SMF (keyed by subscriptionId)
	SmfSubscriptions sync.Map // map[string]*SmfSubscription

	// Collected UE data (keyed by SUPI)
	UeDataStore sync.Map // map[string]*UeCommunicationData
}

// SmfSubscription represents a subscription to SMF event exposure
type SmfSubscription struct {
	SubscriptionId string
	SmfEndpoint    string
	TargetSupi     string
	NotifId        string
	Events         []models.SmfEvent
	CreatedAt      time.Time
}

// UeCommunicationData stores collected communication data for a UE
type UeCommunicationData struct {
	Supi       string
	Dnn        string
	Snssai     *models.Snssai
	PduSessId  int32
	RatType    models.RatType
	StartTime  time.Time
	LastUpdate time.Time

	// Raw events received from SMF
	Events []models.SmfEventExposureEventNotification

	// Session lifecycle tracking
	IsActive          bool
	CommStartTime     time.Time     // PDU Session established time
	TotalCommDuration time.Duration // Total active communication time

	// UP Status tracking (for commDur calculation via ACTIVATED/DEACTIVATED)
	LastActivationTime time.Time // Last UP ACTIVATED timestamp

	// Aggregated metrics from UPF_EVENT
	TotalUlVolume int64 // Total uplink volume (bytes)
	TotalDlVolume int64 // Total downlink volume (bytes)
	SessionCount  int32

	// Throughput measurements from UPF_EVENT
	LastUlThroughput string // Latest uplink throughput (e.g., "10 Mbps")
	LastDlThroughput string // Latest downlink throughput
}

// GetSelf returns the singleton CollectorContext
func GetSelf() *CollectorContext {
	return &collectorContext
}

// StoreSubscription stores an SMF subscription
func (c *CollectorContext) StoreSubscription(sub *SmfSubscription) {
	c.SmfSubscriptions.Store(sub.SubscriptionId, sub)
}

// GetSubscription retrieves an SMF subscription by ID
func (c *CollectorContext) GetSubscription(subscriptionId string) (*SmfSubscription, bool) {
	if value, ok := c.SmfSubscriptions.Load(subscriptionId); ok {
		return value.(*SmfSubscription), true
	}
	return nil, false
}

// DeleteSubscription removes an SMF subscription
func (c *CollectorContext) DeleteSubscription(subscriptionId string) {
	c.SmfSubscriptions.Delete(subscriptionId)
}

// StoreUeData stores or updates UE communication data
func (c *CollectorContext) StoreUeData(data *UeCommunicationData) {
	c.UeDataStore.Store(data.Supi, data)
}

// GetUeData retrieves UE communication data by SUPI
func (c *CollectorContext) GetUeData(supi string) (*UeCommunicationData, bool) {
	if value, ok := c.UeDataStore.Load(supi); ok {
		return value.(*UeCommunicationData), true
	}
	return nil, false
}

// GetOrCreateUeData retrieves existing data or creates new entry
func (c *CollectorContext) GetOrCreateUeData(supi string) *UeCommunicationData {
	if data, ok := c.GetUeData(supi); ok {
		return data
	}
	newData := &UeCommunicationData{
		Supi:      supi,
		StartTime: time.Now(),
		Events:    make([]models.SmfEventExposureEventNotification, 0),
	}
	c.StoreUeData(newData)
	return newData
}

// AppendEvent adds an SMF event to UE data
func (c *CollectorContext) AppendEvent(supi string, event models.SmfEventExposureEventNotification) {
	data := c.GetOrCreateUeData(supi)
	data.Events = append(data.Events, event)
	data.LastUpdate = time.Now()

	// Update from event metadata
	if event.Dnn != "" {
		data.Dnn = event.Dnn
	}
	if event.Snssai != nil {
		data.Snssai = event.Snssai
	}
	if event.PduSeId != 0 {
		data.PduSessId = event.PduSeId
	}

	c.StoreUeData(data)
}

// ClearUeData removes all UE data (for testing or reset)
func (c *CollectorContext) ClearUeData() {
	c.UeDataStore.Range(func(key, value interface{}) bool {
		c.UeDataStore.Delete(key)
		return true
	})
}

// ClearSubscriptions removes all subscriptions (for testing or reset)
func (c *CollectorContext) ClearSubscriptions() {
	c.SmfSubscriptions.Range(func(key, value interface{}) bool {
		c.SmfSubscriptions.Delete(key)
		return true
	})
}
