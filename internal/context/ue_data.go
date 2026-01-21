package context

import (
	"sync"
	"time"

	"github.com/free5gc/openapi/models"
)

// SmfSubscription represents a subscription to SMF event exposure
type SmfSubscription struct {
	SubscriptionId string
	SmfEndpoint    string
	TargetSupi     string
	NotifId        string
	Events         []string
	CreatedAt      time.Time
}

// UeCommunicationData stores collected communication data for a UE
type UeCommunicationData struct {
	mu sync.Mutex // Protects concurrent access to this struct

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

// Lock acquires the mutex for this UE data
func (d *UeCommunicationData) Lock() { d.mu.Lock() }

// Unlock releases the mutex
func (d *UeCommunicationData) Unlock() { d.mu.Unlock() }

// --- SMF Subscription Management ---

// StoreSmfSubscription stores an SMF subscription
func (c *NWDAFContext) StoreSmfSubscription(sub *SmfSubscription) {
	c.smfSubscriptions.Store(sub.SubscriptionId, sub)
}

// GetSmfSubscription retrieves an SMF subscription by ID
func (c *NWDAFContext) GetSmfSubscription(subscriptionId string) (*SmfSubscription, bool) {
	if value, ok := c.smfSubscriptions.Load(subscriptionId); ok {
		return value.(*SmfSubscription), true
	}
	return nil, false
}

// DeleteSmfSubscription removes an SMF subscription
func (c *NWDAFContext) DeleteSmfSubscription(subscriptionId string) {
	c.smfSubscriptions.Delete(subscriptionId)
}

// --- UE Data Management ---

// StoreUeData stores or updates UE communication data
func (c *NWDAFContext) StoreUeData(data *UeCommunicationData) {
	c.ueDataStore.Store(data.Supi, data)
}

// GetUeData retrieves UE communication data by SUPI
func (c *NWDAFContext) GetUeData(supi string) (*UeCommunicationData, bool) {
	if value, ok := c.ueDataStore.Load(supi); ok {
		return value.(*UeCommunicationData), true
	}
	return nil, false
}

// GetOrCreateUeData retrieves existing data or creates new entry atomically
// Uses sync.Map.LoadOrStore to prevent race conditions when multiple goroutines
// try to create data for the same SUPI concurrently
func (c *NWDAFContext) GetOrCreateUeData(supi string) *UeCommunicationData {
	newData := &UeCommunicationData{
		Supi:      supi,
		StartTime: time.Now(),
		Events:    make([]models.SmfEventExposureEventNotification, 0),
	}

	// LoadOrStore returns the existing value if present, otherwise stores and returns newData
	// This is atomic - only one goroutine's newData will be stored
	actual, _ := c.ueDataStore.LoadOrStore(supi, newData)
	return actual.(*UeCommunicationData)
}

// AppendEvent adds an SMF event to UE data
func (c *NWDAFContext) AppendEvent(supi string, event models.SmfEventExposureEventNotification) {
	data := c.GetOrCreateUeData(supi)
	data.Lock()
	defer data.Unlock()

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
}

// ClearUeData removes all UE data (for testing or reset)
func (c *NWDAFContext) ClearUeData() {
	c.ueDataStore.Range(func(key, value interface{}) bool {
		c.ueDataStore.Delete(key)
		return true
	})
}

// ClearSmfSubscriptions removes all SMF subscriptions (for testing or reset)
func (c *NWDAFContext) ClearSmfSubscriptions() {
	c.smfSubscriptions.Range(func(key, value interface{}) bool {
		c.smfSubscriptions.Delete(key)
		return true
	})
}
