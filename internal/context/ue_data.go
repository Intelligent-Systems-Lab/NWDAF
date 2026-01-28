package context

import (
	"fmt"
	"sync"
	"time"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

// SmfSubscriptionResource manages shared SMF subscriptions with reference counting
// Per free5gc pattern: group related structures and methods in same file
// This enables multiple NWDAF subscriptions to share a single SMF subscription
type SmfSubscriptionResource struct {
	mu sync.Mutex // Protects concurrent access to this struct

	// Identify the SMF subscription
	SmfEndpoint   string
	Supi          string
	SmfSubId      string // SMF subscription ID
	CorrelationId string // Primary correlationId for UPF notifications

	// Reference tracking
	RefCount    int32           // Number of NWDAF subscriptions using this resource
	NwdafSubIds map[string]bool // Set of NWDAF subscription IDs using this resource

	// Metadata
	CreatedAt  time.Time
	LastUsedAt time.Time
}

// Lock acquires the mutex for thread-safe access
func (r *SmfSubscriptionResource) Lock() { r.mu.Lock() }

// Unlock releases the mutex
func (r *SmfSubscriptionResource) Unlock() { r.mu.Unlock() }

// GetInfo returns a copy of the resource info in a thread-safe manner
// Returns (correlationId, smfSubId, refCount)
func (r *SmfSubscriptionResource) GetInfo() (string, string, int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.CorrelationId, r.SmfSubId, r.RefCount
}

// ValidateInvariant checks that RefCount equals len(NwdafSubIds)
// Returns error if the invariant is violated
// This is a defensive check to catch bugs in reference counting logic
func (r *SmfSubscriptionResource) ValidateInvariant() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.validateInvariantLocked()
}

// validateInvariantLocked is the internal version that assumes lock is already held
// Use this when calling from a method that already holds the lock
func (r *SmfSubscriptionResource) validateInvariantLocked() error {
	expectedCount := int32(len(r.NwdafSubIds))
	if r.RefCount != expectedCount {
		return fmt.Errorf(
			"INVARIANT VIOLATION: RefCount=%d but len(NwdafSubIds)=%d (resource=%s:%s)",
			r.RefCount, expectedCount, r.SmfEndpoint, r.Supi,
		)
	}
	return nil
}

// UpfDataPoint represents a single UPF measurement with timestamp
// Used for storing raw data instead of pre-aggregating
type UpfDataPoint struct {
	Timestamp    time.Time
	UlVolume     int64
	DlVolume     int64
	UlThroughput string
	DlThroughput string
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

	// Raw UPF data points for on-demand aggregation
	RawUpfData []UpfDataPoint

	// Session lifecycle tracking
	IsActive          bool
	CommStartTime     time.Time     // PDU Session established time
	TotalCommDuration time.Duration // Total active communication time

	// UP Status tracking (for commDur calculation via ACTIVATED/DEACTIVATED)
	LastActivationTime time.Time // Last UP ACTIVATED timestamp

	SessionCount int32
}

// Lock acquires the mutex for this UE data
func (d *UeCommunicationData) Lock() { d.mu.Lock() }

// Unlock releases the mutex
func (d *UeCommunicationData) Unlock() { d.mu.Unlock() }

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

// --- SMF Resource Management (Reference Counting) ---

// BuildResourceKey creates a composite key for SMF resource lookup
// Key format: "smfEndpoint:supi"
func BuildResourceKey(smfEndpoint, supi string) string {
	return smfEndpoint + ":" + supi
}

// GetOrCreateSmfResource atomically gets or creates an SMF subscription resource
// Returns (resource, isNew) where isNew indicates if the resource was just created
// This method is thread-safe and handles concurrent access correctly
// Note: If nwdafSubId already exists in the resource, no changes are made (idempotent)
func (c *NWDAFContext) GetOrCreateSmfResource(
	smfEndpoint, supi, nwdafSubId string,
) (*SmfSubscriptionResource, bool) {
	key := BuildResourceKey(smfEndpoint, supi)

	// Try to load existing resource
	if val, ok := c.smfResources.Load(key); ok {
		resource := val.(*SmfSubscriptionResource)
		resource.addReference(nwdafSubId, key)
		return resource, false // existing resource
	}

	// Create new resource
	newResource := &SmfSubscriptionResource{
		SmfEndpoint: smfEndpoint,
		Supi:        supi,
		RefCount:    1,
		NwdafSubIds: map[string]bool{nwdafSubId: true},
		CreatedAt:   time.Now(),
		LastUsedAt:  time.Now(),
	}

	// Atomic store - only one goroutine will succeed
	actual, loaded := c.smfResources.LoadOrStore(key, newResource)

	if loaded {
		// Another goroutine created it first, use that one
		resource := actual.(*SmfSubscriptionResource)
		resource.addReference(nwdafSubId, key)
		return resource, false
	}

	logger.CtxLog.Infof("Created new SMF resource: %s", key)
	return newResource, true // new resource
}

// addReference increments the reference count if nwdafSubId is new
// This method is idempotent - calling with same nwdafSubId multiple times has no effect
func (r *SmfSubscriptionResource) addReference(nwdafSubId, key string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Idempotency check: only add if not already present
	if r.NwdafSubIds[nwdafSubId] {
		logger.CtxLog.Debugf("nwdafSubId already exists, skipping: %s (refCount=%d)", key, r.RefCount)
		return
	}

	// Add new reference
	r.RefCount++
	r.NwdafSubIds[nwdafSubId] = true
	r.LastUsedAt = time.Now()

	logger.CtxLog.Debugf("Added reference to SMF resource: %s (refCount=%d)", key, r.RefCount)

	// Validate invariant
	if err := r.validateInvariantLocked(); err != nil {
		logger.CtxLog.Error(err.Error())
	}
}

// ReleaseSmfResource decrements reference count for an SMF resource
// Returns (shouldDelete, resource) where shouldDelete indicates if refCount reached 0
// If shouldDelete is true, the resource has been removed from the map
func (c *NWDAFContext) ReleaseSmfResource(
	smfEndpoint, supi, nwdafSubId string,
) (shouldDelete bool, resource *SmfSubscriptionResource) {
	key := BuildResourceKey(smfEndpoint, supi)

	val, ok := c.smfResources.Load(key)
	if !ok {
		logger.CtxLog.Warnf("SMF resource not found for release: %s", key)
		return false, nil
	}

	resource = val.(*SmfSubscriptionResource)
	resource.mu.Lock()
	defer resource.mu.Unlock()

	// Remove NWDAF subscription reference
	delete(resource.NwdafSubIds, nwdafSubId)
	resource.RefCount--

	logger.CtxLog.Debugf("Released SMF resource: %s (refCount=%d)", key, resource.RefCount)

	if resource.RefCount <= 0 {
		// Defensive check: warn if NwdafSubIds not empty
		if len(resource.NwdafSubIds) != 0 {
			logger.CtxLog.Warnf(
				"RefCount=0 but NwdafSubIds not empty: %v (resource=%s:%s)",
				resource.NwdafSubIds, resource.SmfEndpoint, resource.Supi,
			)
		}

		c.smfResources.Delete(key)
		logger.CtxLog.Infof("Deleted SMF resource (refCount=0): %s", key)
		return true, resource
	}

	// Validate invariant (use locked version - already holding mutex)
	if err := resource.validateInvariantLocked(); err != nil {
		logger.CtxLog.Error(err.Error())
	}

	return false, resource
}

// GetSmfResource retrieves an SMF resource by composite key
func (c *NWDAFContext) GetSmfResource(smfEndpoint, supi string) (*SmfSubscriptionResource, bool) {
	key := BuildResourceKey(smfEndpoint, supi)
	if val, ok := c.smfResources.Load(key); ok {
		return val.(*SmfSubscriptionResource), true
	}
	return nil, false
}

// ClearSmfResources removes all SMF resources (for testing or reset)
func (c *NWDAFContext) ClearSmfResources() {
	c.smfResources.Range(func(key, value interface{}) bool {
		c.smfResources.Delete(key)
		return true
	})
}
