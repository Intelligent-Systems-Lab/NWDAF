package context

import (
	"fmt"
	"sync"
	"time"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

// TargetType indicates the type of subscription target
type TargetType string

const (
	TargetType_SUPI     TargetType = "SUPI"
	TargetType_GROUP_ID TargetType = "GROUP_ID"
	TargetType_ANY_UE   TargetType = "ANY_UE"
)

// UpfDataPoint represents a single UPF measurement with timestamp
// Used for storing raw data instead of pre-aggregating
type UpfDataPoint struct {
	Timestamp time.Time

	// Volume measurements (TS 29.564 VolumeMeasurement)
	TotalVolume      int64
	UlVolume         int64
	DlVolume         int64
	TotalNbOfPackets uint64
	UlNbOfPackets    uint64
	DlNbOfPackets    uint64

	// Throughput measurements (TS 29.564 ThroughputMeasurement)
	// Received as TS29571 string (e.g. "16 Kbps"), stored as base units after parsing
	UlThroughput       float64 // bps
	DlThroughput       float64 // bps
	UlPacketThroughput float64 // pps
	DlPacketThroughput float64 // pps
}

// SmfSubscription is the unified structure for SMF subscription management
// Combines former SmfSubscriptionResource (reference counting) and SubscriptionMeta (target type)
// Per TS 23.502 §4.15.4.5.2: Subscriptions are always SUPI-based (Group IDs resolved by NWDAF)
// Key: correlationId
type SmfSubscription struct {
	mu sync.Mutex

	// Identity (primary key)
	CorrelationId string

	// Target identification (always SUPI after Group ID resolution)
	TargetType TargetType // Always TargetType_SUPI after refactoring
	Supi       string     // Target SUPI

	// SMF subscription info
	SmfEndpoint string
	SmfSubId    string
	ProfileKey  string

	// Reference counting: multiple NWDAF subscriptions can share one SMF subscription
	RefCount    int32
	NwdafSubIds map[string]bool // Set of NWDAF subscription IDs using this

	// Timestamps
	CreatedAt  time.Time
	LastUpdate time.Time
}

// Lock acquires the mutex
func (s *SmfSubscription) Lock() { s.mu.Lock() }

// Unlock releases the mutex
func (s *SmfSubscription) Unlock() { s.mu.Unlock() }

// UpdateLastSeen updates the LastUpdate timestamp
func (s *SmfSubscription) UpdateLastSeen() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastUpdate = time.Now()
}

// GetInfo returns key info in thread-safe manner
func (s *SmfSubscription) GetInfo() (correlationId, smfSubId string, refCount int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.CorrelationId, s.SmfSubId, s.RefCount
}

func (s *SmfSubscription) SubscriberIDsSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]string, 0, len(s.NwdafSubIds))
	for id := range s.NwdafSubIds {
		ids = append(ids, id)
	}
	return ids
}

// AddReference adds an NWDAF subscription reference (idempotent)
func (s *SmfSubscription) AddReference(nwdafSubId string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.NwdafSubIds[nwdafSubId] {
		// Already exists
		return false
	}

	s.RefCount++
	s.NwdafSubIds[nwdafSubId] = true
	s.LastUpdate = time.Now()

	logger.CtxLog.Debugf("Added reference: correlationId=%s, nwdafSubId=%s, refCount=%d",
		s.CorrelationId, nwdafSubId, s.RefCount)
	return true
}

// RemoveReference removes an NWDAF subscription reference
// Returns true if this was the last reference (refCount reaches 0)
func (s *SmfSubscription) RemoveReference(nwdafSubId string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.NwdafSubIds[nwdafSubId] {
		logger.CtxLog.Warnf("RemoveSmfReference: missing sub=%s", nwdafSubId)
		return false
	}

	delete(s.NwdafSubIds, nwdafSubId)
	s.RefCount--
	s.LastUpdate = time.Now()

	logger.CtxLog.Debugf("Removed reference: correlationId=%s, nwdafSubId=%s, refCount=%d",
		s.CorrelationId, nwdafSubId, s.RefCount)
	return s.RefCount <= 0
}

// ValidateInvariant checks RefCount == len(NwdafSubIds)
func (s *SmfSubscription) ValidateInvariant() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	expected := int32(len(s.NwdafSubIds))
	if s.RefCount != expected {
		return fmt.Errorf("INVARIANT VIOLATION: RefCount=%d but len(NwdafSubIds)=%d (correlationId=%s)",
			s.RefCount, expected, s.CorrelationId)
	}
	return nil
}

// --- TrafficDataBucket and TrafficData (unchanged) ---

// TrafficDataBucket is a container holding all TrafficData for a single correlation ID
// This is the outer layer of the two-layer nested map structure
type TrafficDataBucket struct {
	mu sync.RWMutex

	CorrelationId string                  // Unique ID for this bucket
	dataMap       map[string]*TrafficData // ipAddress → *TrafficData

	CreatedAt  time.Time
	LastUpdate time.Time
}

// NewTrafficDataBucket creates a new empty bucket
func NewTrafficDataBucket(correlationId string) *TrafficDataBucket {
	return &TrafficDataBucket{
		CorrelationId: correlationId,
		dataMap:       make(map[string]*TrafficData),
		CreatedAt:     time.Now(),
		LastUpdate:    time.Now(),
	}
}

// Get retrieves TrafficData for an IP address
func (b *TrafficDataBucket) Get(ipAddr string) *TrafficData {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.dataMap[ipAddr]
}

// GetOrCreate retrieves or creates TrafficData for an IP address
func (b *TrafficDataBucket) GetOrCreate(ipAddr string) *TrafficData {
	b.mu.Lock()
	defer b.mu.Unlock()

	if data, exists := b.dataMap[ipAddr]; exists {
		return data
	}

	data := &TrafficData{
		CorrelationId: b.CorrelationId,
		IpAddress:     ipAddr,
		CreatedAt:     time.Now(),
	}
	b.dataMap[ipAddr] = data
	b.LastUpdate = time.Now()

	logger.CtxLog.Debugf("CreateTrafficData: corr=%s", b.CorrelationId)
	return data
}

// GetAll returns all TrafficData in this bucket
func (b *TrafficDataBucket) GetAll() []*TrafficData {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*TrafficData, 0, len(b.dataMap))
	for _, data := range b.dataMap {
		result = append(result, data)
	}
	return result
}

// Delete removes TrafficData for an IP address
func (b *TrafficDataBucket) Delete(ipAddr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.dataMap, ipAddr)
	b.LastUpdate = time.Now()
}

// Count returns number of IPs in this bucket
func (b *TrafficDataBucket) Count() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.dataMap)
}

// GetIpAddresses returns all tracked IP addresses
func (b *TrafficDataBucket) GetIpAddresses() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ips := make([]string, 0, len(b.dataMap))
	for ip := range b.dataMap {
		ips = append(ips, ip)
	}
	return ips
}

// TrafficData stores UPF traffic data for a single UE session
type TrafficData struct {
	mu sync.Mutex

	// Identity
	CorrelationId string
	IpAddress     string

	// UE Identity (enriched when available)
	Supi string
	Gpsi string

	// Session info
	Dnn     string
	Snssai  *models.Snssai
	RatType models.RatType

	// Timestamps
	CreatedAt  time.Time
	LastUpdate time.Time
}

// Lock acquires the mutex
func (d *TrafficData) Lock() { d.mu.Lock() }

// Unlock releases the mutex
func (d *TrafficData) Unlock() { d.mu.Unlock() }

// EnrichWithSupi sets SUPI if not already set
func (d *TrafficData) EnrichWithSupi(supi string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.Supi == "" && supi != "" {
		d.Supi = supi
		return true
	}
	return false
}

// AdrfSmfInfo records the SMF subscription parameters needed to reconstruct
// smfDataSub when storing UPF data to ADRF (TS 29.575 NadrfDataStoreRecord).
// Populated at SMF subscription time; keyed by correlationId.
//
// SmfSubscription serves routing/lifecycle purposes only.
// Storing here captures the actual parameters used at subscription time,
// rather than re-deriving from config which may change at runtime.
type AdrfSmfInfo struct {
	Supi        string // Target SUPI
	NotifId     string // correlationId (SMF notifId)
	NotifUri    string // SMF notification URI
	UpfNotifUri string // UPF bundled notification URI (for reconstructing eventSubs)
	NotifMethod string // e.g. "PERIODIC"
	RepPeriod   int32  // report period in seconds
}

// StoreAdrfSmfInfo saves the SMF subscription parameters for a correlationId.
func (c *NWDAFContext) StoreAdrfSmfInfo(correlationId string, info *AdrfSmfInfo) {
	c.adrfSmfInfos.Store(correlationId, info)
}

// GetAdrfSmfInfo retrieves the SMF subscription parameters for a correlationId.
// Returns nil if not found.
func (c *NWDAFContext) GetAdrfSmfInfo(correlationId string) *AdrfSmfInfo {
	if val, ok := c.adrfSmfInfos.Load(correlationId); ok {
		return val.(*AdrfSmfInfo)
	}
	return nil
}

// DeleteAdrfSmfInfo removes the SMF subscription parameters for a correlationId.
func (c *NWDAFContext) DeleteAdrfSmfInfo(correlationId string) {
	c.adrfSmfInfos.Delete(correlationId)
}

// --- NWDAFContext methods for SmfSubscription management ---

func (c *NWDAFContext) getSmfTargetProfileKey(targetId, smfEndpoint, profileKey string) string {
	return targetId + "@" + smfEndpoint + "@" + profileKey
}

func (c *NWDAFContext) GetSmfCorrelationIdForProfile(
	targetId, smfEndpoint, profileKey string,
) (string, bool) {
	key := c.getSmfTargetProfileKey(targetId, smfEndpoint, profileKey)
	if val, ok := c.smfTargetMap.Load(key); ok {
		return val.(string), true
	}
	return "", false
}

func (c *NWDAFContext) StoreSmfCorrelationIdForProfile(
	targetId, smfEndpoint, profileKey, correlationId string,
) {
	c.smfTargetMap.Store(c.getSmfTargetProfileKey(targetId, smfEndpoint, profileKey), correlationId)
}

func (c *NWDAFContext) RemoveSmfCorrelationIdForProfile(targetId, smfEndpoint, profileKey string) {
	c.smfTargetMap.Delete(c.getSmfTargetProfileKey(targetId, smfEndpoint, profileKey))
}

// GetOrCreateSmfSubscription gets or creates an SMF subscription
// Returns (subscription, isNew)
func (c *NWDAFContext) GetOrCreateSmfSubscription(correlationId, nwdafSubId string) (*SmfSubscription, bool) {
	// Try to load existing
	if val, ok := c.smfSubscriptions.Load(correlationId); ok {
		sub := val.(*SmfSubscription)
		sub.AddReference(nwdafSubId)
		return sub, false
	}

	// Create new
	newSub := &SmfSubscription{
		CorrelationId: correlationId,
		RefCount:      1,
		NwdafSubIds:   map[string]bool{nwdafSubId: true},
		CreatedAt:     time.Now(),
		LastUpdate:    time.Now(),
	}

	actual, loaded := c.smfSubscriptions.LoadOrStore(correlationId, newSub)
	if loaded {
		// Another goroutine created it first
		sub := actual.(*SmfSubscription)
		sub.AddReference(nwdafSubId)
		return sub, false
	}

	logger.CtxLog.Debugf("CreateSmfSubscription: corr=%s", correlationId)
	return newSub, true
}

// GetSmfSubscription retrieves an SMF subscription by correlation ID
func (c *NWDAFContext) GetSmfSubscription(correlationId string) *SmfSubscription {
	if val, ok := c.smfSubscriptions.Load(correlationId); ok {
		return val.(*SmfSubscription)
	}
	return nil
}

func (c *NWDAFContext) HasActiveNwdafSubscriber(correlationId string) bool {
	smfSubscription := c.GetSmfSubscription(correlationId)
	if smfSubscription == nil {
		return false
	}

	for _, subscriptionID := range smfSubscription.SubscriberIDsSnapshot() {
		subscription := c.GetSubscription(subscriptionID)
		if subscription == nil {
			continue
		}
		_, _, _, active := subscription.RuntimeSnapshot()
		if active {
			return true
		}
	}
	return false
}

// Identifier returns the target identifier string for logging and mapping
// Per TS 23.502: Subscriptions are always SUPI-based
func (s *SmfSubscription) Identifier() string {
	return "supi=" + s.Supi
}

// ReleaseSmfSubscription decrements reference and returns true if last reference
func (c *NWDAFContext) ReleaseSmfSubscription(
	correlationId, nwdafSubId string,
) (shouldDelete bool, sub *SmfSubscription) {
	val, ok := c.smfSubscriptions.Load(correlationId)
	if !ok {
		logger.CtxLog.Warnf("ReleaseSmfSubscription: missing corr=%s", correlationId)
		return false, nil
	}

	sub = val.(*SmfSubscription)
	isLast := sub.RemoveReference(nwdafSubId)

	if isLast {
		c.smfSubscriptions.Delete(correlationId)

		// Also cleanup the target mapping
		// We need to reconstruct the target identifier and endpoint
		targetId := sub.Identifier()
		c.RemoveSmfCorrelationIdForProfile(targetId, sub.SmfEndpoint, sub.ProfileKey)

		logger.CtxLog.Debugf("DeleteSmfSubscription: corr=%s target=%s", correlationId, targetId)
		return true, sub
	}

	return false, sub
}

// DeleteSmfSubscription removes an SMF subscription
func (c *NWDAFContext) DeleteSmfSubscription(correlationId string) {
	c.smfSubscriptions.Delete(correlationId)
	logger.CtxLog.Debugf("Deleted SmfSubscription: %s", correlationId)
}

// ClearSmfSubscriptions removes all (for testing)
func (c *NWDAFContext) ClearSmfSubscriptions() {
	c.smfSubscriptions.Range(func(key, value interface{}) bool {
		c.smfSubscriptions.Delete(key)
		return true
	})
}

// --- NWDAFContext methods for Traffic Data management ---

// GetOrCreateTrafficBucket gets or creates a bucket for correlation ID
func (c *NWDAFContext) GetOrCreateTrafficBucket(correlationId string) *TrafficDataBucket {
	newBucket := NewTrafficDataBucket(correlationId)

	actual, loaded := c.trafficDataStore.LoadOrStore(correlationId, newBucket)
	if !loaded {
		logger.CtxLog.Debugf("Created TrafficDataBucket: %s", correlationId)
	}
	return actual.(*TrafficDataBucket)
}

// GetTrafficBucket retrieves a bucket by correlation ID
func (c *NWDAFContext) GetTrafficBucket(correlationId string) *TrafficDataBucket {
	if val, ok := c.trafficDataStore.Load(correlationId); ok {
		return val.(*TrafficDataBucket)
	}
	return nil
}

// DeleteTrafficBucket removes entire bucket
func (c *NWDAFContext) DeleteTrafficBucket(correlationId string) {
	c.trafficDataStore.Delete(correlationId)
	logger.CtxLog.Debugf("Deleted TrafficDataBucket: %s", correlationId)
}

// GetOrCreateTrafficData convenience method
func (c *NWDAFContext) GetOrCreateTrafficData(correlationId, ipAddr string) *TrafficData {
	bucket := c.GetOrCreateTrafficBucket(correlationId)
	return bucket.GetOrCreate(ipAddr)
}

// GetAllTrafficDataForCorrelation returns all TrafficData for a correlation
func (c *NWDAFContext) GetAllTrafficDataForCorrelation(correlationId string) []*TrafficData {
	bucket := c.GetTrafficBucket(correlationId)
	if bucket == nil {
		return nil
	}
	return bucket.GetAll()
}

// ClearTrafficDataStore removes all traffic data (for testing)
func (c *NWDAFContext) ClearTrafficDataStore() {
	c.trafficDataStore.Range(func(key, value interface{}) bool {
		c.trafficDataStore.Delete(key)
		return true
	})
}

// --- Query by NWDAF Subscription ---

// GetCorrelationIdsByNwdafSubId returns all correlation IDs for an NWDAF subscription
// Efficient O(1) lookup via nwdafSubResourcesMap
func (c *NWDAFContext) GetCorrelationIdsByNwdafSubId(nwdafSubId string) []string {
	resources := c.GetNwdafSubResources(nwdafSubId)
	if len(resources) == 0 {
		return nil
	}

	ids := make([]string, 0, len(resources))
	for _, r := range resources {
		ids = append(ids, r.CorrelationId)
	}
	return ids
}

// GetTrafficBucketsByNwdafSubId returns all traffic buckets for an NWDAF subscription
func (c *NWDAFContext) GetTrafficBucketsByNwdafSubId(nwdafSubId string) []*TrafficDataBucket {
	corrIds := c.GetCorrelationIdsByNwdafSubId(nwdafSubId)
	if len(corrIds) == 0 {
		return nil
	}

	buckets := make([]*TrafficDataBucket, 0, len(corrIds))
	for _, corrId := range corrIds {
		if bucket := c.GetTrafficBucket(corrId); bucket != nil {
			buckets = append(buckets, bucket)
		}
	}
	return buckets
}

// GetTrafficDataByNwdafSubId returns all traffic data for an NWDAF subscription
func (c *NWDAFContext) GetTrafficDataByNwdafSubId(nwdafSubId string) []*TrafficData {
	buckets := c.GetTrafficBucketsByNwdafSubId(nwdafSubId)
	if len(buckets) == 0 {
		return nil
	}

	var result []*TrafficData
	for _, bucket := range buckets {
		result = append(result, bucket.GetAll()...)
	}
	return result
}

// --- NWDAF Subscription Resource Tracking ---

// NwdafSubResource tracks a single SMF resource used by an NWDAF subscription
// Used for:
//   - Cleanup: proper resource release when subscription is deleted
//   - Data query: correlationId lookup to retrieve traffic data
//   - Group tracking: OriginalGroupId for analytics aggregation
type NwdafSubResource struct {
	SmfEndpoint     string     // SMF endpoint URL
	TargetType      TargetType // Always SUPI after Group ID resolution
	Supi            string     // Target SUPI
	CorrelationId   string     // CorrelationId for this SMF subscription
	OriginalGroupId string     // Source Group ID (if resolved from group subscription)
	CreatedAt       time.Time  // When resource was added
}

// AddNwdafSubResource adds a resource tracking entry for an NWDAF subscription
func (c *NWDAFContext) AddNwdafSubResource(nwdafSubId string, resource NwdafSubResource) {
	var resources []NwdafSubResource
	if val, ok := c.nwdafSubResourcesMap.Load(nwdafSubId); ok {
		resources = val.([]NwdafSubResource)
	}
	for _, existing := range resources {
		if existing.CorrelationId == resource.CorrelationId &&
			existing.OriginalGroupId == resource.OriginalGroupId {
			return
		}
	}
	resources = append(resources, resource)
	c.nwdafSubResourcesMap.Store(nwdafSubId, resources)

	logger.CtxLog.Debugf("AddNwdafSubResource: sub=%s resources=%d", nwdafSubId, len(resources))
}

func (c *NWDAFContext) SetNwdafSubResources(nwdafSubId string, resources []NwdafSubResource) {
	c.nwdafSubResourcesMap.Store(nwdafSubId, append([]NwdafSubResource(nil), resources...))
}

// GetNwdafSubResources retrieves all resources for an NWDAF subscription
func (c *NWDAFContext) GetNwdafSubResources(nwdafSubId string) []NwdafSubResource {
	if val, ok := c.nwdafSubResourcesMap.Load(nwdafSubId); ok {
		return val.([]NwdafSubResource)
	}
	return nil
}

// DeleteNwdafSubResources removes all resource tracking for an NWDAF subscription
func (c *NWDAFContext) DeleteNwdafSubResources(nwdafSubId string) {
	c.nwdafSubResourcesMap.Delete(nwdafSubId)
	logger.CtxLog.Debugf("Deleted resources for nwdafSubId=%s", nwdafSubId)
}

// ClearNwdafSubResourcesMap removes all resource tracking (for testing)
func (c *NWDAFContext) ClearNwdafSubResourcesMap() {
	c.nwdafSubResourcesMap.Range(func(key, value interface{}) bool {
		c.nwdafSubResourcesMap.Delete(key)
		return true
	})
}

// GetGroupIdByCorrelationId finds the original Group ID associated with a Correlation ID
// Optimized: uses the SmfSubscription's known NWDAF subscribers instead of scanning all resources
func (c *NWDAFContext) GetGroupIdByCorrelationId(correlationId string) string {
	sub := c.GetSmfSubscription(correlationId)
	if sub == nil {
		return ""
	}

	sub.Lock()
	defer sub.Unlock()

	for nwdafSubId := range sub.NwdafSubIds {
		resources := c.GetNwdafSubResources(nwdafSubId)
		for _, r := range resources {
			if r.CorrelationId == correlationId && r.OriginalGroupId != "" {
				return r.OriginalGroupId
			}
		}
	}

	return ""
}
