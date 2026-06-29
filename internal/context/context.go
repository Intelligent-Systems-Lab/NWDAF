package context

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

var nwdafContext *NWDAFContext

func Init() {
	nwdafContext = &NWDAFContext{
		NfId:          uuid.New().String(),
		subscriptions: make(map[string]*Subscription),
	}
	logger.CtxLog.Infof("NWDAF Context initialized with NfId: %s", nwdafContext.NfId)
}

func GetSelf() *NWDAFContext {
	return nwdafContext
}

type NWDAFContext struct {
	NfId      string
	NwdafName string

	// Subscriptions storage (NWDAF consumer subscriptions)
	mu            sync.RWMutex
	subscriptions map[string]*Subscription

	// NWDAF subscription resources: nwdafSubId → []NwdafSubResource
	// Tracks SMF resources per NWDAF subscription for:
	//   - Cleanup: proper resource release on subscription deletion
	//   - Data query: correlationId lookup for traffic data retrieval
	//   - Group tracking: OriginalGroupId for analytics aggregation
	nwdafSubResourcesMap sync.Map // map[string][]NwdafSubResource

	// --- Unified Storage ---

	// SMF subscriptions: correlationId → *SmfSubscription
	// Unified structure with reference counting and target type support
	smfSubscriptions sync.Map

	// Traffic data: correlationId → *TrafficDataBucket
	// Two-layer nested map: bucket contains map[ipAddress]*TrafficData
	trafficDataStore sync.Map

	// SMF target mapping: targetIdentifier + "@" + smfEndpoint → correlationId
	// Used to reuse active SMF subscriptions for the same target and endpoint
	smfTargetMap sync.Map

	// --- ML Model Storage ---

	// ML model info: nwdafSubId → *MlModelInfo
	// Tracks ML model state per subscription for ML-based analytics
	mlModelInfoStore sync.Map

	// --- Group Resolution ---

	// GroupResolver for resolving Group ID → SUPI list
	// Per TS 23.502 §4.15.4.5.2
	groupResolver *GroupResolver

	// --- Accuracy Monitoring ---

	// Shared model registry: modelUrl → *SharedModelInfo
	// Tracks loaded models to avoid duplicate ML service initialization
	sharedModelRegistry sync.Map

	// Per-model accuracy stores: modelUrl → *ModelAccuracyStore
	// Only used when accuracy monitoring is enabled
	modelAccuracyStores sync.Map

	// ADRF SMF info: correlationId → *AdrfSmfInfo
	// Captures SMF subscription parameters at subscription time for ADRF storage.
	adrfSmfInfos sync.Map

	// Sequential correlation ID counter
	correlationIdCounter atomic.Int64
}

// Subscription represents an individual event subscription
type Subscription struct {
	ID              string
	NotificationURI string
	NotifCorrId     string
	EventSubs       []models.NwdafEventsSubscriptionEventSubscription
	EvtReq          *models.ReportingInformation
	CreatedAt       time.Time
	UpdatedAt       time.Time

	// Notification control (populated from EvtReq)
	NotifMethod  string     // PERIODIC, ONE_TIME, ON_EVENT_DETECTION
	RepPeriod    int32      // Repetition period in seconds
	MaxReportNbr int32      // Max number of reports (0 = unlimited)
	ReportCount  int32      // Current report count
	MonDur       *time.Time // Monitoring duration expiry

	// Status
	IsActive bool // Whether subscription is active

	// Scheduler reference (managed externally to avoid import cycle)
	Scheduler interface{ Stop() }
}

// NewSubscriptionId generates a new unique subscription ID
func NewSubscriptionId() string {
	return uuid.New().String()
}

// AddSubscription adds a new subscription to the context
func (c *NWDAFContext) AddSubscription(sub *Subscription) {
	c.mu.Lock()
	defer c.mu.Unlock()

	sub.CreatedAt = time.Now()
	sub.UpdatedAt = sub.CreatedAt
	c.subscriptions[sub.ID] = sub

	logger.CtxLog.Debugf("AddSubscription: sub=%s", sub.ID)
}

// GetSubscription retrieves a subscription by ID
func (c *NWDAFContext) GetSubscription(id string) *Subscription {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.subscriptions[id]
}

// UpdateSubscription updates an existing subscription
func (c *NWDAFContext) UpdateSubscription(sub *Subscription) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.subscriptions[sub.ID]; !exists {
		return false
	}

	sub.UpdatedAt = time.Now()
	c.subscriptions[sub.ID] = sub

	logger.CtxLog.Debugf("UpdateSubscription: sub=%s", sub.ID)
	return true
}

// DeleteSubscription removes a subscription by ID
func (c *NWDAFContext) DeleteSubscription(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.subscriptions[id]; !exists {
		return false
	}

	delete(c.subscriptions, id)
	logger.CtxLog.Debugf("DeleteSubscription: sub=%s", id)
	return true
}

// GetAllSubscriptions returns all subscriptions
func (c *NWDAFContext) GetAllSubscriptions() []*Subscription {
	c.mu.RLock()
	defer c.mu.RUnlock()

	subs := make([]*Subscription, 0, len(c.subscriptions))
	for _, sub := range c.subscriptions {
		subs = append(subs, sub)
	}
	return subs
}

// StopAllSubscriptionSchedulers stops every active subscription scheduler.
// The stop calls run outside the context lock so scheduler shutdown can block
// without stalling subscription reads or updates.
func (c *NWDAFContext) StopAllSubscriptionSchedulers() int {
	c.mu.Lock()
	schedulers := make([]interface{ Stop() }, 0, len(c.subscriptions))
	for _, sub := range c.subscriptions {
		if sub.Scheduler == nil {
			continue
		}

		schedulers = append(schedulers, sub.Scheduler)
		sub.Scheduler = nil
	}
	c.mu.Unlock()

	for _, scheduler := range schedulers {
		scheduler.Stop()
	}

	if len(schedulers) > 0 {
		logger.CtxLog.Infof("Stopped %d subscription schedulers", len(schedulers))
	}

	return len(schedulers)
}

// SubscriptionCount returns the number of active subscriptions
func (c *NWDAFContext) SubscriptionCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.subscriptions)
}

// --- ML Model Info Methods (per-subscription) ---

// SetMlModelInfo stores ML model info for a subscription
func (c *NWDAFContext) SetMlModelInfo(nwdafSubId string, info *MlModelInfo) {
	c.mlModelInfoStore.Store(nwdafSubId, info)
	logger.CtxLog.Debugf("Stored ML model info for subscription %s", nwdafSubId)
}

// GetMlModelInfo retrieves ML model info for a subscription
func (c *NWDAFContext) GetMlModelInfo(nwdafSubId string) *MlModelInfo {
	if val, ok := c.mlModelInfoStore.Load(nwdafSubId); ok {
		return val.(*MlModelInfo)
	}
	return nil
}

// DeleteMlModelInfo removes ML model info for a subscription
func (c *NWDAFContext) DeleteMlModelInfo(nwdafSubId string) {
	c.mlModelInfoStore.Delete(nwdafSubId)
	logger.CtxLog.Debugf("Deleted ML model info for subscription %s", nwdafSubId)
}

// ============================================================================
// Group Resolver Methods
// ============================================================================

// SetGroupResolver sets the GroupResolver for Group ID resolution
func (c *NWDAFContext) SetGroupResolver(resolver *GroupResolver) {
	c.groupResolver = resolver
}

// GetGroupResolver returns the GroupResolver
func (c *NWDAFContext) GetGroupResolver() *GroupResolver {
	return c.groupResolver
}

// ============================================================================
// Shared Model Registry Methods (always active)
// ============================================================================

// GetOrCreateSharedModel returns existing or creates new SharedModelInfo
// Returns (model, isNew)
func (c *NWDAFContext) GetOrCreateSharedModel(
	modelUrl string, event models.NwdafEvent,
) (*SharedModelInfo, bool) {
	newModel := NewSharedModelInfo(modelUrl, event)
	actual, loaded := c.sharedModelRegistry.LoadOrStore(modelUrl, newModel)
	return actual.(*SharedModelInfo), !loaded
}

// GetSharedModel returns SharedModelInfo for modelUrl (nil if not exists)
func (c *NWDAFContext) GetSharedModel(modelUrl string) *SharedModelInfo {
	if val, ok := c.sharedModelRegistry.Load(modelUrl); ok {
		return val.(*SharedModelInfo)
	}
	return nil
}

// DeleteSharedModel removes SharedModelInfo for modelUrl
func (c *NWDAFContext) DeleteSharedModel(modelUrl string) {
	c.sharedModelRegistry.Delete(modelUrl)
	logger.CtxLog.Debugf("Deleted shared model: %s", modelUrl)
}

// ============================================================================
// Per-Model Accuracy Store Methods (optional monitoring)
// ============================================================================

// GetOrCreateModelAccuracyStore returns existing or creates new store
// Returns (store, isNew)
func (c *NWDAFContext) GetOrCreateModelAccuracyStore(
	modelUrl string,
) (*ModelAccuracyStore, bool) {
	newStore := NewModelAccuracyStore(modelUrl)
	actual, loaded := c.modelAccuracyStores.LoadOrStore(modelUrl, newStore)
	return actual.(*ModelAccuracyStore), !loaded
}

// GetModelAccuracyStore returns store for modelUrl (nil if not exists)
func (c *NWDAFContext) GetModelAccuracyStore(
	modelUrl string,
) *ModelAccuracyStore {
	if val, ok := c.modelAccuracyStores.Load(modelUrl); ok {
		return val.(*ModelAccuracyStore)
	}
	return nil
}

// DeleteModelAccuracyStore removes and stops store for modelUrl
func (c *NWDAFContext) DeleteModelAccuracyStore(modelUrl string) {
	if val, ok := c.modelAccuracyStores.LoadAndDelete(modelUrl); ok {
		val.(*ModelAccuracyStore).StopMonitor()
	}
	logger.CtxLog.Debugf("Deleted accuracy store: %s", modelUrl)
}

// NewCorrelationId returns a sequential, predictable correlation ID (corr-1, corr-2, ...)
func (c *NWDAFContext) NewCorrelationId() string {
	n := c.correlationIdCounter.Add(1)
	return fmt.Sprintf("corr-%d", n)
}
