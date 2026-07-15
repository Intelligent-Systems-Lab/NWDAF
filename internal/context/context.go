package context

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

var nwdafContext *NWDAFContext

const (
	nwdafEventsSubscriptionAPIVersion     = "v1"
	nwdafEventsSubscriptionAPIFullVersion = "1.0.0"
)

func Init() {
	nwdafContext = &NWDAFContext{
		NfId:                uuid.New().String(),
		nfServiceInstanceId: uuid.New().String(),
		subscriptions:       make(map[string]*Subscription),
	}
	logger.CtxLog.Infof("NWDAF Context initialized with NfId: %s", nwdafContext.NfId)
}

func GetSelf() *NWDAFContext {
	return nwdafContext
}

type NWDAFContext struct {
	NfId      string
	NwdafName string

	nfManagementMu      sync.RWMutex
	nfServiceInstanceId string
	nrfUri              string
	nfProfile           models.NrfNfManagementNfProfile
	registered          bool
	registrationUri     string
	oauth2Required      bool
	heartBeatTimer      int32

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

	// ADRF SMF info: correlationId → *AdrfSmfInfo
	// Captures SMF subscription parameters at subscription time for ADRF storage.
	adrfSmfInfos sync.Map

	// Sequential correlation ID counter
	correlationIdCounter atomic.Int64
}

type NFRegistrationState struct {
	Registered     bool
	ResourceURI    string
	OAuth2Required bool
	HeartBeatTimer int32
}

func (c *NWDAFContext) ConfigureNFManagement(
	nrfUri string,
	nwdafName string,
	sbiUri string,
	sbiScheme string,
	registerIPv4 string,
	sbiPort int,
) error {
	if c == nil {
		return fmt.Errorf("NWDAF context is nil")
	}
	if nrfUri == "" {
		return fmt.Errorf("NRF URI is required")
	}
	if c.NfId == "" {
		return fmt.Errorf("NF instance ID is required")
	}
	if c.nfServiceInstanceId == "" {
		c.nfServiceInstanceId = uuid.New().String()
	}
	registerIPv4 = strings.TrimSpace(registerIPv4)
	parsedRegisterIPv4 := net.ParseIP(registerIPv4)
	if strings.Contains(registerIPv4, ":") || parsedRegisterIPv4 == nil ||
		parsedRegisterIPv4.To4() == nil || parsedRegisterIPv4.IsUnspecified() {
		return fmt.Errorf("SBI registration address must be a valid non-wildcard IPv4 address")
	}
	if sbiPort <= 0 || sbiPort > 65535 {
		return fmt.Errorf("SBI port must be between 1 and 65535")
	}

	var scheme models.UriScheme
	switch sbiScheme {
	case string(models.UriScheme_HTTP):
		scheme = models.UriScheme_HTTP
	case string(models.UriScheme_HTTPS):
		scheme = models.UriScheme_HTTPS
	default:
		return fmt.Errorf("unsupported SBI scheme %q", sbiScheme)
	}

	profile := models.NrfNfManagementNfProfile{
		NfInstanceId:   c.NfId,
		NfInstanceName: nwdafName,
		NfType:         models.NrfNfManagementNfType_NWDAF,
		NfStatus:       models.NrfNfManagementNfStatus_REGISTERED,
		Ipv4Addresses:  []string{registerIPv4},
		NwdafInfo: &models.NwdafInfo{
			NwdafEvents: []models.NwdafEvent{models.NwdafEvent_UE_COMMUNICATION},
		},
		NfServices: []models.NrfNfManagementNfService{
			{
				ServiceInstanceId: c.nfServiceInstanceId,
				ServiceName:       models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION,
				Versions: []models.NfServiceVersion{
					{
						ApiVersionInUri: nwdafEventsSubscriptionAPIVersion,
						ApiFullVersion:  nwdafEventsSubscriptionAPIFullVersion,
					},
				},
				Scheme:          scheme,
				NfServiceStatus: models.NfServiceStatus_REGISTERED,
				IpEndPoints: []models.IpEndPoint{
					{
						Ipv4Address: registerIPv4,
						Transport:   models.NrfNfManagementTransportProtocol_TCP,
						Port:        int32(sbiPort),
					},
				},
				ApiPrefix: sbiUri,
			},
		},
	}

	c.nfManagementMu.Lock()
	defer c.nfManagementMu.Unlock()
	c.NwdafName = nwdafName
	c.nrfUri = nrfUri
	c.nfProfile = profile
	c.registered = false
	c.registrationUri = ""
	c.oauth2Required = false
	c.heartBeatTimer = 0
	return nil
}

func (c *NWDAFContext) NrfUri() string {
	if c == nil {
		return ""
	}
	c.nfManagementMu.RLock()
	defer c.nfManagementMu.RUnlock()
	return c.nrfUri
}

func (c *NWDAFContext) NFProfile() models.NrfNfManagementNfProfile {
	if c == nil {
		return models.NrfNfManagementNfProfile{}
	}
	c.nfManagementMu.RLock()
	defer c.nfManagementMu.RUnlock()
	return c.nfProfile
}

func (c *NWDAFContext) MarkRegistered(resourceURI string) {
	if c == nil {
		return
	}
	c.nfManagementMu.Lock()
	defer c.nfManagementMu.Unlock()
	c.registered = true
	c.registrationUri = resourceURI
	c.oauth2Required = false
}

func (c *NWDAFContext) RecordOAuth2Required(resourceURI string) {
	if c == nil {
		return
	}
	c.nfManagementMu.Lock()
	defer c.nfManagementMu.Unlock()
	c.registered = false
	c.registrationUri = resourceURI
	c.oauth2Required = true
}

func (c *NWDAFContext) RecordHeartBeatTimer(heartBeatTimer int32) {
	if c == nil {
		return
	}
	c.nfManagementMu.Lock()
	defer c.nfManagementMu.Unlock()
	c.heartBeatTimer = heartBeatTimer
}

func (c *NWDAFContext) MarkDeregistered() {
	if c == nil {
		return
	}
	c.nfManagementMu.Lock()
	defer c.nfManagementMu.Unlock()
	c.registered = false
	c.registrationUri = ""
}

func (c *NWDAFContext) RegistrationState() NFRegistrationState {
	if c == nil {
		return NFRegistrationState{}
	}
	c.nfManagementMu.RLock()
	defer c.nfManagementMu.RUnlock()
	return NFRegistrationState{
		Registered:     c.registered,
		ResourceURI:    c.registrationUri,
		OAuth2Required: c.oauth2Required,
		HeartBeatTimer: c.heartBeatTimer,
	}
}

// Subscription represents an individual event subscription
type Subscription struct {
	runtimeMu sync.Mutex

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

	RuntimeRevision        int64
	CollectionRequirements CollectionRequirements
	ObservationSourceIDs   []string
	deliveredReportIDs     map[string]struct{}
	inFlightReportIDs      map[string]struct{}
	lastReportSequence     int64
}

type CollectionRequirements struct {
	SamplingIntervalSeconds int
	RequiredMeasurements    []string
}

type RuntimeCompletionDisposition int

const (
	RuntimeCompletionCompleted RuntimeCompletionDisposition = iota
	RuntimeCompletionAlreadyInactive
	RuntimeCompletionStale
	RuntimeCompletionFuture
)

func (s *Subscription) SetRuntime(
	revision int64,
	requirements CollectionRequirements,
	sourceIDs []string,
) {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	s.RuntimeRevision = revision
	s.CollectionRequirements = requirements
	s.ObservationSourceIDs = append([]string(nil), sourceIDs...)
}

func (s *Subscription) RuntimeSnapshot() (int64, CollectionRequirements, []string, bool) {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	return s.RuntimeRevision, CollectionRequirements{
		SamplingIntervalSeconds: s.CollectionRequirements.SamplingIntervalSeconds,
		RequiredMeasurements:    append([]string(nil), s.CollectionRequirements.RequiredMeasurements...),
	}, append([]string(nil), s.ObservationSourceIDs...), s.IsActive
}

func (s *Subscription) CollectionRequirementsSnapshot() CollectionRequirements {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	return CollectionRequirements{
		SamplingIntervalSeconds: s.CollectionRequirements.SamplingIntervalSeconds,
		RequiredMeasurements:    append([]string(nil), s.CollectionRequirements.RequiredMeasurements...),
	}
}

func (s *Subscription) SetActive(active bool) {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	s.IsActive = active
}

func (s *Subscription) CompleteRuntime(revision int64) RuntimeCompletionDisposition {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()

	switch {
	case revision < s.RuntimeRevision:
		return RuntimeCompletionStale
	case revision > s.RuntimeRevision:
		return RuntimeCompletionFuture
	case !s.IsActive:
		return RuntimeCompletionAlreadyInactive
	default:
		s.IsActive = false
		return RuntimeCompletionCompleted
	}
}

func (s *Subscription) BeginReport(
	reportID string,
	revision, sequence int64,
) (delivered, inFlight, stale bool) {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	if !s.IsActive || revision != s.RuntimeRevision || sequence <= 0 {
		return false, false, true
	}
	if s.deliveredReportIDs == nil {
		s.deliveredReportIDs = make(map[string]struct{})
		s.inFlightReportIDs = make(map[string]struct{})
	}
	if _, ok := s.deliveredReportIDs[reportID]; ok {
		return true, false, false
	}
	if _, ok := s.inFlightReportIDs[reportID]; ok {
		return false, true, false
	}
	if sequence <= s.lastReportSequence {
		return false, false, true
	}
	s.inFlightReportIDs[reportID] = struct{}{}
	return false, false, false
}

func (s *Subscription) CompleteReport(reportID string, sequence int64, delivered bool) {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	delete(s.inFlightReportIDs, reportID)
	if !delivered {
		return
	}
	s.deliveredReportIDs[reportID] = struct{}{}
	if sequence > s.lastReportSequence {
		s.lastReportSequence = sequence
	}
	const completedReportWindow = 256
	if len(s.deliveredReportIDs) > completedReportWindow {
		// IDs are deterministic and old retries are bounded by PyAnLF, so clearing
		// the bounded process-local cache is preferable to unbounded growth.
		s.deliveredReportIDs = map[string]struct{}{reportID: {}}
	}
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

// NewCorrelationId returns a sequential, predictable correlation ID (corr-1, corr-2, ...)
func (c *NWDAFContext) NewCorrelationId() string {
	n := c.correlationIdCounter.Add(1)
	return fmt.Sprintf("corr-%d", n)
}
