package context

import (
	"encoding/json"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"

	"github.com/google/uuid"

	compatnrf "github.com/free5gc/nwdaf/internal/compat/nrf"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
	"github.com/free5gc/openapi/oauth"
)

var nwdafContext *NWDAFContext

const (
	nwdafEventsSubscriptionAPIVersion                        = "v1"
	nwdafEventsSubscriptionAPIFullVersion                    = "1.0.0"
	nwdafMLModelProvisionAPIVersion                          = "v1"
	nwdafMLModelProvisionAPIFullVersion                      = "1.1.4"
	nwdafMLModelMonitorAPIVersion                            = "v1"
	nwdafMLModelMonitorAPIFullVersion                        = "1.0.2"
	nwdafMLModelMonitorServiceName        models.ServiceName = "nnwdaf-mlmodelmonitor"
	nwdafMLModelTrainingAPIVersion                           = "v1"
	nwdafMLModelTrainingAPIFullVersion                       = "1.0.5"
	nwdafMLModelTrainingServiceName       models.ServiceName = "nnwdaf-mlmodeltraining"
)

func Init() {
	InitWithNFInstanceID("")
}

func InitWithNFInstanceID(nfInstanceID string) {
	nfInstanceID = strings.TrimSpace(nfInstanceID)
	if nfInstanceID == "" {
		nfInstanceID = uuid.New().String()
	}
	nwdafContext = &NWDAFContext{
		NfId:                              nfInstanceID,
		nfServiceInstanceId:               uuid.New().String(),
		mlModelProvisionServiceInstanceID: uuid.New().String(),
		mlModelMonitorServiceInstanceID:   uuid.New().String(),
		mlModelTrainingServiceInstanceID:  uuid.New().String(),
		analyticsRoutes:                   make(map[string]AnalyticsSubscriptionRoute),
		analyticsTombstones:               make(map[string]struct{}),
		smfPeerRoutes:                     make(map[string]SmfPeerResourceRoute),
		mlModelProvisionRoutes:            make(map[string]MLModelProvisionSubscriptionRoute),
		mlModelRegistrationRoutes:         make(map[string]MLModelMonitorRegistrationRoute),
		mlModelMonitorRoutes:              make(map[string]MLModelMonitorSubscriptionRoute),
		mlModelTrainingRoutes:             make(map[MLModelTrainingResourceKey]MLModelTrainingSubscriptionRoute),
		mlModelTrainingPendingRoutes:      make(map[string]MLModelTrainingSubscriptionRoute),
		mlModelDeletionRecords:            make(map[MLModelResourceKind]map[string]MLModelDeletionRecord),
	}
	logger.CtxLog.Infof("NWDAF Context initialized with NfId: %s", nwdafContext.NfId)
}

func GetSelf() *NWDAFContext {
	return nwdafContext
}

type NWDAFContext struct {
	NfId      string
	NwdafName string

	nfManagementMu                    sync.RWMutex
	nfServiceInstanceId               string
	mlModelProvisionServiceInstanceID string
	mlModelMonitorServiceInstanceID   string
	mlModelTrainingServiceInstanceID  string
	nrfUri                            string
	nrfCertPem                        string
	nfProfile                         compatnrf.NFProfile
	registered                        bool
	registrationUri                   string
	oauth2Required                    bool
	heartBeatTimer                    int32

	mu                           sync.RWMutex
	analyticsRoutes              map[string]AnalyticsSubscriptionRoute
	analyticsTombstones          map[string]struct{}
	smfPeerMu                    sync.RWMutex
	smfPeerRoutes                map[string]SmfPeerResourceRoute
	mlModelRouteMu               sync.RWMutex
	mlModelProvisionRoutes       map[string]MLModelProvisionSubscriptionRoute
	mlModelRegistrationRoutes    map[string]MLModelMonitorRegistrationRoute
	mlModelMonitorRoutes         map[string]MLModelMonitorSubscriptionRoute
	mlModelTrainingRoutes        map[MLModelTrainingResourceKey]MLModelTrainingSubscriptionRoute
	mlModelTrainingPendingRoutes map[string]MLModelTrainingSubscriptionRoute
	mlModelDeletionRecords       map[MLModelResourceKind]map[string]MLModelDeletionRecord
}

type SmfPeerResourceRoute struct {
	SubscriptionID           string
	ResourceLocation         string
	TargetAPIBaseURI         string
	CorrelationID            string
	AcceptedSubscription     models.NsmfEventExposure
	AcceptedSubscriptionJSON json.RawMessage
	PendingCleanup           bool
	NwdafSubscriptionIDs     []string
}

func (c *NWDAFContext) AddSmfPeerResourceRoute(route *SmfPeerResourceRoute) bool {
	if route == nil {
		return false
	}
	key := smfPeerResourceRouteKey(route.TargetAPIBaseURI, route.SubscriptionID)
	if c == nil || key == "" || route.ResourceLocation == "" {
		return false
	}
	c.smfPeerMu.Lock()
	defer c.smfPeerMu.Unlock()
	if c.smfPeerRoutes == nil {
		c.smfPeerRoutes = make(map[string]SmfPeerResourceRoute)
	}
	if _, exists := c.smfPeerRoutes[key]; exists {
		return false
	}
	stored := *route
	stored.NwdafSubscriptionIDs = append([]string(nil), route.NwdafSubscriptionIDs...)
	c.smfPeerRoutes[key] = stored
	return true
}

func (c *NWDAFContext) GetSmfPeerResourceRoute(
	targetAPIBaseURI string,
	id string,
) (SmfPeerResourceRoute, bool) {
	if c == nil {
		return SmfPeerResourceRoute{}, false
	}
	c.smfPeerMu.RLock()
	defer c.smfPeerMu.RUnlock()
	route, exists := c.smfPeerRoutes[smfPeerResourceRouteKey(targetAPIBaseURI, id)]
	route.NwdafSubscriptionIDs = append([]string(nil), route.NwdafSubscriptionIDs...)
	return route, exists
}

func (c *NWDAFContext) UpdateSmfPeerResourceRoute(route *SmfPeerResourceRoute) bool {
	if route == nil {
		return false
	}
	key := smfPeerResourceRouteKey(route.TargetAPIBaseURI, route.SubscriptionID)
	if c == nil || key == "" {
		return false
	}
	c.smfPeerMu.Lock()
	defer c.smfPeerMu.Unlock()
	if _, exists := c.smfPeerRoutes[key]; !exists {
		return false
	}
	stored := *route
	stored.NwdafSubscriptionIDs = append([]string(nil), route.NwdafSubscriptionIDs...)
	c.smfPeerRoutes[key] = stored
	return true
}

func (c *NWDAFContext) DeleteSmfPeerResourceRoute(targetAPIBaseURI string, id string) bool {
	if c == nil {
		return false
	}
	key := smfPeerResourceRouteKey(targetAPIBaseURI, id)
	c.smfPeerMu.Lock()
	defer c.smfPeerMu.Unlock()
	if _, exists := c.smfPeerRoutes[key]; !exists {
		return false
	}
	delete(c.smfPeerRoutes, key)
	return true
}

func smfPeerResourceRouteKey(targetAPIBaseURI string, id string) string {
	targetAPIBaseURI = strings.TrimRight(strings.TrimSpace(targetAPIBaseURI), "/")
	id = strings.TrimSpace(id)
	if targetAPIBaseURI == "" || id == "" {
		return ""
	}
	return targetAPIBaseURI + "\x00" + id
}

func (c *NWDAFContext) GetAllSmfPeerResourceRoutes() []*SmfPeerResourceRoute {
	if c == nil {
		return nil
	}
	c.smfPeerMu.RLock()
	defer c.smfPeerMu.RUnlock()
	routes := make([]*SmfPeerResourceRoute, 0, len(c.smfPeerRoutes))
	for key := range c.smfPeerRoutes {
		route := c.smfPeerRoutes[key]
		route.NwdafSubscriptionIDs = append([]string(nil), route.NwdafSubscriptionIDs...)
		routes = append(routes, &route)
	}
	return routes
}

type AnalyticsSubscriptionRoute struct {
	SubscriptionID          string
	ExternalNotificationURI string
	AcceptedSubscription    models.NnwdafEventsSubscription
	ProcessGeneration       string
}

func (c *NWDAFContext) AddAnalyticsSubscriptionRoute(route AnalyticsSubscriptionRoute) bool {
	if c == nil || route.SubscriptionID == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.analyticsRoutes[route.SubscriptionID]; exists {
		return false
	}
	c.analyticsRoutes[route.SubscriptionID] = route
	return true
}

func (c *NWDAFContext) UpdateAnalyticsSubscriptionRoute(route AnalyticsSubscriptionRoute) bool {
	if c == nil || route.SubscriptionID == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.analyticsRoutes[route.SubscriptionID]; !exists {
		return false
	}
	c.analyticsRoutes[route.SubscriptionID] = route
	return true
}

func (c *NWDAFContext) GetAnalyticsSubscriptionRoute(id string) (AnalyticsSubscriptionRoute, bool) {
	if c == nil {
		return AnalyticsSubscriptionRoute{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	route, exists := c.analyticsRoutes[id]
	return route, exists
}

func (c *NWDAFContext) DeleteAnalyticsSubscriptionRoute(id string) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.analyticsRoutes[id]; !exists {
		return false
	}
	delete(c.analyticsRoutes, id)
	return true
}

func (c *NWDAFContext) TombstoneAnalyticsSubscription(id string) {
	if c == nil || id == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.analyticsRoutes, id)
	if c.analyticsTombstones == nil {
		c.analyticsTombstones = make(map[string]struct{})
	}
	c.analyticsTombstones[id] = struct{}{}
}

func (c *NWDAFContext) IsAnalyticsSubscriptionTombstoned(id string) bool {
	if c == nil || id == "" {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, found := c.analyticsTombstones[id]
	return found
}

func (c *NWDAFContext) GetAllAnalyticsSubscriptionRoutes() []AnalyticsSubscriptionRoute {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	routes := make([]AnalyticsSubscriptionRoute, 0, len(c.analyticsRoutes))
	for _, route := range c.analyticsRoutes {
		routes = append(routes, route)
	}
	return routes
}

type NFRegistrationState struct {
	Registered     bool
	ResourceURI    string
	OAuth2Required bool
	HeartBeatTimer int32
}

type NFManagementConfig struct {
	NrfURI       string
	NrfCertPEM   string
	NwdafName    string
	SBIURI       string
	SBIScheme    string
	RegisterIPv4 string
	SBIPort      int
	ServiceNames []models.ServiceName
	NwdafInfo    *compatnrf.NwdafInfo
}

type FLCapabilityProjectionEntry struct {
	MLAnalyticsIDs   []models.NwdafEvent
	FLCapabilityType compatnrf.FLCapabilityType
}

func (c *NWDAFContext) ConfigureNFManagement(config NFManagementConfig) error {
	if c == nil {
		return fmt.Errorf("NWDAF context is nil")
	}
	if c.NfId == "" {
		return fmt.Errorf("NF instance ID is required")
	}
	if c.nfServiceInstanceId == "" {
		c.nfServiceInstanceId = uuid.New().String()
	}
	if c.mlModelProvisionServiceInstanceID == "" {
		c.mlModelProvisionServiceInstanceID = uuid.New().String()
	}
	if c.mlModelMonitorServiceInstanceID == "" {
		c.mlModelMonitorServiceInstanceID = uuid.New().String()
	}
	if c.mlModelTrainingServiceInstanceID == "" {
		c.mlModelTrainingServiceInstanceID = uuid.New().String()
	}
	registerIPv4 := strings.TrimSpace(config.RegisterIPv4)
	parsedRegisterIPv4 := net.ParseIP(registerIPv4)
	if strings.Contains(registerIPv4, ":") || parsedRegisterIPv4 == nil ||
		parsedRegisterIPv4.To4() == nil || parsedRegisterIPv4.IsUnspecified() {
		return fmt.Errorf("SBI registration address must be a valid non-wildcard IPv4 address")
	}
	if config.SBIPort <= 0 || config.SBIPort > 65535 {
		return fmt.Errorf("SBI port must be between 1 and 65535")
	}

	var scheme models.UriScheme
	switch config.SBIScheme {
	case string(models.UriScheme_HTTP):
		scheme = models.UriScheme_HTTP
	case string(models.UriScheme_HTTPS):
		scheme = models.UriScheme_HTTPS
	default:
		return fmt.Errorf("unsupported SBI scheme %q", config.SBIScheme)
	}

	profile := compatnrf.NFProfile{
		NrfNfManagementNfProfile: models.NrfNfManagementNfProfile{
			NfInstanceId:   c.NfId,
			NfInstanceName: config.NwdafName,
			NfType:         models.NrfNfManagementNfType_NWDAF,
			NfStatus:       models.NrfNfManagementNfStatus_REGISTERED,
			Ipv4Addresses:  []string{registerIPv4},
		},
		NwdafInfo: config.NwdafInfo,
	}
	for _, serviceName := range config.ServiceNames {
		serviceInstanceID, apiVersion, apiFullVersion, err := c.serviceMetadata(serviceName)
		if err != nil {
			return err
		}
		profile.NfServices = append(profile.NfServices, buildNwdafService(
			serviceInstanceID,
			serviceName,
			apiVersion,
			apiFullVersion,
			scheme,
			registerIPv4,
			config.SBIPort,
			config.SBIURI,
		))
	}

	c.nfManagementMu.Lock()
	defer c.nfManagementMu.Unlock()
	c.NwdafName = config.NwdafName
	c.nrfUri = config.NrfURI
	c.nrfCertPem = strings.TrimSpace(config.NrfCertPEM)
	c.nfProfile = profile
	c.registered = false
	c.registrationUri = ""
	c.oauth2Required = false
	c.heartBeatTimer = 0
	return nil
}

func (c *NWDAFContext) serviceMetadata(
	serviceName models.ServiceName,
) (string, string, string, error) {
	switch serviceName {
	case models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION:
		return c.nfServiceInstanceId,
			nwdafEventsSubscriptionAPIVersion,
			nwdafEventsSubscriptionAPIFullVersion,
			nil
	case models.ServiceName_NNWDAF_MLMODELPROVISION:
		return c.mlModelProvisionServiceInstanceID,
			nwdafMLModelProvisionAPIVersion,
			nwdafMLModelProvisionAPIFullVersion,
			nil
	case nwdafMLModelMonitorServiceName:
		return c.mlModelMonitorServiceInstanceID,
			nwdafMLModelMonitorAPIVersion,
			nwdafMLModelMonitorAPIFullVersion,
			nil
	case nwdafMLModelTrainingServiceName:
		return c.mlModelTrainingServiceInstanceID,
			nwdafMLModelTrainingAPIVersion,
			nwdafMLModelTrainingAPIFullVersion,
			nil
	default:
		return "", "", "", fmt.Errorf("unsupported NWDAF service %q", serviceName)
	}
}

func buildNwdafService(
	serviceInstanceID string,
	serviceName models.ServiceName,
	apiVersion string,
	apiFullVersion string,
	scheme models.UriScheme,
	registerIPv4 string,
	sbiPort int,
	sbiURI string,
) models.NrfNfManagementNfService {
	return models.NrfNfManagementNfService{
		ServiceInstanceId: serviceInstanceID,
		ServiceName:       serviceName,
		Versions: []models.NfServiceVersion{{
			ApiVersionInUri: apiVersion,
			ApiFullVersion:  apiFullVersion,
		}},
		Scheme:          scheme,
		NfServiceStatus: models.NfServiceStatus_REGISTERED,
		IpEndPoints: []models.IpEndPoint{{
			Ipv4Address: registerIPv4,
			Transport:   models.NrfNfManagementTransportProtocol_TCP,
			Port:        int32(sbiPort),
		}},
		ApiPrefix: sbiURI,
	}
}

func (c *NWDAFContext) NrfUri() string {
	if c == nil {
		return ""
	}
	c.nfManagementMu.RLock()
	defer c.nfManagementMu.RUnlock()
	return c.nrfUri
}

func (c *NWDAFContext) NrfCertPem() string {
	if c == nil {
		return ""
	}
	c.nfManagementMu.RLock()
	defer c.nfManagementMu.RUnlock()
	return c.nrfCertPem
}

func (c *NWDAFContext) NFProfile() models.NrfNfManagementNfProfile {
	if c == nil {
		return models.NrfNfManagementNfProfile{}
	}
	c.nfManagementMu.RLock()
	defer c.nfManagementMu.RUnlock()
	return c.nfProfile.NrfNfManagementNfProfile
}

func (c *NWDAFContext) NFProfileSnapshot() (compatnrf.NFProfile, error) {
	if c == nil {
		return compatnrf.NFProfile{}, fmt.Errorf("NWDAF context is nil")
	}
	c.nfManagementMu.RLock()
	defer c.nfManagementMu.RUnlock()
	encoded, err := json.Marshal(c.nfProfile)
	if err != nil {
		return compatnrf.NFProfile{}, fmt.Errorf("encode NF profile snapshot: %w", err)
	}
	var snapshot compatnrf.NFProfile
	if err = json.Unmarshal(encoded, &snapshot); err != nil {
		return compatnrf.NFProfile{}, fmt.Errorf("decode NF profile snapshot: %w", err)
	}
	return snapshot, nil
}

func (c *NWDAFContext) FLCapabilityProjection() ([]FLCapabilityProjectionEntry, error) {
	profile, err := c.NFProfileSnapshot()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(profile.NfInstanceId) == "" ||
		profile.NfType != models.NrfNfManagementNfType_NWDAF ||
		profile.NfStatus != models.NrfNfManagementNfStatus_REGISTERED {
		return nil, fmt.Errorf("NF profile snapshot is not initialized for NWDAF registration")
	}

	projection := make([]FLCapabilityProjectionEntry, 0)
	if profile.NwdafInfo == nil {
		return projection, nil
	}
	for index, entry := range profile.NwdafInfo.MLAnalyticsList {
		if entry.FLCapabilityType == "" {
			continue
		}
		if !compatnrf.IsKnownFLCapability(entry.FLCapabilityType) {
			return nil, fmt.Errorf(
				"NF profile mlAnalyticsList[%d] has unsupported flCapabilityType %q",
				index,
				entry.FLCapabilityType,
			)
		}
		if len(entry.MLAnalyticsIDs) == 0 {
			return nil, fmt.Errorf(
				"NF profile mlAnalyticsList[%d] requires mlAnalyticsIds for FL capability",
				index,
			)
		}

		analyticsIDs := append([]models.NwdafEvent(nil), entry.MLAnalyticsIDs...)
		for analyticsIndex, analyticsID := range analyticsIDs {
			if value := string(analyticsID); value == "" || strings.TrimSpace(value) != value {
				return nil, fmt.Errorf(
					"NF profile mlAnalyticsList[%d].mlAnalyticsIds[%d] is invalid",
					index,
					analyticsIndex,
				)
			}
		}
		slices.SortFunc(analyticsIDs, func(left, right models.NwdafEvent) int {
			return strings.Compare(string(left), string(right))
		})
		analyticsIDs = slices.Compact(analyticsIDs)
		projection = append(projection, FLCapabilityProjectionEntry{
			MLAnalyticsIDs:   analyticsIDs,
			FLCapabilityType: entry.FLCapabilityType,
		})
	}

	slices.SortFunc(projection, func(left, right FLCapabilityProjectionEntry) int {
		return strings.Compare(flCapabilityProjectionKey(left), flCapabilityProjectionKey(right))
	})
	projection = slices.CompactFunc(
		projection,
		func(left, right FLCapabilityProjectionEntry) bool {
			return flCapabilityProjectionKey(left) == flCapabilityProjectionKey(right)
		},
	)
	return projection, nil
}

func flCapabilityProjectionKey(entry FLCapabilityProjectionEntry) string {
	values := make([]string, 0, len(entry.MLAnalyticsIDs)+1)
	values = append(values, string(entry.FLCapabilityType))
	for _, analyticsID := range entry.MLAnalyticsIDs {
		values = append(values, string(analyticsID))
	}
	return strings.Join(values, "\x00")
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
	c.registered = true
	c.registrationUri = resourceURI
	c.oauth2Required = true
}

func (c *NWDAFContext) AuthorizationCheck(token string, serviceName models.ServiceName) error {
	if c == nil {
		return fmt.Errorf("NWDAF context is nil")
	}
	c.nfManagementMu.RLock()
	oauth2Required := c.oauth2Required
	nrfCertPem := c.nrfCertPem
	c.nfManagementMu.RUnlock()
	if !oauth2Required {
		return nil
	}
	return oauth.VerifyOAuth(token, string(serviceName), nrfCertPem)
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
