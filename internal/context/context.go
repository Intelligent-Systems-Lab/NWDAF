package context

import (
	"encoding/json"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"

	"github.com/google/uuid"

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
)

func Init() {
	nwdafContext = &NWDAFContext{
		NfId:                              uuid.New().String(),
		nfServiceInstanceId:               uuid.New().String(),
		mlModelProvisionServiceInstanceID: uuid.New().String(),
		mlModelMonitorServiceInstanceID:   uuid.New().String(),
		analyticsRoutes:                   make(map[string]AnalyticsSubscriptionRoute),
		smfPeerRoutes:                     make(map[string]SmfPeerResourceRoute),
		mlModelProvisionRoutes:            make(map[string]MLModelProvisionSubscriptionRoute),
		mlModelRegistrationRoutes:         make(map[string]MLModelMonitorRegistrationRoute),
		mlModelMonitorRoutes:              make(map[string]MLModelMonitorSubscriptionRoute),
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
	nrfUri                            string
	nrfCertPem                        string
	nfProfile                         models.NrfNfManagementNfProfile
	registered                        bool
	registrationUri                   string
	oauth2Required                    bool
	heartBeatTimer                    int32

	mu                        sync.RWMutex
	analyticsRoutes           map[string]AnalyticsSubscriptionRoute
	smfPeerMu                 sync.RWMutex
	smfPeerRoutes             map[string]SmfPeerResourceRoute
	mlModelRouteMu            sync.RWMutex
	mlModelProvisionRoutes    map[string]MLModelProvisionSubscriptionRoute
	mlModelRegistrationRoutes map[string]MLModelMonitorRegistrationRoute
	mlModelMonitorRoutes      map[string]MLModelMonitorSubscriptionRoute
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

type SmfPeerResourceAssociation struct {
	TargetAPIBaseURI     string
	PeerSubscriptionID   string
	NwdafSubscriptionIDs []string
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

func (c *NWDAFContext) ReplaceSmfPeerResourceAssociations(
	associations []SmfPeerResourceAssociation,
) (bool, bool) {
	if c == nil {
		return false, false
	}
	c.smfPeerMu.Lock()
	defer c.smfPeerMu.Unlock()

	updates := make(map[string][]string, len(associations))
	for _, association := range associations {
		key := smfPeerResourceRouteKey(
			association.TargetAPIBaseURI,
			association.PeerSubscriptionID,
		)
		if key == "" {
			return false, false
		}
		if _, duplicate := updates[key]; duplicate {
			return false, false
		}
		if _, exists := c.smfPeerRoutes[key]; !exists {
			return false, false
		}
		updates[key] = append([]string(nil), association.NwdafSubscriptionIDs...)
	}

	changed := false
	for key := range c.smfPeerRoutes {
		route := c.smfPeerRoutes[key]
		nextIDs := updates[key]
		nextPendingCleanup := len(nextIDs) == 0
		if !slices.Equal(route.NwdafSubscriptionIDs, nextIDs) ||
			route.PendingCleanup != nextPendingCleanup {
			changed = true
		}
		route.NwdafSubscriptionIDs = append([]string(nil), nextIDs...)
		route.PendingCleanup = nextPendingCleanup
		c.smfPeerRoutes[key] = route
	}
	return true, changed
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

func (c *NWDAFContext) ConfigureNFManagement(
	nrfUri string,
	nrfCertPem string,
	nwdafName string,
	sbiUri string,
	sbiScheme string,
	registerIPv4 string,
	sbiPort int,
	advertiseEventsSubscription bool,
	advertiseMLModelProvision bool,
	advertiseMLModelMonitor bool,
) error {
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
	}
	if advertiseEventsSubscription || advertiseMLModelProvision {
		profile.NwdafInfo = &models.NwdafInfo{}
		if advertiseEventsSubscription {
			profile.NwdafInfo.NwdafEvents = []models.NwdafEvent{
				models.NwdafEvent_UE_COMMUNICATION,
			}
		}
		if advertiseMLModelProvision {
			profile.NwdafInfo.MlAnalyticsList = []models.MlAnalyticsInfo{{
				MlAnalyticsIds: []models.NwdafEvent{
					models.NwdafEvent_UE_COMMUNICATION,
				},
			}}
		}
	}
	if advertiseEventsSubscription {
		profile.NfServices = append(profile.NfServices, buildNwdafService(
			c.nfServiceInstanceId,
			models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION,
			nwdafEventsSubscriptionAPIVersion,
			nwdafEventsSubscriptionAPIFullVersion,
			scheme,
			registerIPv4,
			sbiPort,
			sbiUri,
		))
	}
	if advertiseMLModelProvision {
		profile.NfServices = append(profile.NfServices, buildNwdafService(
			c.mlModelProvisionServiceInstanceID,
			models.ServiceName_NNWDAF_MLMODELPROVISION,
			nwdafMLModelProvisionAPIVersion,
			nwdafMLModelProvisionAPIFullVersion,
			scheme,
			registerIPv4,
			sbiPort,
			sbiUri,
		))
	}
	if advertiseMLModelMonitor {
		profile.NfServices = append(profile.NfServices, buildNwdafService(
			c.mlModelMonitorServiceInstanceID,
			nwdafMLModelMonitorServiceName,
			nwdafMLModelMonitorAPIVersion,
			nwdafMLModelMonitorAPIFullVersion,
			scheme,
			registerIPv4,
			sbiPort,
			sbiUri,
		))
	}

	c.nfManagementMu.Lock()
	defer c.nfManagementMu.Unlock()
	c.NwdafName = nwdafName
	c.nrfUri = nrfUri
	c.nrfCertPem = strings.TrimSpace(nrfCertPem)
	c.nfProfile = profile
	c.registered = false
	c.registrationUri = ""
	c.oauth2Required = false
	c.heartBeatTimer = 0
	return nil
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
