package context

import (
	"encoding/json"
	"time"

	"github.com/free5gc/nwdaf/internal/backend"
)

type MLModelRouteParty string

const (
	MLModelRoutePartyAnLFBackend MLModelRouteParty = "ANLF_BACKEND"
	MLModelRoutePartyMTLFBackend MLModelRouteParty = "MTLF_BACKEND"
	MLModelRoutePartyExternal    MLModelRouteParty = "EXTERNAL"
)

type MLModelRouteDirection string

const (
	MLModelRouteDirectionInbound  MLModelRouteDirection = "INBOUND"
	MLModelRouteDirectionOutbound MLModelRouteDirection = "OUTBOUND"
)

type MLModelRouteLifecycle string

const (
	MLModelRouteCreating       MLModelRouteLifecycle = "CREATING"
	MLModelRouteActive         MLModelRouteLifecycle = "ACTIVE"
	MLModelRouteReplacing      MLModelRouteLifecycle = "REPLACING"
	MLModelRouteDeleting       MLModelRouteLifecycle = "DELETING"
	MLModelRoutePendingCleanup MLModelRouteLifecycle = "PENDING_CLEANUP"
)

type MLModelResourceKind string

const (
	MLModelResourceProvisionSubscription MLModelResourceKind = "ML_MODEL_PROVISION_SUBSCRIPTION"
	MLModelResourceMonitorRegistration   MLModelResourceKind = "ML_MODEL_MONITOR_REGISTRATION"
	MLModelResourceMonitorSubscription   MLModelResourceKind = "ML_MODEL_MONITOR_SUBSCRIPTION"
	MLModelResourceTrainingSubscription  MLModelResourceKind = "ML_MODEL_TRAINING_SUBSCRIPTION"
)

// MLModelDeletionRecord is the process-local acknowledgement ledger retained
// after a backend generation is discarded. It intentionally carries no
// subscription representation or backend runtime state.
type MLModelDeletionRecord struct {
	ResourceID        string
	ProcessGeneration string
	CleanupAttempted  bool
}

func (c *NWDAFContext) TombstoneMLModelResource(record MLModelDeletionRecord, kind MLModelResourceKind) {
	if c == nil || kind == "" || record.ResourceID == "" {
		return
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	if c.mlModelDeletionRecords == nil {
		c.mlModelDeletionRecords = make(map[MLModelResourceKind]map[string]MLModelDeletionRecord)
	}
	if c.mlModelDeletionRecords[kind] == nil {
		c.mlModelDeletionRecords[kind] = make(map[string]MLModelDeletionRecord)
	}
	c.mlModelDeletionRecords[kind][record.ResourceID] = record
}

// ConsumeMLModelDeletionRecord removes one late-delete acknowledgement. A
// second DELETE therefore returns the normal not-found response.
func (c *NWDAFContext) ConsumeMLModelDeletionRecord(kind MLModelResourceKind, resourceID string) bool {
	if c == nil || kind == "" || resourceID == "" {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	records := c.mlModelDeletionRecords[kind]
	if _, found := records[resourceID]; !found {
		return false
	}
	delete(records, resourceID)
	return true
}

func (c *NWDAFContext) GetMLModelDeletionRecord(
	kind MLModelResourceKind,
	resourceID string,
) (MLModelDeletionRecord, bool) {
	if c == nil || kind == "" || resourceID == "" {
		return MLModelDeletionRecord{}, false
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	record, found := c.mlModelDeletionRecords[kind][resourceID]
	return record, found
}

type MLModelPeerRoute struct {
	Direction         MLModelRouteDirection
	SelectedTarget    *backend.SelectedTarget
	PeerLocation      string
	BackendLocation   string
	BackendResourceID string
	LifecycleState    MLModelRouteLifecycle
	ProcessGeneration string
	RelatedBackend    backend.Kind
	RelatedGeneration string
	CleanupAttempts   int
	NextCleanupAt     time.Time
}

type MLModelProvisionSubscriptionRoute struct {
	SubscriptionID             string
	PeerRoute                  MLModelPeerRoute
	AcceptedRepresentation     json.RawMessage
	BackendRepresentation      json.RawMessage
	Initiator                  MLModelRouteParty
	Destination                MLModelRouteParty
	DestinationNotificationURI string
	NotificationCorrelationID  string
}

type MLModelMonitorRegistrationRoute struct {
	RegistrationID         string
	PeerRoute              MLModelPeerRoute
	AcceptedRepresentation json.RawMessage
	BackendRepresentation  json.RawMessage
	Initiator              MLModelRouteParty
}

type MLModelMonitorSubscriptionRoute struct {
	SubscriptionID             string
	PeerRoute                  MLModelPeerRoute
	OwnerRegistrationID        string
	AcceptedRepresentation     json.RawMessage
	BackendRepresentation      json.RawMessage
	Destination                MLModelRouteParty
	DestinationNotificationURI string
	NotificationCorrelationID  string
}

func (c *NWDAFContext) AddMLModelProvisionSubscriptionRoute(
	route MLModelProvisionSubscriptionRoute,
) bool {
	if c == nil || route.SubscriptionID == "" {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	if c.mlModelProvisionRoutes == nil {
		c.mlModelProvisionRoutes = make(map[string]MLModelProvisionSubscriptionRoute)
	}
	if _, exists := c.mlModelProvisionRoutes[route.SubscriptionID]; exists {
		return false
	}
	c.mlModelProvisionRoutes[route.SubscriptionID] = cloneProvisionRoute(route)
	return true
}

func (c *NWDAFContext) UpdateMLModelProvisionSubscriptionRoute(
	route MLModelProvisionSubscriptionRoute,
) bool {
	if c == nil || route.SubscriptionID == "" {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	if _, exists := c.mlModelProvisionRoutes[route.SubscriptionID]; !exists {
		return false
	}
	c.mlModelProvisionRoutes[route.SubscriptionID] = cloneProvisionRoute(route)
	return true
}

func (c *NWDAFContext) GetMLModelProvisionSubscriptionRoute(
	subscriptionID string,
) (MLModelProvisionSubscriptionRoute, bool) {
	if c == nil {
		return MLModelProvisionSubscriptionRoute{}, false
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	route, exists := c.mlModelProvisionRoutes[subscriptionID]
	return cloneProvisionRoute(route), exists
}

func (c *NWDAFContext) DeleteMLModelProvisionSubscriptionRoute(subscriptionID string) bool {
	if c == nil {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	if _, exists := c.mlModelProvisionRoutes[subscriptionID]; !exists {
		return false
	}
	delete(c.mlModelProvisionRoutes, subscriptionID)
	return true
}

func (c *NWDAFContext) GetAllMLModelProvisionSubscriptionRoutes() []MLModelProvisionSubscriptionRoute {
	if c == nil {
		return nil
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	routes := make([]MLModelProvisionSubscriptionRoute, 0, len(c.mlModelProvisionRoutes))
	for _, route := range c.mlModelProvisionRoutes {
		routes = append(routes, cloneProvisionRoute(route))
	}
	return routes
}

func (c *NWDAFContext) FindMLModelProvisionSubscriptionRouteByBackendResourceID(
	backendResourceID string,
) (MLModelProvisionSubscriptionRoute, bool) {
	if c == nil || backendResourceID == "" {
		return MLModelProvisionSubscriptionRoute{}, false
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	for _, route := range c.mlModelProvisionRoutes {
		if route.PeerRoute.BackendResourceID == backendResourceID {
			return cloneProvisionRoute(route), true
		}
	}
	return MLModelProvisionSubscriptionRoute{}, false
}

func (c *NWDAFContext) AddMLModelMonitorRegistrationRoute(route MLModelMonitorRegistrationRoute) bool {
	if c == nil || route.RegistrationID == "" {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	if c.mlModelRegistrationRoutes == nil {
		c.mlModelRegistrationRoutes = make(map[string]MLModelMonitorRegistrationRoute)
	}
	if _, exists := c.mlModelRegistrationRoutes[route.RegistrationID]; exists {
		return false
	}
	c.mlModelRegistrationRoutes[route.RegistrationID] = cloneRegistrationRoute(route)
	return true
}

func (c *NWDAFContext) GetMLModelMonitorRegistrationRoute(
	registrationID string,
) (MLModelMonitorRegistrationRoute, bool) {
	if c == nil {
		return MLModelMonitorRegistrationRoute{}, false
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	route, exists := c.mlModelRegistrationRoutes[registrationID]
	return cloneRegistrationRoute(route), exists
}

func (c *NWDAFContext) UpdateMLModelMonitorRegistrationRoute(
	route MLModelMonitorRegistrationRoute,
) bool {
	if c == nil || route.RegistrationID == "" {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	if _, exists := c.mlModelRegistrationRoutes[route.RegistrationID]; !exists {
		return false
	}
	c.mlModelRegistrationRoutes[route.RegistrationID] = cloneRegistrationRoute(route)
	return true
}

func (c *NWDAFContext) DeleteMLModelMonitorRegistrationRoute(registrationID string) bool {
	if c == nil {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	if _, exists := c.mlModelRegistrationRoutes[registrationID]; !exists {
		return false
	}
	delete(c.mlModelRegistrationRoutes, registrationID)
	return true
}

func (c *NWDAFContext) GetAllMLModelMonitorRegistrationRoutes() []MLModelMonitorRegistrationRoute {
	if c == nil {
		return nil
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	routes := make([]MLModelMonitorRegistrationRoute, 0, len(c.mlModelRegistrationRoutes))
	for _, route := range c.mlModelRegistrationRoutes {
		routes = append(routes, cloneRegistrationRoute(route))
	}
	return routes
}

func (c *NWDAFContext) FindMLModelMonitorRegistrationRouteByBackendResourceID(
	backendResourceID string,
) (MLModelMonitorRegistrationRoute, bool) {
	if c == nil || backendResourceID == "" {
		return MLModelMonitorRegistrationRoute{}, false
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	for _, route := range c.mlModelRegistrationRoutes {
		if route.PeerRoute.BackendResourceID == backendResourceID {
			return cloneRegistrationRoute(route), true
		}
	}
	return MLModelMonitorRegistrationRoute{}, false
}

func (c *NWDAFContext) AddMLModelMonitorSubscriptionRoute(route MLModelMonitorSubscriptionRoute) bool {
	if c == nil || route.SubscriptionID == "" {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	if c.mlModelMonitorRoutes == nil {
		c.mlModelMonitorRoutes = make(map[string]MLModelMonitorSubscriptionRoute)
	}
	if _, exists := c.mlModelMonitorRoutes[route.SubscriptionID]; exists {
		return false
	}
	c.mlModelMonitorRoutes[route.SubscriptionID] = cloneMonitorSubscriptionRoute(route)
	return true
}

func (c *NWDAFContext) UpdateMLModelMonitorSubscriptionRoute(route MLModelMonitorSubscriptionRoute) bool {
	if c == nil || route.SubscriptionID == "" {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	if _, exists := c.mlModelMonitorRoutes[route.SubscriptionID]; !exists {
		return false
	}
	c.mlModelMonitorRoutes[route.SubscriptionID] = cloneMonitorSubscriptionRoute(route)
	return true
}

func (c *NWDAFContext) GetMLModelMonitorSubscriptionRoute(
	subscriptionID string,
) (MLModelMonitorSubscriptionRoute, bool) {
	if c == nil {
		return MLModelMonitorSubscriptionRoute{}, false
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	route, exists := c.mlModelMonitorRoutes[subscriptionID]
	return cloneMonitorSubscriptionRoute(route), exists
}

func (c *NWDAFContext) FindMLModelMonitorSubscriptionRouteByCorrelation(
	notificationCorrelationID string,
) (MLModelMonitorSubscriptionRoute, bool) {
	if c == nil || notificationCorrelationID == "" {
		return MLModelMonitorSubscriptionRoute{}, false
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	for _, route := range c.mlModelMonitorRoutes {
		if route.NotificationCorrelationID == notificationCorrelationID {
			return cloneMonitorSubscriptionRoute(route), true
		}
	}
	return MLModelMonitorSubscriptionRoute{}, false
}

func (c *NWDAFContext) FindMLModelMonitorSubscriptionRouteByBackendResourceID(
	backendResourceID string,
) (MLModelMonitorSubscriptionRoute, bool) {
	if c == nil || backendResourceID == "" {
		return MLModelMonitorSubscriptionRoute{}, false
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	for _, route := range c.mlModelMonitorRoutes {
		if route.PeerRoute.BackendResourceID == backendResourceID {
			return cloneMonitorSubscriptionRoute(route), true
		}
	}
	return MLModelMonitorSubscriptionRoute{}, false
}

func (c *NWDAFContext) DeleteMLModelMonitorSubscriptionRoute(subscriptionID string) bool {
	if c == nil {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	if _, exists := c.mlModelMonitorRoutes[subscriptionID]; !exists {
		return false
	}
	delete(c.mlModelMonitorRoutes, subscriptionID)
	return true
}

func (c *NWDAFContext) GetAllMLModelMonitorSubscriptionRoutes() []MLModelMonitorSubscriptionRoute {
	if c == nil {
		return nil
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	routes := make([]MLModelMonitorSubscriptionRoute, 0, len(c.mlModelMonitorRoutes))
	for _, route := range c.mlModelMonitorRoutes {
		routes = append(routes, cloneMonitorSubscriptionRoute(route))
	}
	return routes
}

func cloneProvisionRoute(route MLModelProvisionSubscriptionRoute) MLModelProvisionSubscriptionRoute {
	route.PeerRoute = clonePeerRoute(route.PeerRoute)
	route.AcceptedRepresentation = append(json.RawMessage(nil), route.AcceptedRepresentation...)
	route.BackendRepresentation = append(json.RawMessage(nil), route.BackendRepresentation...)
	return route
}

func cloneRegistrationRoute(route MLModelMonitorRegistrationRoute) MLModelMonitorRegistrationRoute {
	route.PeerRoute = clonePeerRoute(route.PeerRoute)
	route.AcceptedRepresentation = append(json.RawMessage(nil), route.AcceptedRepresentation...)
	route.BackendRepresentation = append(json.RawMessage(nil), route.BackendRepresentation...)
	return route
}

func cloneMonitorSubscriptionRoute(route MLModelMonitorSubscriptionRoute) MLModelMonitorSubscriptionRoute {
	route.PeerRoute = clonePeerRoute(route.PeerRoute)
	route.AcceptedRepresentation = append(json.RawMessage(nil), route.AcceptedRepresentation...)
	route.BackendRepresentation = append(json.RawMessage(nil), route.BackendRepresentation...)
	return route
}

func clonePeerRoute(route MLModelPeerRoute) MLModelPeerRoute {
	if route.SelectedTarget != nil {
		target := *route.SelectedTarget
		route.SelectedTarget = &target
	}
	return route
}
