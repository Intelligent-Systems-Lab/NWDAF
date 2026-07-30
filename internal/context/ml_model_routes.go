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

type MLModelPeerRoute struct {
	Direction         MLModelRouteDirection
	SelectedTarget    *backend.SelectedTarget
	PeerLocation      string
	BackendLocation   string
	BackendResourceID string
	LifecycleState    MLModelRouteLifecycle
	ProcessGeneration string
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

// ReconcileMTLFMLModelRoutes records that the current MTLF process accepted
// the Go-owned snapshot. Locally owned MTLF resources are recreated under the
// Go route ID; outbound peer mappings keep their peer Location unchanged.
func (c *NWDAFContext) ReconcileMTLFMLModelRoutes(processGeneration string) {
	if c == nil || processGeneration == "" {
		return
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	for routeID, route := range c.mlModelProvisionRoutes {
		if route.PeerRoute.LifecycleState != MLModelRouteActive ||
			route.PeerRoute.ProcessGeneration == processGeneration {
			continue
		}
		if route.PeerRoute.SelectedTarget == nil {
			route.PeerRoute.ProcessGeneration = processGeneration
			route.PeerRoute.BackendResourceID = route.SubscriptionID
			c.mlModelProvisionRoutes[routeID] = route
		}
	}
	for routeID, route := range c.mlModelRegistrationRoutes {
		if route.PeerRoute.LifecycleState != MLModelRouteActive ||
			route.PeerRoute.ProcessGeneration == processGeneration {
			continue
		}
		if route.PeerRoute.SelectedTarget == nil {
			route.PeerRoute.ProcessGeneration = processGeneration
			route.PeerRoute.BackendResourceID = route.RegistrationID
			c.mlModelRegistrationRoutes[routeID] = route
		}
	}
	for routeID, route := range c.mlModelMonitorRoutes {
		if route.PeerRoute.LifecycleState != MLModelRouteActive ||
			route.PeerRoute.ProcessGeneration == processGeneration {
			continue
		}
		if route.PeerRoute.SelectedTarget != nil {
			route.PeerRoute.ProcessGeneration = processGeneration
			c.mlModelMonitorRoutes[routeID] = route
		}
	}
	for routeID, route := range c.mlModelTrainingRoutes {
		if route.PeerRoute.LifecycleState != MLModelRouteActive ||
			route.PeerRoute.ProcessGeneration == processGeneration {
			continue
		}
		if route.PeerRoute.SelectedTarget == nil {
			route.PeerRoute.ProcessGeneration = processGeneration
			route.PeerRoute.BackendResourceID = route.SubscriptionID
		} else {
			route.PeerRoute.ProcessGeneration = processGeneration
		}
		c.mlModelTrainingRoutes[routeID] = route
	}
}

// ReconcileAnLFMLModelRoutes records that the current AnLF process accepted
// the Go-owned snapshot. Locally owned monitor resources are recreated under
// the Go route ID; outbound peer mappings retain their peer Location.
func (c *NWDAFContext) ReconcileAnLFMLModelRoutes(processGeneration string) {
	if c == nil || processGeneration == "" {
		return
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	for routeID, route := range c.mlModelMonitorRoutes {
		if route.PeerRoute.LifecycleState != MLModelRouteActive ||
			route.PeerRoute.ProcessGeneration == processGeneration {
			continue
		}
		if route.PeerRoute.SelectedTarget == nil {
			route.PeerRoute.ProcessGeneration = processGeneration
			route.PeerRoute.BackendResourceID = route.SubscriptionID
			c.mlModelMonitorRoutes[routeID] = route
		}
	}
	for routeID, route := range c.mlModelProvisionRoutes {
		if route.PeerRoute.LifecycleState != MLModelRouteActive ||
			route.PeerRoute.ProcessGeneration == processGeneration {
			continue
		}
		if route.PeerRoute.SelectedTarget != nil {
			route.PeerRoute.ProcessGeneration = processGeneration
			c.mlModelProvisionRoutes[routeID] = route
		}
	}
	for routeID, route := range c.mlModelRegistrationRoutes {
		if route.PeerRoute.LifecycleState != MLModelRouteActive ||
			route.PeerRoute.ProcessGeneration == processGeneration {
			continue
		}
		if route.PeerRoute.SelectedTarget != nil {
			route.PeerRoute.ProcessGeneration = processGeneration
			c.mlModelRegistrationRoutes[routeID] = route
		}
	}
}
