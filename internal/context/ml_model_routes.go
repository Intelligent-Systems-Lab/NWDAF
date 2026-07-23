package context

import "encoding/json"

type MLModelRouteParty string

const (
	MLModelRoutePartyAnLFBackend MLModelRouteParty = "ANLF_BACKEND"
	MLModelRoutePartyMTLFBackend MLModelRouteParty = "MTLF_BACKEND"
	MLModelRoutePartyExternal    MLModelRouteParty = "EXTERNAL"
)

type MLModelProvisionSubscriptionRoute struct {
	SubscriptionID             string
	AcceptedRepresentation     json.RawMessage
	BackendRepresentation      json.RawMessage
	Initiator                  MLModelRouteParty
	Destination                MLModelRouteParty
	DestinationNotificationURI string
	NotificationCorrelationID  string
}

type MLModelMonitorRegistrationRoute struct {
	RegistrationID         string
	AcceptedRepresentation json.RawMessage
	BackendRepresentation  json.RawMessage
	Initiator              MLModelRouteParty
}

type MLModelMonitorSubscriptionRoute struct {
	SubscriptionID             string
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
	route.AcceptedRepresentation = append(json.RawMessage(nil), route.AcceptedRepresentation...)
	route.BackendRepresentation = append(json.RawMessage(nil), route.BackendRepresentation...)
	return route
}

func cloneRegistrationRoute(route MLModelMonitorRegistrationRoute) MLModelMonitorRegistrationRoute {
	route.AcceptedRepresentation = append(json.RawMessage(nil), route.AcceptedRepresentation...)
	route.BackendRepresentation = append(json.RawMessage(nil), route.BackendRepresentation...)
	return route
}

func cloneMonitorSubscriptionRoute(route MLModelMonitorSubscriptionRoute) MLModelMonitorSubscriptionRoute {
	route.AcceptedRepresentation = append(json.RawMessage(nil), route.AcceptedRepresentation...)
	route.BackendRepresentation = append(json.RawMessage(nil), route.BackendRepresentation...)
	return route
}
