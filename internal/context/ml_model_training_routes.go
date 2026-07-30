package context

import (
	"encoding/json"
)

type MLModelTrainingSubscriptionRoute struct {
	SubscriptionID             string
	PeerRoute                  MLModelPeerRoute
	AcceptedRepresentation     json.RawMessage
	BackendRepresentation      json.RawMessage
	Initiator                  MLModelRouteParty
	Destination                MLModelRouteParty
	DestinationNotificationURI string
	NotificationCorrelationID  string
	MLCorrelationID            string
	ExpectedRoundIndicator     *int64
}

func (c *NWDAFContext) AddMLModelTrainingSubscriptionRoute(
	route MLModelTrainingSubscriptionRoute,
) bool {
	if c == nil || route.SubscriptionID == "" || route.NotificationCorrelationID == "" {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	if c.mlModelTrainingRoutes == nil {
		c.mlModelTrainingRoutes = make(map[string]MLModelTrainingSubscriptionRoute)
	}
	if _, exists := c.mlModelTrainingRoutes[route.SubscriptionID]; exists {
		return false
	}
	for _, existing := range c.mlModelTrainingRoutes {
		if existing.NotificationCorrelationID == route.NotificationCorrelationID {
			return false
		}
	}
	c.mlModelTrainingRoutes[route.SubscriptionID] = cloneTrainingRoute(route)
	return true
}

func (c *NWDAFContext) UpdateMLModelTrainingSubscriptionRoute(
	route MLModelTrainingSubscriptionRoute,
) bool {
	if c == nil || route.SubscriptionID == "" {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	if _, exists := c.mlModelTrainingRoutes[route.SubscriptionID]; !exists {
		return false
	}
	for id, existing := range c.mlModelTrainingRoutes {
		if id != route.SubscriptionID &&
			existing.NotificationCorrelationID == route.NotificationCorrelationID {
			return false
		}
	}
	c.mlModelTrainingRoutes[route.SubscriptionID] = cloneTrainingRoute(route)
	return true
}

func (c *NWDAFContext) GetMLModelTrainingSubscriptionRoute(
	subscriptionID string,
) (MLModelTrainingSubscriptionRoute, bool) {
	if c == nil {
		return MLModelTrainingSubscriptionRoute{}, false
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	route, found := c.mlModelTrainingRoutes[subscriptionID]
	return cloneTrainingRoute(route), found
}

func (c *NWDAFContext) GetAllMLModelTrainingSubscriptionRoutes() []MLModelTrainingSubscriptionRoute {
	if c == nil {
		return nil
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	routes := make([]MLModelTrainingSubscriptionRoute, 0, len(c.mlModelTrainingRoutes))
	for _, route := range c.mlModelTrainingRoutes {
		routes = append(routes, cloneTrainingRoute(route))
	}
	return routes
}

func (c *NWDAFContext) FindMLModelTrainingSubscriptionRouteByBackendResourceID(
	backendResourceID string,
) (MLModelTrainingSubscriptionRoute, bool) {
	if c == nil || backendResourceID == "" {
		return MLModelTrainingSubscriptionRoute{}, false
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	for _, route := range c.mlModelTrainingRoutes {
		if route.PeerRoute.BackendResourceID == backendResourceID {
			return cloneTrainingRoute(route), true
		}
	}
	return MLModelTrainingSubscriptionRoute{}, false
}

func (c *NWDAFContext) FindMLModelTrainingSubscriptionRouteByCorrelation(
	notificationCorrelationID string,
) (MLModelTrainingSubscriptionRoute, bool) {
	if c == nil || notificationCorrelationID == "" {
		return MLModelTrainingSubscriptionRoute{}, false
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	for _, route := range c.mlModelTrainingRoutes {
		if route.NotificationCorrelationID == notificationCorrelationID {
			return cloneTrainingRoute(route), true
		}
	}
	return MLModelTrainingSubscriptionRoute{}, false
}

func (c *NWDAFContext) DeleteMLModelTrainingSubscriptionRoute(subscriptionID string) bool {
	if c == nil {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	if _, found := c.mlModelTrainingRoutes[subscriptionID]; !found {
		return false
	}
	delete(c.mlModelTrainingRoutes, subscriptionID)
	return true
}

func cloneTrainingRoute(route MLModelTrainingSubscriptionRoute) MLModelTrainingSubscriptionRoute {
	route.PeerRoute = clonePeerRoute(route.PeerRoute)
	route.AcceptedRepresentation = append(json.RawMessage(nil), route.AcceptedRepresentation...)
	route.BackendRepresentation = append(json.RawMessage(nil), route.BackendRepresentation...)
	if route.ExpectedRoundIndicator != nil {
		round := *route.ExpectedRoundIndicator
		route.ExpectedRoundIndicator = &round
	}
	return route
}
