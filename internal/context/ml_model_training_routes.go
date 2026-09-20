package context

import "encoding/json"

type MLModelTrainingResourceKey struct {
	Direction         MLModelRouteDirection
	OwnerNFInstanceID string
	SubscriptionID    string
}

type MLModelTrainingSubscriptionRoute struct {
	SubscriptionID                  string
	OwnerNFInstanceID               string
	CallbackRouteID                 string
	PeerRoute                       MLModelPeerRoute
	AcceptedRepresentation          json.RawMessage
	BackendRepresentation           json.RawMessage
	Initiator                       MLModelRouteParty
	Destination                     MLModelRouteParty
	DestinationNotificationURI      string
	NotificationCorrelationID       string
	MLCorrelationID                 string
	ExpectedRoundIndicator          *int64
	OfferedSupportedFeatures        string
	NegotiatedSupportedFeatures     string
	HierarchicalFLFeatureNegotiated bool
	BoundParticipantNFInstanceID    string
}

func (r MLModelTrainingSubscriptionRoute) ResourceKey() MLModelTrainingResourceKey {
	return MLModelTrainingResourceKey{
		Direction: r.PeerRoute.Direction, OwnerNFInstanceID: r.OwnerNFInstanceID,
		SubscriptionID: r.SubscriptionID,
	}
}

func (c *NWDAFContext) AddMLModelTrainingSubscriptionRoute(route MLModelTrainingSubscriptionRoute) bool {
	if c == nil || route.NotificationCorrelationID == "" || route.OwnerNFInstanceID == "" {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	if c.mlModelTrainingRoutes == nil {
		c.mlModelTrainingRoutes = make(map[MLModelTrainingResourceKey]MLModelTrainingSubscriptionRoute)
	}
	if c.mlModelTrainingPendingRoutes == nil {
		c.mlModelTrainingPendingRoutes = make(map[string]MLModelTrainingSubscriptionRoute)
	}
	if c.trainingCorrelationExistsLocked(route.NotificationCorrelationID, "", MLModelTrainingResourceKey{}) {
		return false
	}
	if route.SubscriptionID == "" {
		if route.PeerRoute.Direction != MLModelRouteDirectionOutbound || route.CallbackRouteID == "" {
			return false
		}
		if _, exists := c.mlModelTrainingPendingRoutes[route.CallbackRouteID]; exists {
			return false
		}
		c.mlModelTrainingPendingRoutes[route.CallbackRouteID] = cloneTrainingRoute(route)
		return true
	}
	key := route.ResourceKey()
	if _, exists := c.mlModelTrainingRoutes[key]; exists {
		return false
	}
	c.mlModelTrainingRoutes[key] = cloneTrainingRoute(route)
	return true
}

func (c *NWDAFContext) UpdateMLModelTrainingSubscriptionRoute(route MLModelTrainingSubscriptionRoute) bool {
	if c == nil || route.OwnerNFInstanceID == "" {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	if route.SubscriptionID == "" {
		if route.CallbackRouteID == "" {
			return false
		}
		if _, exists := c.mlModelTrainingPendingRoutes[route.CallbackRouteID]; !exists {
			return false
		}
		if c.trainingCorrelationExistsLocked(
			route.NotificationCorrelationID, route.CallbackRouteID, MLModelTrainingResourceKey{},
		) {
			return false
		}
		c.mlModelTrainingPendingRoutes[route.CallbackRouteID] = cloneTrainingRoute(route)
		return true
	}
	key := route.ResourceKey()
	if _, exists := c.mlModelTrainingRoutes[key]; !exists {
		return false
	}
	if c.trainingCorrelationExistsLocked(route.NotificationCorrelationID, "", key) {
		return false
	}
	c.mlModelTrainingRoutes[key] = cloneTrainingRoute(route)
	return true
}

func (c *NWDAFContext) ActivatePendingMLModelTrainingRoute(
	callbackRouteID string, route MLModelTrainingSubscriptionRoute,
) bool {
	if c == nil || callbackRouteID == "" || route.SubscriptionID == "" ||
		route.CallbackRouteID != callbackRouteID {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	previous, found := c.mlModelTrainingPendingRoutes[callbackRouteID]
	key := route.ResourceKey()
	if !found || previous.NotificationCorrelationID != route.NotificationCorrelationID ||
		previous.OwnerNFInstanceID != route.OwnerNFInstanceID {
		return false
	}
	if _, exists := c.mlModelTrainingRoutes[key]; exists {
		return false
	}
	delete(c.mlModelTrainingPendingRoutes, callbackRouteID)
	c.mlModelTrainingRoutes[key] = cloneTrainingRoute(route)
	return true
}

func (c *NWDAFContext) GetMLModelTrainingSubscriptionRoute(
	key MLModelTrainingResourceKey,
) (MLModelTrainingSubscriptionRoute, bool) {
	if c == nil {
		return MLModelTrainingSubscriptionRoute{}, false
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	route, found := c.mlModelTrainingRoutes[key]
	return cloneTrainingRoute(route), found
}

func (c *NWDAFContext) GetPendingMLModelTrainingRoute(
	callbackRouteID string,
) (MLModelTrainingSubscriptionRoute, bool) {
	if c == nil {
		return MLModelTrainingSubscriptionRoute{}, false
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	route, found := c.mlModelTrainingPendingRoutes[callbackRouteID]
	return cloneTrainingRoute(route), found
}

func (c *NWDAFContext) GetAllMLModelTrainingSubscriptionRoutes() []MLModelTrainingSubscriptionRoute {
	if c == nil {
		return nil
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	routes := make([]MLModelTrainingSubscriptionRoute, 0,
		len(c.mlModelTrainingRoutes)+len(c.mlModelTrainingPendingRoutes))
	for _, route := range c.mlModelTrainingRoutes {
		routes = append(routes, cloneTrainingRoute(route))
	}
	for _, route := range c.mlModelTrainingPendingRoutes {
		routes = append(routes, cloneTrainingRoute(route))
	}
	return routes
}

func (c *NWDAFContext) FindMLModelTrainingSubscriptionRouteByCallback(
	callbackRouteID string,
) (MLModelTrainingSubscriptionRoute, bool) {
	if c == nil || callbackRouteID == "" {
		return MLModelTrainingSubscriptionRoute{}, false
	}
	c.mlModelRouteMu.RLock()
	defer c.mlModelRouteMu.RUnlock()
	if route, found := c.mlModelTrainingPendingRoutes[callbackRouteID]; found {
		return cloneTrainingRoute(route), true
	}
	for _, route := range c.mlModelTrainingRoutes {
		if route.CallbackRouteID == callbackRouteID {
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
	for _, route := range c.mlModelTrainingPendingRoutes {
		if route.NotificationCorrelationID == notificationCorrelationID {
			return cloneTrainingRoute(route), true
		}
	}
	return MLModelTrainingSubscriptionRoute{}, false
}

func (c *NWDAFContext) DeleteMLModelTrainingSubscriptionRoute(key MLModelTrainingResourceKey) bool {
	if c == nil {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	if _, found := c.mlModelTrainingRoutes[key]; !found {
		return false
	}
	delete(c.mlModelTrainingRoutes, key)
	return true
}

func (c *NWDAFContext) DeletePendingMLModelTrainingRoute(callbackRouteID string) bool {
	if c == nil {
		return false
	}
	c.mlModelRouteMu.Lock()
	defer c.mlModelRouteMu.Unlock()
	if _, found := c.mlModelTrainingPendingRoutes[callbackRouteID]; !found {
		return false
	}
	delete(c.mlModelTrainingPendingRoutes, callbackRouteID)
	return true
}

func (c *NWDAFContext) trainingCorrelationExistsLocked(
	correlationID, exceptPending string, exceptKey MLModelTrainingResourceKey,
) bool {
	for key, route := range c.mlModelTrainingRoutes {
		if key != exceptKey && route.NotificationCorrelationID == correlationID {
			return true
		}
	}
	for callbackID, route := range c.mlModelTrainingPendingRoutes {
		if callbackID != exceptPending && route.NotificationCorrelationID == correlationID {
			return true
		}
	}
	return false
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
