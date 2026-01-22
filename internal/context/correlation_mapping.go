package context

import (
	"time"

	"github.com/free5gc/nwdaf/internal/logger"
)

// NwdafSubResource tracks a single SMF resource used by an NWDAF subscription
// Used for proper cleanup when subscription is deleted
type NwdafSubResource struct {
	SmfEndpoint   string    // SMF endpoint URL
	Supi          string    // Target SUPI
	CorrelationId string    // CorrelationId for this SMF subscription
	CreatedAt     time.Time // When resource was added
}

// --- Notification Routing (correlationId → supi) ---

// StoreCorrelationToSupi stores correlationId -> SUPI mapping for UPF notification routing
func (c *NWDAFContext) StoreCorrelationToSupi(correlationId, supi string) {
	c.correlationToSupiMap.Store(correlationId, supi)
	logger.CtxLog.Debugf("Stored correlation mapping: %s -> %s", correlationId, supi)
}

// GetSupiByCorrelationId retrieves SUPI from correlationId
// Used by UPF notification handler to identify target UE
func (c *NWDAFContext) GetSupiByCorrelationId(correlationId string) (string, bool) {
	if val, ok := c.correlationToSupiMap.Load(correlationId); ok {
		return val.(string), true
	}
	return "", false
}

// DeleteCorrelationToSupi removes correlationId -> SUPI mapping
// Called when SMF subscription is fully released (refCount reaches 0)
func (c *NWDAFContext) DeleteCorrelationToSupi(correlationId string) {
	c.correlationToSupiMap.Delete(correlationId)
	logger.CtxLog.Debugf("Deleted correlation mapping: %s", correlationId)
}

// ClearCorrelationToSupiMap removes all mappings (for testing)
func (c *NWDAFContext) ClearCorrelationToSupiMap() {
	c.correlationToSupiMap.Range(func(key, value interface{}) bool {
		c.correlationToSupiMap.Delete(key)
		return true
	})
}

// --- Cleanup Tracking (nwdafSubId → resources) ---

// AddNwdafSubResource adds a resource tracking entry for an NWDAF subscription
// Each NWDAF subscription tracks its own resources for proper cleanup
func (c *NWDAFContext) AddNwdafSubResource(nwdafSubId string, resource NwdafSubResource) {
	var resources []NwdafSubResource
	if val, ok := c.nwdafSubResourcesMap.Load(nwdafSubId); ok {
		resources = val.([]NwdafSubResource)
	}
	resources = append(resources, resource)
	c.nwdafSubResourcesMap.Store(nwdafSubId, resources)
	logger.CtxLog.Debugf("Added resource for nwdafSubId=%s: endpoint=%s, supi=%s",
		nwdafSubId, resource.SmfEndpoint, resource.Supi)
}

// GetNwdafSubResources retrieves all resources for an NWDAF subscription
// Called during subscription cleanup to release SMF resources
func (c *NWDAFContext) GetNwdafSubResources(nwdafSubId string) []NwdafSubResource {
	if val, ok := c.nwdafSubResourcesMap.Load(nwdafSubId); ok {
		return val.([]NwdafSubResource)
	}
	return nil
}

// DeleteNwdafSubResources removes all resource tracking for an NWDAF subscription
// Called after cleanup is complete
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
