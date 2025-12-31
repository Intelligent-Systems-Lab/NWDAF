package context

import (
	"sync"
	"time"

	"github.com/free5gc/openapi/models"
	"github.com/google/uuid"

	"github.com/free5gc/nwdaf/internal/logger"
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

	// Subscriptions storage
	mu            sync.RWMutex
	subscriptions map[string]*Subscription
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
	
	logger.CtxLog.Infof("Added subscription: %s", sub.ID)
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
	
	logger.CtxLog.Infof("Updated subscription: %s", sub.ID)
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
	logger.CtxLog.Infof("Deleted subscription: %s", id)
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
