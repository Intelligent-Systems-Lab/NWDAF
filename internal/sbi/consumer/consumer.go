// Package consumer provides client services for consuming external NF APIs
// Following free5gc consumer pattern for modular NF client management
package consumer

import (
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/factory"
)

var consumerLog = logger.ConsLog

// Consumer aggregates all external NF service clients
// NsmfService and NmtlfService are embedded, so their exported methods are automatically
// promoted to Consumer (no manual wrapper methods needed)
type Consumer struct {
	*NsmfService
	*NmtlfService
	Adrf *AdrfClient // nil if ADRF not configured
}

// NewConsumer creates a new Consumer with all service clients initialized
func NewConsumer() (*Consumer, error) {
	c := &Consumer{}

	// Initialize SMF service with the consumer reference
	c.NsmfService = NewNsmfService(c)

	// Initialize MTLF service with the consumer reference
	c.NmtlfService = NewNmtlfService(c)

	if factory.NwdafConfig.Configuration.Adrf.AdrfEnabled() {
		c.Adrf = NewAdrfClient(factory.NwdafConfig.Configuration.Adrf.Url)
		consumerLog.Infof("ADRF client initialized: url=%s", factory.NwdafConfig.Configuration.Adrf.Url)
	}

	consumerLog.Info("Consumer initialized")
	return c, nil
}

// Context returns the NWDAF context
func (c *Consumer) Context() *nwdaf_context.NWDAFContext {
	return nwdaf_context.GetSelf()
}
