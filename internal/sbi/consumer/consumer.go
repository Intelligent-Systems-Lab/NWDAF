// Package consumer provides client services for consuming external NF APIs
// Following free5gc consumer pattern for modular NF client management
package consumer

import (
	"net/http"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/app"
)

var consumerLog = logger.ConsLog

type nwdaf interface {
	app.App
}

type SmfServiceClient interface {
	SubscribeToSmf(smfEndpoint string, opts SmfSubscriptionOptions) (string, error)
	UnsubscribeFromSmf(smfEndpoint string, subscriptionId string) error
	HTTPClient() *http.Client
}

type MtlfServiceClient interface {
	SubscribeToMtlf(mtlfEndpoint string, opts MtlfSubscriptionOptions) (string, error)
	UnsubscribeFromMtlf(mtlfEndpoint string, subscriptionId string) error
	HTTPClient() *http.Client
}

// Consumer aggregates all external NF service clients.
type Consumer struct {
	nwdaf

	smfService  SmfServiceClient
	mtlfService MtlfServiceClient
	Adrf        *AdrfClient // nil if ADRF not configured
}

func NewConsumerWithServices(
	nwdaf nwdaf,
	smfService SmfServiceClient,
	mtlfService MtlfServiceClient,
	adrf *AdrfClient,
) *Consumer {
	return &Consumer{
		nwdaf:       nwdaf,
		smfService:  smfService,
		mtlfService: mtlfService,
		Adrf:        adrf,
	}
}

// NewConsumer creates a new Consumer with all service clients initialized.
func NewConsumer(nwdaf nwdaf) (*Consumer, error) {
	c := NewConsumerWithServices(
		nwdaf,
		NewNsmfService(),
		NewNmtlfService(),
		nil,
	)

	if nwdaf != nil {
		cfg := nwdaf.Config()
		if cfg != nil && cfg.Configuration != nil && cfg.Configuration.Adrf.AdrfEnabled() {
			c.Adrf = NewAdrfClient(cfg.Configuration.Adrf.Url)
			consumerLog.Infof("ADRF client initialized: url=%s", cfg.Configuration.Adrf.Url)
		}
	}

	consumerLog.Info("Consumer initialized")
	return c, nil
}

// Context returns the NWDAF context
func (c *Consumer) Context() *nwdaf_context.NWDAFContext {
	if c.nwdaf != nil {
		return c.nwdaf.Context()
	}
	return nwdaf_context.GetSelf()
}

func (c *Consumer) SubscribeToSmf(smfEndpoint string, opts SmfSubscriptionOptions) (string, error) {
	return c.smfService.SubscribeToSmf(smfEndpoint, opts)
}

func (c *Consumer) UnsubscribeFromSmf(smfEndpoint string, subscriptionId string) error {
	return c.smfService.UnsubscribeFromSmf(smfEndpoint, subscriptionId)
}

func (c *Consumer) SubscribeToMtlf(mtlfEndpoint string, opts MtlfSubscriptionOptions) (string, error) {
	return c.mtlfService.SubscribeToMtlf(mtlfEndpoint, opts)
}

func (c *Consumer) UnsubscribeFromMtlf(mtlfEndpoint string, subscriptionId string) error {
	return c.mtlfService.UnsubscribeFromMtlf(mtlfEndpoint, subscriptionId)
}

func (c *Consumer) SmfService() SmfServiceClient {
	return c.smfService
}

func (c *Consumer) MtlfService() MtlfServiceClient {
	return c.mtlfService
}
