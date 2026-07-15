// Package consumer provides client services for consuming external NF APIs
// Following free5gc consumer pattern for modular NF client management
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/app"
	"github.com/free5gc/openapi/models"
)

var consumerLog = logger.ConsLog

type nwdaf interface {
	app.App
}

type SmfServiceClient interface {
	SubscribeToSmf(ctx context.Context, smfEndpoint string, opts SmfSubscriptionOptions) (string, error)
	UnsubscribeFromSmf(ctx context.Context, smfEndpoint string, subscriptionId string) error
	HTTPClient() *http.Client
}

type MtlfServiceClient interface {
	SubscribeToMtlf(ctx context.Context, mtlfEndpoint string, opts MtlfSubscriptionOptions) (string, error)
	UnsubscribeFromMtlf(ctx context.Context, mtlfEndpoint string, subscriptionId string) error
	HTTPClient() *http.Client
}

type AdrfServiceAPI interface {
	StorageRequest(ctx context.Context, info *nwdaf_context.AdrfSmfInfo, upfNotifJSONs []json.RawMessage) (string, error)
	RetrievalSubscribe(
		ctx context.Context,
		info *nwdaf_context.AdrfSmfInfo,
		notifCorrId string,
		notifURI string,
		timePeriod AdrfTimePeriod,
	) (string, error)
	RetrievalRequest(ctx context.Context, fetchCorrIds []string) (*NadrfDataStoreRecord, error)
	RetrievalUnsubscribe(ctx context.Context, subscriptionId string) error
	HTTPClient() *http.Client
}

type ConsumerAPI interface {
	DiscoverSmfEventExposure(ctx context.Context) ([]string, error)
	SubscribeToSmf(ctx context.Context, smfEndpoint string, opts SmfSubscriptionOptions) (string, error)
	UnsubscribeFromSmf(ctx context.Context, smfEndpoint string, subscriptionId string) error
	SubscribeToMtlf(ctx context.Context, mtlfEndpoint string, opts MtlfSubscriptionOptions) (string, error)
	UnsubscribeFromMtlf(ctx context.Context, mtlfEndpoint string, subscriptionId string) error
	AdrfClient() AdrfServiceAPI
}

// Consumer aggregates all external NF service clients.
type Consumer struct {
	nwdaf

	smfService  SmfServiceClient
	mtlfService MtlfServiceClient
	nrfService  *NrfService
	Adrf        AdrfServiceAPI // nil if ADRF not configured
}

func newConsumerWithServices(
	nwdaf nwdaf,
	smfService SmfServiceClient,
	mtlfService MtlfServiceClient,
	adrf AdrfServiceAPI,
) *Consumer {
	return &Consumer{
		nwdaf:       nwdaf,
		smfService:  smfService,
		mtlfService: mtlfService,
		nrfService:  newNrfService(),
		Adrf:        adrf,
	}
}

// NewConsumer creates a new Consumer with all service clients initialized.
func NewConsumer(nwdaf nwdaf) (*Consumer, error) {
	c := newConsumerWithServices(
		nwdaf,
		NewNsmfService(),
		NewNmtlfService(),
		nil,
	)

	if nwdaf != nil {
		cfg := nwdaf.Config()
		if cfg != nil && cfg.Configuration != nil {
			if cfg.Configuration.Adrf.AdrfEnabled() {
				c.Adrf = NewAdrfClient(cfg.Configuration.Adrf.Url)
				consumerLog.Info("ADRF client initialized")
			}
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

func (c *Consumer) SubscribeToSmf(
	ctx context.Context,
	smfEndpoint string,
	opts SmfSubscriptionOptions,
) (string, error) {
	requestCtx, err := c.smfRequestContext(ctx)
	if err != nil {
		return "", err
	}
	return c.smfService.SubscribeToSmf(requestCtx, smfEndpoint, opts)
}

func (c *Consumer) UnsubscribeFromSmf(
	ctx context.Context,
	smfEndpoint string,
	subscriptionId string,
) error {
	requestCtx, err := c.smfRequestContext(ctx)
	if err != nil {
		return err
	}
	return c.smfService.UnsubscribeFromSmf(requestCtx, smfEndpoint, subscriptionId)
}

func (c *Consumer) DiscoverSmfEventExposure(ctx context.Context) ([]string, error) {
	return c.nrfService.DiscoverSmfEventExposure(ctx, c.Context())
}

func (c *Consumer) smfRequestContext(ctx context.Context) (context.Context, error) {
	nwdafCtx := c.Context()
	if nwdafCtx == nil || !nwdafCtx.RegistrationState().OAuth2Required {
		return ctx, nil
	}
	requestCtx, err := c.nrfService.getTokenContext(
		ctx,
		nwdafCtx,
		models.ServiceName_NSMF_EVENT_EXPOSURE,
		models.NrfNfManagementNfType_SMF,
	)
	if err != nil {
		return nil, fmt.Errorf("authorize SMF Event Exposure request: %w", err)
	}
	return requestCtx, nil
}

func (c *Consumer) SubscribeToMtlf(
	ctx context.Context,
	mtlfEndpoint string,
	opts MtlfSubscriptionOptions,
) (string, error) {
	return c.mtlfService.SubscribeToMtlf(ctx, mtlfEndpoint, opts)
}

func (c *Consumer) UnsubscribeFromMtlf(
	ctx context.Context,
	mtlfEndpoint string,
	subscriptionId string,
) error {
	return c.mtlfService.UnsubscribeFromMtlf(ctx, mtlfEndpoint, subscriptionId)
}

func (c *Consumer) SmfService() SmfServiceClient {
	return c.smfService
}

func (c *Consumer) MtlfService() MtlfServiceClient {
	return c.mtlfService
}

func (c *Consumer) AdrfClient() AdrfServiceAPI {
	return c.Adrf
}

func (c *Consumer) RegisterNFInstance(ctx context.Context) (RegistrationResult, error) {
	return c.nrfService.RegisterNFInstance(ctx, c.Context())
}

func (c *Consumer) DeregisterNFInstance(ctx context.Context) error {
	return c.nrfService.DeregisterNFInstance(ctx, c.Context())
}
