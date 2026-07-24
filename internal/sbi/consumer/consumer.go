// Package consumer provides client services for consuming external NF APIs
// Following free5gc consumer pattern for modular NF client management
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

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

	adrfClientsMu sync.Mutex
	adrfClients   map[string]*AdrfClient
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
		adrfClients: make(map[string]*AdrfClient),
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

func (c *Consumer) DiscoverSmfProfiles(ctx context.Context) (*NFDiscoveryResult, error) {
	return c.nrfService.DiscoverSmfProfiles(ctx, c.Context())
}

func (c *Consumer) DiscoverNFInstances(
	ctx context.Context,
	query NFDiscoveryQuery,
) (*NFDiscoveryResult, error) {
	return c.nrfService.DiscoverNFInstances(ctx, c.Context(), query)
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

func (c *Consumer) StoreAdrfDataRecord(
	ctx context.Context,
	targetAPIBaseURI string,
	body []byte,
) (*StandardAdrfResponse, error) {
	if targetAPIBaseURI == "" {
		return nil, errors.New("ADRF target API root is required")
	}
	client := c.adrfClientForTarget(targetAPIBaseURI)
	return client.ExecuteStandardStorageRequest(ctx, body)
}

func (c *Consumer) CreateAdrfRetrievalSubscription(
	ctx context.Context,
	targetAPIBaseURI string,
	body []byte,
) (*StandardAdrfResponse, error) {
	if targetAPIBaseURI == "" {
		return nil, errors.New("ADRF target API root is required")
	}
	return c.adrfClientForTarget(targetAPIBaseURI).ExecuteStandardRetrievalSubscribe(ctx, body)
}

func (c *Consumer) DeleteAdrfRetrievalSubscription(
	ctx context.Context,
	targetAPIBaseURI string,
	resourceLocation string,
) (*StandardAdrfResponse, error) {
	if targetAPIBaseURI == "" || resourceLocation == "" {
		return nil, errors.New("ADRF target and resource Location are required")
	}
	return c.adrfClientForTarget(targetAPIBaseURI).
		ExecuteStandardRetrievalUnsubscribe(ctx, resourceLocation)
}

func (c *Consumer) adrfClientForTarget(targetAPIBaseURI string) *AdrfClient {
	target := strings.TrimRight(strings.TrimSpace(targetAPIBaseURI), "/")
	c.adrfClientsMu.Lock()
	defer c.adrfClientsMu.Unlock()
	if client, ok := c.adrfClients[target]; ok {
		return client
	}
	client := NewAdrfClient(target)
	c.adrfClients[target] = client
	return client
}

func (c *Consumer) RegisterNFInstance(ctx context.Context) (RegistrationResult, error) {
	return c.nrfService.RegisterNFInstance(ctx, c.Context())
}

func (c *Consumer) DeregisterNFInstance(ctx context.Context) error {
	return c.nrfService.DeregisterNFInstance(ctx, c.Context())
}
