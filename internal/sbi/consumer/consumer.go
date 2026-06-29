// Package consumer provides client services for consuming external NF APIs
// Following free5gc consumer pattern for modular NF client management
package consumer

import (
	"context"
	"encoding/json"
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
	SubscribeToSmf(ctx context.Context, smfEndpoint string, opts SmfSubscriptionOptions) (string, error)
	UnsubscribeFromSmf(ctx context.Context, smfEndpoint string, subscriptionId string) error
	HTTPClient() *http.Client
}

type MtlfServiceClient interface {
	SubscribeToMtlf(ctx context.Context, mtlfEndpoint string, opts MtlfSubscriptionOptions) (string, error)
	UnsubscribeFromMtlf(ctx context.Context, mtlfEndpoint string, subscriptionId string) error
	HTTPClient() *http.Client
}

type MlServiceAPI interface {
	InitializeModel(ctx context.Context, modelUrl string) (string, error)
	UnloadModel(ctx context.Context, modelId string) error
	Predict(ctx context.Context, modelId string, trafficData []TrafficObservation) (*PredictResponse, error)
	HTTPClient() *http.Client
}

type DaisyServiceAPI interface {
	TriggerTrainingAsync(ctx context.Context, task map[string]any, callbackURL string, tidOverride string) (string, error)
	UploadData(ctx context.Context, tid string, groupId string, upfEventNotifs []json.RawMessage) error
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
	SubscribeToSmf(ctx context.Context, smfEndpoint string, opts SmfSubscriptionOptions) (string, error)
	UnsubscribeFromSmf(ctx context.Context, smfEndpoint string, subscriptionId string) error
	SubscribeToMtlf(ctx context.Context, mtlfEndpoint string, opts MtlfSubscriptionOptions) (string, error)
	UnsubscribeFromMtlf(ctx context.Context, mtlfEndpoint string, subscriptionId string) error
	MlClient() MlServiceAPI
	DaisyClient() DaisyServiceAPI
	AdrfClient() AdrfServiceAPI
}

// Consumer aggregates all external NF service clients.
type Consumer struct {
	nwdaf

	smfService   SmfServiceClient
	mtlfService  MtlfServiceClient
	mlService    MlServiceAPI
	daisyService DaisyServiceAPI
	Adrf         AdrfServiceAPI // nil if ADRF not configured
}

func newConsumerWithServices(
	nwdaf nwdaf,
	smfService SmfServiceClient,
	mtlfService MtlfServiceClient,
	mlService MlServiceAPI,
	daisyService DaisyServiceAPI,
	adrf AdrfServiceAPI,
) *Consumer {
	return &Consumer{
		nwdaf:        nwdaf,
		smfService:   smfService,
		mtlfService:  mtlfService,
		mlService:    mlService,
		daisyService: daisyService,
		Adrf:         adrf,
	}
}

// NewConsumer creates a new Consumer with all service clients initialized.
func NewConsumer(nwdaf nwdaf) (*Consumer, error) {
	c := newConsumerWithServices(
		nwdaf,
		NewNsmfService(),
		NewNmtlfService(),
		nil,
		nil,
		nil,
	)

	if nwdaf != nil {
		cfg := nwdaf.Config()
		if cfg != nil && cfg.Configuration != nil {
			if mlCfg := cfg.Configuration.MlService; mlCfg != nil && mlCfg.Enabled && mlCfg.Endpoint != "" {
				c.mlService = NewMlServiceClient(mlCfg.Endpoint)
				consumerLog.Info("ML service client initialized")
			}
			if mtlfCfg := cfg.Configuration.Mtlf; mtlfCfg != nil && mtlfCfg.Enabled && mtlfCfg.Endpoint != "" {
				c.daisyService = NewDaisyClient(mtlfCfg.Endpoint)
				consumerLog.Info("Daisy client initialized")
			}
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
	return c.smfService.SubscribeToSmf(ctx, smfEndpoint, opts)
}

func (c *Consumer) UnsubscribeFromSmf(
	ctx context.Context,
	smfEndpoint string,
	subscriptionId string,
) error {
	return c.smfService.UnsubscribeFromSmf(ctx, smfEndpoint, subscriptionId)
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

func (c *Consumer) MlClient() MlServiceAPI {
	return c.mlService
}

func (c *Consumer) DaisyClient() DaisyServiceAPI {
	return c.daisyService
}

func (c *Consumer) AdrfClient() AdrfServiceAPI {
	return c.Adrf
}
