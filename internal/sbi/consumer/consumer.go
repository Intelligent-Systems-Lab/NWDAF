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
	SubscribeToSmf(smfEndpoint string, opts SmfSubscriptionOptions) (string, error)
	UnsubscribeFromSmf(smfEndpoint string, subscriptionId string) error
	HTTPClient() *http.Client
}

type MtlfServiceClient interface {
	SubscribeToMtlf(mtlfEndpoint string, opts MtlfSubscriptionOptions) (string, error)
	UnsubscribeFromMtlf(mtlfEndpoint string, subscriptionId string) error
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
	SubscribeToSmf(smfEndpoint string, opts SmfSubscriptionOptions) (string, error)
	UnsubscribeFromSmf(smfEndpoint string, subscriptionId string) error
	SubscribeToMtlf(mtlfEndpoint string, opts MtlfSubscriptionOptions) (string, error)
	UnsubscribeFromMtlf(mtlfEndpoint string, subscriptionId string) error
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
				consumerLog.Infof("ML service client initialized: endpoint=%s", mlCfg.Endpoint)
			}
			if mtlfCfg := cfg.Configuration.Mtlf; mtlfCfg != nil && mtlfCfg.Enabled && mtlfCfg.Endpoint != "" {
				c.daisyService = NewDaisyClient(mtlfCfg.Endpoint)
				consumerLog.Infof("Daisy client initialized: endpoint=%s", mtlfCfg.Endpoint)
			}
			if cfg.Configuration.Adrf.AdrfEnabled() {
				c.Adrf = NewAdrfClient(cfg.Configuration.Adrf.Url)
				consumerLog.Infof("ADRF client initialized: url=%s", cfg.Configuration.Adrf.Url)
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

func (c *Consumer) MlClient() MlServiceAPI {
	return c.mlService
}

func (c *Consumer) DaisyClient() DaisyServiceAPI {
	return c.daisyService
}

func (c *Consumer) AdrfClient() AdrfServiceAPI {
	return c.Adrf
}
