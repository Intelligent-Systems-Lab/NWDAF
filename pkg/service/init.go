package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	"github.com/free5gc/nwdaf/internal/anlf"
	anlfclient "github.com/free5gc/nwdaf/internal/anlf/client"
	anlfprocessor "github.com/free5gc/nwdaf/internal/anlf/processor"
	"github.com/free5gc/nwdaf/internal/backend"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/mtlf"
	mtlfclient "github.com/free5gc/nwdaf/internal/mtlf/client"
	mtlfprocessor "github.com/free5gc/nwdaf/internal/mtlf/processor"
	"github.com/free5gc/nwdaf/internal/sbi"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/internal/sbi/notifier"
	"github.com/free5gc/nwdaf/internal/sbi/processor"
	"github.com/free5gc/nwdaf/pkg/app"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
	"github.com/free5gc/openapi/oauth"
)

var _ app.App = &NwdafApp{}

const nrfDeregistrationTimeout = 5 * time.Second

type NwdafApp struct {
	cfg               *factory.Config
	nwdafCtx          *nwdaf_context.NWDAFContext
	ctx               context.Context
	cancel            context.CancelFunc
	consumer          *consumer.Consumer
	nrfManagement     consumer.NFManagementService
	processor         *processor.Processor
	reportDispatcher  *notifier.ReportDispatcher
	sbiServer         *sbi.Server
	anlfServer        *anlf.Server
	anlfAvailability  *backend.AvailabilityMonitor
	mtlfAvailability  *backend.AvailabilityMonitor
	anlfBackendClient *anlfclient.Client
	mtlfBackendClient *mtlfclient.BackendClient
	mtlfServer        *mtlf.Server
	processInstanceID string
	wg                sync.WaitGroup
	deregisterTimeout time.Duration
}

func NewApp(ctx context.Context, cfg *factory.Config) (*NwdafApp, error) {
	nwdaf := &NwdafApp{
		cfg:               cfg,
		wg:                sync.WaitGroup{},
		deregisterTimeout: nrfDeregistrationTimeout,
		processInstanceID: uuid.New().String(),
	}

	// Set log settings
	if cfg.Logger != nil {
		nwdaf.SetLogEnable(cfg.Logger.Enable)
		nwdaf.SetLogLevel(cfg.Logger.Level)
		nwdaf.SetReportCaller(cfg.Logger.ReportCaller)
	}

	nwdaf.ctx, nwdaf.cancel = context.WithCancel(ctx)

	// Initialize context
	nwdaf_context.InitWithNFInstanceID(cfg.GetNFInstanceID())
	nwdaf.nwdafCtx = nwdaf_context.GetSelf()
	if err := nwdaf.nwdafCtx.ConfigureNFManagement(nwdaf_context.NFManagementConfig{
		NrfURI:       cfg.GetNrfUri(),
		NrfCertPEM:   cfg.GetNrfCertPem(),
		NwdafName:    cfg.GetNwdafName(),
		SBIURI:       cfg.GetSbiUri(),
		SBIScheme:    cfg.GetSbiScheme(),
		RegisterIPv4: cfg.GetSbiRegisterIP(),
		SBIPort:      cfg.GetSbiPort(),
		ServiceNames: cfg.GetServiceNameList(),
		NwdafInfo:    cfg.GetNwdafInfo(),
	}); err != nil {
		return nil, fmt.Errorf("configure NRF NFManagement context: %w", err)
	}

	// Initialize consumer
	var err error
	nwdaf.consumer, err = consumer.NewConsumer(nwdaf)
	if err != nil {
		return nil, err
	}
	nwdaf.nrfManagement = nwdaf.consumer

	if cfg.Configuration != nil &&
		cfg.Configuration.AnlfBackend != nil &&
		cfg.Configuration.AnlfBackend.Enabled &&
		cfg.Configuration.AnlfBackend.Endpoint != "" {
		client := anlfclient.NewClient(
			cfg.Configuration.AnlfBackend.Endpoint,
			time.Duration(cfg.Configuration.AnlfBackend.RequestTimeoutOrDefault())*time.Second,
		)
		nwdaf.anlfBackendClient = client
		nwdaf.anlfAvailability = backend.NewAvailabilityMonitor(
			nwdaf.probeAnlfBackend,
			nwdaf.resetAnlfBackend,
		)
	}

	if cfg.Configuration != nil && cfg.Configuration.MtlfBackend != nil &&
		cfg.Configuration.MtlfBackend.Enabled {
		nwdaf.mtlfBackendClient, err = mtlfclient.NewBackendClient(
			cfg.Configuration.MtlfBackend.Endpoint,
			time.Duration(cfg.Configuration.MtlfBackend.RequestTimeoutOrDefault())*time.Second,
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("create MTLF backend client: %w", err)
		}
		nwdaf.mtlfAvailability = backend.NewAvailabilityMonitor(
			nwdaf.probeMtlfBackend,
			nwdaf.resetMtlfBackend,
		)
	}

	reportDispatcher := notifier.NewReportDispatcher(nwdaf.ctx)
	nwdaf.reportDispatcher = reportDispatcher
	anlfProcessor := anlfprocessor.NewProcessor(reportDispatcher)
	anlfProcessor.SetTrainingDataDescriptorRelay(
		nwdaf.mtlfBackendClient,
		nwdaf.mtlfAvailability,
	)
	if cfg.NrfRegistrationEnabled() {
		anlfProcessor.SetNFDiscoveryProxy(nwdaf.consumer)
	}
	anlfProcessor.SetSmfEventExposureProxy(nwdaf.consumer)
	anlfProcessor.SetUdmCollectionProxy(nwdaf.consumer)
	anlfProcessor.SetAdrfStorageProxy(nwdaf.consumer)

	// Initialize processor
	nwdaf.processor = processor.NewProcessor(nwdaf)
	nwdaf.processor.SetEventsSubscriptionBackend(nwdaf.anlfBackendClient, nwdaf.anlfAvailability)
	nwdaf.processor.SetMLModelBackends(
		nwdaf.mtlfBackendClient,
		nwdaf.anlfBackendClient,
		nwdaf.mtlfAvailability,
		nwdaf.anlfAvailability,
	)
	nwdaf.processor.SetMLModelPeerConsumer(nwdaf.consumer)

	// Initialize SBI server
	nwdaf.sbiServer, err = sbi.NewServer(nwdaf, "")
	if err != nil {
		return nil, err
	}
	nwdaf.anlfServer, err = anlf.NewServer(cfg, anlfProcessor)
	if err != nil {
		return nil, err
	}
	nwdaf.anlfServer.SetMLModelGateway(nwdaf.processor)
	var discoveryProxy interface {
		DiscoverNFInstances(context.Context, backend.NFDiscoveryQuery) (*consumer.NFDiscoveryResult, error)
	}
	if cfg.NrfRegistrationEnabled() {
		discoveryProxy = nwdaf.consumer
	}
	mtlfProcessor := mtlfprocessor.New(nwdaf.processor, discoveryProxy, nwdaf.consumer)
	nwdaf.mtlfServer, err = mtlf.NewServer(
		cfg,
		mtlfProcessor,
		nwdaf.processInstanceID,
	)
	if err != nil {
		return nil, err
	}
	return nwdaf, nil
}

func (a *NwdafApp) Config() *factory.Config {
	return a.cfg
}

func (a *NwdafApp) Context() *nwdaf_context.NWDAFContext {
	return a.nwdafCtx
}

func (a *NwdafApp) CancelContext() context.Context {
	return a.ctx
}

func (a *NwdafApp) Processor() *processor.Processor {
	return a.processor
}

func (a *NwdafApp) SetLogEnable(enable bool) {
	logger.MainLog.Infof("Log enable is set to [%v]", enable)
	if enable {
		logger.Log.SetOutput(os.Stderr)
	} else {
		logger.Log.SetOutput(io.Discard)
	}
}

func (a *NwdafApp) SetLogLevel(level string) {
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		logger.MainLog.Warnf("Log level [%s] is invalid", level)
		return
	}
	logger.MainLog.Infof("Log level is set to [%s]", level)
	logger.Log.SetLevel(lvl)
}

func (a *NwdafApp) SetReportCaller(reportCaller bool) {
	logger.MainLog.Infof("Report Caller is set to [%v]", reportCaller)
	logger.Log.SetReportCaller(reportCaller)
}

func (a *NwdafApp) Start() {
	if err := a.Run(); err != nil {
		logger.InitLog.Fatalf("Run NWDAF failed: %+v", err)
	}
}

func (a *NwdafApp) Run() error {
	logger.InitLog.Infoln("NWDAF starting")

	if err := a.startRuntime(); err != nil {
		a.cancel()
		if errors.Is(err, context.Canceled) && a.ctx.Err() != nil {
			logger.InitLog.Infoln("NWDAF startup canceled by shutdown signal")
			return nil
		}
		return err
	}

	a.WaitRoutineStopped()
	return nil
}

func (a *NwdafApp) startRuntime() error {
	if a.cfg.NrfRegistrationEnabled() {
		result, registrationErr := a.nrfManagement.RegisterNFInstance(a.ctx)
		a.nwdafCtx.RecordHeartBeatTimer(result.HeartBeatTimer)
		if registrationErr != nil {
			if result.RemoteRegistered {
				if result.OAuth2Required {
					a.nwdafCtx.RecordOAuth2Required(result.ResourceURI)
				} else {
					a.nwdafCtx.MarkRegistered(result.ResourceURI)
				}
				a.deregisterFromNrf()
			}
			return fmt.Errorf("register NWDAF with NRF: %w", registrationErr)
		}
		if result.OAuth2Required {
			a.nwdafCtx.RecordOAuth2Required(result.ResourceURI)
			a.logOAuthCertificateState()
		} else {
			a.nwdafCtx.MarkRegistered(result.ResourceURI)
		}
		logger.InitLog.Infof(
			"Registered NWDAF with NRF: nfInstanceId=%s oauth2Required=%t",
			a.nwdafCtx.NfId,
			result.OAuth2Required,
		)
	} else {
		logger.InitLog.Info("NRF registration is disabled for configured-endpoint deployment")
	}

	if ctxErr := a.ctx.Err(); ctxErr != nil {
		a.deregisterFromNrf()
		return fmt.Errorf("startup canceled after NRF registration: %w", ctxErr)
	}
	if serverErr := a.startOwnedServers(); serverErr != nil {
		a.deregisterFromNrf()
		a.stopOwnedServers()
		a.wg.Wait()
		return fmt.Errorf("start NWDAF listeners after NRF registration: %w", serverErr)
	}
	if ctxErr := a.ctx.Err(); ctxErr != nil {
		a.deregisterFromNrf()
		a.stopOwnedServers()
		a.wg.Wait()
		return fmt.Errorf("startup canceled after listener startup: %w", ctxErr)
	}

	a.wg.Add(1)
	go a.listenShutdownEvent()
	a.startBackendAvailabilityMonitors()
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.processor.RunMLModelPeerCleanup(a.ctx)
	}()
	logger.InitLog.Infoln("NWDAF startup complete")
	return nil
}

func (a *NwdafApp) startBackendAvailabilityMonitors() {
	for _, availability := range []*backend.AvailabilityMonitor{a.anlfAvailability, a.mtlfAvailability} {
		if availability == nil {
			continue
		}
		a.wg.Add(1)
		go func(connection *backend.AvailabilityMonitor) {
			defer a.wg.Done()
			connection.Run(a.ctx)
		}(availability)
	}
}

func (a *NwdafApp) probeMtlfBackend(ctx context.Context) (backend.ProbeResult, error) {
	if a.mtlfBackendClient == nil {
		return backend.ProbeResult{}, fmt.Errorf("MTLF backend client is not configured")
	}
	health, err := a.mtlfBackendClient.CheckReadiness(ctx)
	return backend.ProbeResult{
		ProcessInstanceID: health.ProcessInstanceID,
		Ready:             err == nil,
	}, err
}

func (a *NwdafApp) probeAnlfBackend(ctx context.Context) (backend.ProbeResult, error) {
	if a.anlfBackendClient == nil {
		return backend.ProbeResult{}, fmt.Errorf("AnLF backend client is not configured")
	}
	health, err := a.anlfBackendClient.CheckReadiness(ctx)
	return backend.ProbeResult{
		ProcessInstanceID: health.ProcessInstanceID,
		Ready:             err == nil,
	}, err
}

func (a *NwdafApp) resetAnlfBackend(ctx context.Context, generation string) {
	if a == nil || a.processor == nil {
		return
	}
	var dispatch func([]models.NnwdafEventsSubscriptionNotification, []byte) error
	if a.reportDispatcher != nil {
		dispatch = a.reportDispatcher.DispatchEventsSubscriptionNotifications
	}
	removed := a.processor.ResetAnalyticsGeneration(generation, dispatch)
	a.cleanupSmfResources(ctx, removed)
	a.processor.ResetMLModelBackendGeneration(ctx, backend.KindAnLF, generation)
	logger.InitLog.Warnf(
		"AnLF backend process generation reset: generation=%s analyticsSubscriptions=%d",
		generation,
		len(removed),
	)
}

func (a *NwdafApp) resetMtlfBackend(ctx context.Context, generation string) {
	if a != nil && a.processor != nil {
		a.processor.ResetMLModelBackendGeneration(ctx, backend.KindMTLF, generation)
		logger.InitLog.Warnf(
			"MTLF backend process generation reset: generation=%s",
			generation,
		)
	}
}

func (a *NwdafApp) cleanupSmfResources(ctx context.Context, removedSubscriptionIDs []string) {
	if a == nil || a.nwdafCtx == nil || a.consumer == nil || len(removedSubscriptionIDs) == 0 {
		return
	}
	removed := make(map[string]struct{}, len(removedSubscriptionIDs))
	for _, id := range removedSubscriptionIDs {
		removed[id] = struct{}{}
	}
	for _, route := range a.nwdafCtx.GetAllSmfPeerResourceRoutes() {
		retained := route.NwdafSubscriptionIDs[:0]
		for _, id := range route.NwdafSubscriptionIDs {
			if _, found := removed[id]; !found {
				retained = append(retained, id)
			}
		}
		route.NwdafSubscriptionIDs = retained
		if len(retained) > 0 {
			a.nwdafCtx.UpdateSmfPeerResourceRoute(route)
			continue
		}
		if _, err := a.consumer.DeleteSmfEventExposure(
			ctx,
			route.TargetAPIBaseURI,
			route.SubscriptionID,
		); err != nil {
			logger.InitLog.Warnf(
				"Failed to clean SMF resource after AnLF loss: apiRoot=%s subscriptionId=%s err=%v",
				route.TargetAPIBaseURI,
				route.SubscriptionID,
				err,
			)
		}
		a.nwdafCtx.DeleteSmfPeerResourceRoute(
			route.TargetAPIBaseURI,
			route.SubscriptionID,
		)
	}
}

func (a *NwdafApp) logOAuthCertificateState() {
	certPath := a.nwdafCtx.NrfCertPem()
	if certPath == "" {
		logger.InitLog.Error(
			"NRF requires OAuth2 but nrfCertPem is not configured; " +
				"protected inbound SBI requests will be rejected",
		)
		return
	}
	if _, err := oauth.ParsePublicKeyFromPEM(certPath); err != nil {
		logger.InitLog.Errorf(
			"NRF requires OAuth2 but nrfCertPem is unusable; "+
				"protected inbound SBI requests will be rejected: path=%s err=%v",
			certPath,
			err,
		)
	}
}

func (a *NwdafApp) listenShutdownEvent() {
	defer func() {
		if p := recover(); p != nil {
			logger.InitLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}
		a.wg.Done()
	}()

	<-a.ctx.Done()
	a.terminateProcedure()
}

func (a *NwdafApp) Terminate() {
	a.cancel()
}

// startOwnedServers leaves already-started listeners running on error so the
// lifecycle owner can deregister from NRF before listener cleanup.
func (a *NwdafApp) startOwnedServers() error {
	if err := a.sbiServer.Run(&a.wg); err != nil {
		return err
	}
	if err := a.anlfServer.Run(&a.wg); err != nil {
		return err
	}
	if a.mtlfServer != nil {
		if err := a.mtlfServer.Run(&a.wg); err != nil {
			return err
		}
	}
	return nil
}

func (a *NwdafApp) stopOwnedServers() {
	if a.mtlfServer != nil {
		a.mtlfServer.Shutdown()
	}
	if a.anlfServer != nil {
		a.anlfServer.Shutdown()
	}
	if a.sbiServer != nil {
		a.sbiServer.Shutdown()
	}
}

func (a *NwdafApp) terminateProcedure() {
	logger.MainLog.Infof("Terminating NWDAF...")

	a.deregisterFromNrf()

	a.stopOwnedServers()

	logger.InitLog.Infof("NWDAF terminated")
}

func (a *NwdafApp) deregisterFromNrf() {
	if a.nrfManagement == nil || !a.nwdafCtx.RegistrationState().Registered {
		return
	}

	timeout := a.deregisterTimeout
	if timeout <= 0 {
		timeout = nrfDeregistrationTimeout
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := a.nrfManagement.DeregisterNFInstance(cleanupCtx); err != nil {
		logger.InitLog.Errorf(
			"Deregister NWDAF from NRF failed: nfInstanceId=%s err=%v",
			a.nwdafCtx.NfId,
			err,
		)
		return
	}
	a.nwdafCtx.MarkDeregistered()
	logger.InitLog.Infof("Deregistered NWDAF from NRF: nfInstanceId=%s", a.nwdafCtx.NfId)
}

func (a *NwdafApp) WaitRoutineStopped() {
	a.wg.Wait()
	logger.MainLog.Infof("NWDAF App is terminated")
}
