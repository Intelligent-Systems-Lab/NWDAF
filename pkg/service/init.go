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

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/free5gc/nwdaf/internal/anlf"
	anlfclient "github.com/free5gc/nwdaf/internal/anlf/client"
	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/anlf/coordinator"
	anlfprocessor "github.com/free5gc/nwdaf/internal/anlf/processor"
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
	"github.com/free5gc/openapi/oauth"
	"github.com/free5gc/util/mongoapi"
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
	sbiServer         *sbi.Server
	anlfServer        *anlf.Server
	anlfCoordinator   *coordinator.Coordinator
	mtlfServer        *mtlf.Server
	wg                sync.WaitGroup
	deregisterTimeout time.Duration
}

func NewApp(ctx context.Context, cfg *factory.Config) (*NwdafApp, error) {
	nwdaf := &NwdafApp{
		cfg:               cfg,
		wg:                sync.WaitGroup{},
		deregisterTimeout: nrfDeregistrationTimeout,
	}

	// Set log settings
	if cfg.Logger != nil {
		nwdaf.SetLogEnable(cfg.Logger.Enable)
		nwdaf.SetLogLevel(cfg.Logger.Level)
		nwdaf.SetReportCaller(cfg.Logger.ReportCaller)
	}

	nwdaf.ctx, nwdaf.cancel = context.WithCancel(ctx)

	// Initialize context
	nwdaf_context.Init()
	nwdaf.nwdafCtx = nwdaf_context.GetSelf()
	if err := nwdaf.nwdafCtx.ConfigureNFManagement(
		cfg.GetNrfUri(),
		cfg.GetNrfCertPem(),
		cfg.GetNwdafName(),
		cfg.GetSbiUri(),
		cfg.GetSbiScheme(),
		cfg.GetSbiRegisterIP(),
		cfg.GetSbiPort(),
	); err != nil {
		return nil, fmt.Errorf("configure NRF NFManagement context: %w", err)
	}

	// Initialize GroupResolver for Group ID → SUPI resolution
	// Per TS 23.502 §4.15.4.5.2: NWDAF must resolve Group IDs before SMF subscription
	if cfg.Configuration != nil && cfg.Configuration.GroupMembership != nil {
		groupResolver := nwdaf_context.NewGroupResolver(cfg.Configuration.GroupMembership)
		nwdaf.nwdafCtx.SetGroupResolver(groupResolver)
	}

	// Initialize consumer
	var err error
	nwdaf.consumer, err = consumer.NewConsumer(nwdaf)
	if err != nil {
		return nil, err
	}
	nwdaf.nrfManagement = nwdaf.consumer

	var anlfBackend coordinator.BackendRuntimeClient
	var observationBackend coordinator.ObservationSender
	if cfg.Configuration != nil &&
		cfg.Configuration.AnlfBackend != nil &&
		cfg.Configuration.AnlfBackend.Enabled &&
		cfg.Configuration.AnlfBackend.Endpoint != "" {
		client := anlfclient.NewClient(cfg.Configuration.AnlfBackend.Endpoint)
		anlfBackend = client
		observationBackend = client
	}

	var daisyClient mtlf.DaisyAPI
	if cfg.Configuration != nil &&
		cfg.Configuration.Mtlf != nil &&
		cfg.Configuration.Mtlf.Enabled &&
		cfg.Configuration.Mtlf.Endpoint != "" {
		daisyClient = mtlfclient.NewClient(cfg.Configuration.Mtlf.Endpoint)
	}

	var observationConfig *factory.ObservationDeliveryConfig
	if cfg.Configuration != nil && cfg.Configuration.AnlfBackend != nil {
		observationConfig = cfg.Configuration.AnlfBackend.ObservationDelivery
	}
	observationDelivery := coordinator.NewObservationDelivery(
		nwdaf.ctx,
		observationBackend,
		observationConfig,
	)
	nwdaf.anlfCoordinator = coordinator.New(nwdaf, anlfBackend, observationDelivery)
	mtlfService := mtlf.NewMtlfService(nwdaf, daisyClient, nwdaf.consumer.AdrfClient())
	mtlfService.SetOnModelProvisionEvent(func(event contract.ModelProvisionEvent) error {
		_, eventErr := nwdaf.anlfCoordinator.ApplyModelProvisionEvent(event)
		return eventErr
	})
	reportDispatcher := notifier.NewReportDispatcher(nwdaf.ctx)
	anlfProcessor := anlfprocessor.NewProcessor(nwdaf.anlfCoordinator, reportDispatcher)
	anlfProcessor.SetModelAccuracyWorkflow(mtlfService)
	mtlfProcessor := mtlfprocessor.NewProcessor(mtlfService)

	// Initialize processor
	nwdaf.processor = processor.NewProcessor(nwdaf, nwdaf.anlfCoordinator, mtlfService)

	// Initialize SBI server
	nwdaf.sbiServer, err = sbi.NewServer(nwdaf, "")
	if err != nil {
		return nil, err
	}
	nwdaf.anlfServer, err = anlf.NewServer(cfg, anlfProcessor)
	if err != nil {
		return nil, err
	}
	nwdaf.mtlfServer, err = mtlf.NewServer(cfg, mtlfProcessor)
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

func (a *NwdafApp) Consumer() consumer.ConsumerAPI {
	return a.consumer
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

	// Connect to MongoDB
	if a.cfg.Configuration != nil && a.cfg.Configuration.Mongodb != nil {
		mongodb := a.cfg.Configuration.Mongodb
		if err := mongoapi.SetMongoDB(mongodb.Name, mongodb.Url); err != nil {
			logger.InitLog.Errorf("Fail to connect to MongoDB: %+v", err)
		} else {
			// SetMongoDB does not verify the actual connection; Ping to confirm.
			pingCtx, pingCancel := context.WithTimeout(a.ctx, 5*time.Second)
			defer pingCancel()
			if pingErr := mongoapi.Client.Ping(pingCtx, nil); pingErr != nil {
				logger.InitLog.Errorf("MongoDB not reachable (%s): %v", mongodb.Url, pingErr)
			} else {
				logger.InitLog.Infof("Successfully connected to MongoDB (%s)", mongodb.Url)
				nwdaf_context.SetMongoAvailable(true)

				// Initialize Time Series Collection for UPF Traffic Data
				opts := options.CreateCollection().SetTimeSeriesOptions(
					options.TimeSeries().
						SetTimeField("timestamp").
						SetMetaField("metadata"),
				)
				collCtx, collCancel := context.WithTimeout(a.ctx, 5*time.Second)
				defer collCancel()
				collErr := mongoapi.Client.Database(mongodb.Name).CreateCollection(
					collCtx,
					nwdaf_context.UpfTrafficDataColl,
					opts,
				)
				if collErr != nil {
					// It's normal if the collection already exists
					logger.InitLog.Debugf("MongoDB TimeSeries collection creation note: %v", collErr)
				} else {
					logger.InitLog.Infof("Created MongoDB TimeSeries collection: %s", nwdaf_context.UpfTrafficDataColl)
				}
			}
		}
	}

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
	// Set WaitGroup before starting any app-owned worker.
	a.processor.SetWaitGroup(&a.wg)
	a.anlfCoordinator.SetWaitGroup(&a.wg)

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
	a.anlfCoordinator.StartObservationDelivery()
	a.processor.StartMtlfTrainingScheduler(&a.wg)
	logger.InitLog.Infoln("NWDAF startup complete")
	return nil
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
	if err := a.mtlfServer.Run(&a.wg); err != nil {
		return err
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

	a.anlfCoordinator.StopObservationDelivery()

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
