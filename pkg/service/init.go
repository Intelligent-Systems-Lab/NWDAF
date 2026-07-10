package service

import (
	"context"
	"io"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/free5gc/nwdaf/internal/anlf"
	anlfclient "github.com/free5gc/nwdaf/internal/anlf/client"
	anlfprocessor "github.com/free5gc/nwdaf/internal/anlf/processor"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/mtlf"
	mtlfclient "github.com/free5gc/nwdaf/internal/mtlf/client"
	mtlfprocessor "github.com/free5gc/nwdaf/internal/mtlf/processor"
	"github.com/free5gc/nwdaf/internal/notifier"
	"github.com/free5gc/nwdaf/internal/sbi"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/internal/sbi/processor"
	"github.com/free5gc/nwdaf/pkg/app"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/util/mongoapi"
)

var _ app.App = &NwdafApp{}

type NwdafApp struct {
	cfg        *factory.Config
	nwdafCtx   *nwdaf_context.NWDAFContext
	ctx        context.Context
	cancel     context.CancelFunc
	consumer   *consumer.Consumer
	processor  *processor.Processor
	sbiServer  *sbi.Server
	anlfServer *anlf.Server
	mtlfServer *mtlf.Server
	wg         sync.WaitGroup
}

func NewApp(ctx context.Context, cfg *factory.Config) (*NwdafApp, error) {
	nwdaf := &NwdafApp{
		cfg: cfg,
		wg:  sync.WaitGroup{},
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
	nwdaf.nwdafCtx.NwdafName = cfg.GetNwdafName()

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

	var anlfBackend anlf.AnlfBackendAPI
	if cfg.Configuration != nil &&
		cfg.Configuration.AnlfBackend != nil &&
		cfg.Configuration.AnlfBackend.Enabled &&
		cfg.Configuration.AnlfBackend.Endpoint != "" {
		anlfBackend = anlfclient.NewClient(cfg.Configuration.AnlfBackend.Endpoint)
	}

	var daisyClient mtlf.DaisyAPI
	if cfg.Configuration != nil &&
		cfg.Configuration.Mtlf != nil &&
		cfg.Configuration.Mtlf.Enabled &&
		cfg.Configuration.Mtlf.Endpoint != "" {
		daisyClient = mtlfclient.NewClient(cfg.Configuration.Mtlf.Endpoint)
	}

	anlfService := anlf.NewAnlfService(nwdaf, anlfBackend)
	mtlfService := mtlf.NewMtlfService(nwdaf, daisyClient, nwdaf.consumer.AdrfClient())
	reportDispatcher := notifier.NewReportDispatcher(nwdaf.ctx, anlfService)
	anlfProcessor := anlfprocessor.NewProcessor(anlfService, reportDispatcher)
	mtlfProcessor := mtlfprocessor.NewProcessor(mtlfService)

	// Initialize processor
	nwdaf.processor = processor.NewProcessor(nwdaf, anlfService, mtlfService)

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
	logger.InitLog.Infoln("NWDAF Server started")

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

	a.wg.Add(1)
	go a.listenShutdownEvent()

	// Set WaitGroup for processor goroutine lifecycle management
	a.processor.SetWaitGroup(&a.wg)

	if err := a.startOwnedServers(); err != nil {
		logger.InitLog.Fatalf("Run NWDAF servers failed: %+v", err)
	}
	a.processor.StartObservationDelivery()

	// Start MTLF training scheduler only after the owned listeners are ready.
	a.processor.StartMtlfTrainingScheduler(&a.wg)

	a.WaitRoutineStopped()
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

func (a *NwdafApp) startOwnedServers() error {
	if err := a.anlfServer.Run(&a.wg); err != nil {
		return err
	}
	if err := a.mtlfServer.Run(&a.wg); err != nil {
		a.anlfServer.Shutdown()
		return err
	}
	if err := a.sbiServer.Run(&a.wg); err != nil {
		a.anlfServer.Shutdown()
		a.mtlfServer.Shutdown()
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

	a.processor.StopObservationDelivery()

	a.stopOwnedServers()

	logger.InitLog.Infof("NWDAF terminated")
}

func (a *NwdafApp) WaitRoutineStopped() {
	a.wg.Wait()
	logger.MainLog.Infof("NWDAF App is terminated")
}
