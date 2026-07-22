package sbi

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/processor"
	"github.com/free5gc/nwdaf/pkg/app"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
	"github.com/free5gc/util/httpwrapper"
	logger_util "github.com/free5gc/util/logger"
	"github.com/free5gc/util/metrics"
)

type Route struct {
	Name    string
	Method  string
	Pattern string
	APIFunc gin.HandlerFunc
}

func applyRoutes(group *gin.RouterGroup, routes []Route) {
	for _, route := range routes {
		switch route.Method {
		case "GET":
			group.GET(route.Pattern, route.APIFunc)
		case "POST":
			group.POST(route.Pattern, route.APIFunc)
		case "PUT":
			group.PUT(route.Pattern, route.APIFunc)
		case "PATCH":
			group.PATCH(route.Pattern, route.APIFunc)
		case "DELETE":
			group.DELETE(route.Pattern, route.APIFunc)
		}
	}
}

type nwdafApp interface {
	app.App
	CancelContext() context.Context
	Processor() *processor.Processor
}

type processorAPI interface {
	HandleCreateSubscription(
		ctx context.Context,
		req *models.NnwdafEventsSubscription,
	) (*models.NnwdafEventsSubscription, string, *models.ProblemDetails)
	HandleUpdateSubscription(
		ctx context.Context,
		subscriptionID string,
		req *models.NnwdafEventsSubscription,
	) (*models.NnwdafEventsSubscription, *models.ProblemDetails)
	HandleDeleteSubscription(subscriptionID string) *models.ProblemDetails
	HandleAdrfRetrievalNotify(notifCorrID string, fetchCorrIDs []string, terminationReq bool)
}

type Server struct {
	nwdafApp

	httpServer *http.Server
	router     *gin.Engine
	processor  processorAPI
}

const (
	sbiStartupReadyTimeout  = 2 * time.Second
	sbiStartupProbeInterval = 50 * time.Millisecond
	sbiStartupProbeTimeout  = 100 * time.Millisecond
)

func NewServer(nwdaf nwdafApp, tlsKeyLogPath string) (*Server, error) {
	gin.SetMode(gin.ReleaseMode)

	s := &Server{
		nwdafApp:  nwdaf,
		router:    logger_util.NewGinWithLogrus(logger.GinLog),
		processor: nwdaf.Processor(),
	}

	s.router.Use(metrics.InboundMetrics())

	// EventsSubscription routes
	eventsSubRoutes := s.getEventsSubscriptionRoutes()
	eventsSubGroup := s.router.Group(factory.NwdafEventsSubResUriPrefix)
	eventsSubAuthorization := newRouterAuthorizationCheck(
		models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION,
	)
	eventsSubGroup.Use(func(c *gin.Context) {
		eventsSubAuthorization.Check(c, s.Context())
	})
	applyRoutes(eventsSubGroup, eventsSubRoutes)

	// Collector routes (for SMF callbacks)
	collectorRoutes := s.getCollectorRoutes()
	collectorGroup := s.router.Group("/collector")
	applyRoutes(collectorGroup, collectorRoutes)

	cfg := nwdaf.Config()
	bindAddr := cfg.GetSbiBindingAddr()

	logger.SBILog.Infof("Binding addr: [%s]", bindAddr)

	var err error
	if s.httpServer, err = httpwrapper.NewHttp2Server(bindAddr, tlsKeyLogPath, s.router); err != nil {
		logger.InitLog.Errorf("Initialize HTTP server failed: %v", err)
		return nil, err
	}
	s.httpServer.ErrorLog = log.New(logger.SBILog.WriterLevel(logrus.ErrorLevel), "HTTP2: ", 0)

	return s, nil
}

func (s *Server) getEventsSubscriptionRoutes() []Route {
	return []Route{
		{
			Name:    "CreateNWDAFEventsSubscription",
			Method:  "POST",
			Pattern: "/subscriptions",
			APIFunc: s.HandleCreateSubscription,
		},
		{
			Name:    "UpdateNWDAFEventsSubscription",
			Method:  "PUT",
			Pattern: "/subscriptions/:subscriptionId",
			APIFunc: s.HandleUpdateSubscription,
		},
		{
			Name:    "DeleteNWDAFEventsSubscription",
			Method:  "DELETE",
			Pattern: "/subscriptions/:subscriptionId",
			APIFunc: s.HandleDeleteSubscription,
		},
	}
}

func (s *Server) Processor() processorAPI {
	return s.processor
}

func (s *Server) Run(wg *sync.WaitGroup) error {
	scheme := s.Config().GetSbiScheme()
	switch scheme {
	case "http":
	case "https":
		if s.Config().GetCertPemPath() == "" || s.Config().GetCertKeyPath() == "" {
			return fmt.Errorf("SBI TLS config is required for https scheme")
		}
	default:
		return fmt.Errorf("unsupported SBI scheme: %s", scheme)
	}

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", s.httpServer.Addr)
	if err != nil {
		return err
	}
	if closeErr := listener.Close(); closeErr != nil {
		return fmt.Errorf("close SBI preflight listener: %w", closeErr)
	}

	readyCh := make(chan struct{}, 1)
	serveErrCh := make(chan error, 1)
	s.installStartupReadySignal(readyCh)

	wg.Add(1)
	go s.startServer(wg, serveErrCh)

	// The readiness check intentionally proves that the listener is accepting
	// connections before Run() reports success. In https mode this is a
	// listener-readiness guarantee, not a full TLS client handshake proof.
	if waitErr := s.waitUntilServing(readyCh, serveErrCh); waitErr != nil {
		s.Shutdown()
		return waitErr
	}

	return nil
}

func (s *Server) Shutdown() {
	const defaultShutdownTimeout = 2 * time.Second

	if s.httpServer != nil {
		logger.SBILog.Infof("Stop SBI server (listen on %s)", s.httpServer.Addr)
		toCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer cancel()
		if err := s.httpServer.Shutdown(toCtx); err != nil {
			logger.SBILog.Errorf("Could not close SBI server: %#v", err)
		}
	}
}

func (s *Server) startServer(wg *sync.WaitGroup, serveErrCh chan<- error) {
	defer func() {
		if p := recover(); p != nil {
			logger.SBILog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}
		wg.Done()
	}()

	logger.SBILog.Infof("Start SBI server (listen on %s)", s.httpServer.Addr)

	cfg := s.Config()
	scheme := cfg.GetSbiScheme()

	var err error
	switch scheme {
	case "http":
		err = s.httpServer.ListenAndServe()
	case "https":
		err = s.httpServer.ListenAndServeTLS(cfg.GetCertPemPath(), cfg.GetCertKeyPath())
	default:
		err = fmt.Errorf("unsupported SBI scheme: %s", scheme)
	}

	if err != nil && err != http.ErrServerClosed {
		select {
		case serveErrCh <- err:
		default:
		}
		logger.SBILog.Errorf("SBI server error: %v", err)
	}
	logger.SBILog.Infof("SBI server (listen on %s) stopped", s.httpServer.Addr)
}

func (s *Server) installStartupReadySignal(readyCh chan<- struct{}) {
	existingConnState := s.httpServer.ConnState

	s.httpServer.ConnState = func(conn net.Conn, state http.ConnState) {
		if existingConnState != nil {
			existingConnState(conn, state)
		}

		if state == http.StateNew {
			select {
			case readyCh <- struct{}{}:
			default:
			}
		}
	}
}

func (s *Server) waitUntilServing(readyCh <-chan struct{}, serveErrCh <-chan error) error {
	deadline := time.Now().Add(sbiStartupReadyTimeout)

	for {
		select {
		case <-readyCh:
			return nil
		case serveErr := <-serveErrCh:
			return fmt.Errorf("start SBI server: %w", serveErr)
		default:
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("start SBI server: readiness timeout after %s", sbiStartupReadyTimeout)
		}

		// Probe only the TCP accept path. This keeps startup cleanup semantics
		// simple and matches the current listener-readiness contract used by Run().
		probeCtx, cancel := context.WithTimeout(context.Background(), sbiStartupProbeTimeout)
		conn, probeErr := (&net.Dialer{Timeout: sbiStartupProbeTimeout}).DialContext(
			probeCtx,
			"tcp",
			s.httpServer.Addr,
		)
		cancel()
		if probeErr == nil {
			if closeErr := conn.Close(); closeErr != nil {
				return fmt.Errorf("close SBI startup probe connection: %w", closeErr)
			}
		}

		select {
		case <-readyCh:
			return nil
		case serveErr := <-serveErrCh:
			return fmt.Errorf("start SBI server: %w", serveErr)
		case <-time.After(sbiStartupProbeInterval):
		}
	}
}
