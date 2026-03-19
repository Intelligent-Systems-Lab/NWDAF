package sbi

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/processor"
	"github.com/free5gc/nwdaf/pkg/factory"
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
	Config() *factory.Config
	Context() *nwdaf_context.NWDAFContext
	Processor() *processor.Processor
	CancelContext() context.Context
}

type Server struct {
	nwdafApp

	httpServer *http.Server
	router     *gin.Engine
}

func NewServer(nwdaf nwdafApp) (*Server, error) {
	gin.SetMode(gin.ReleaseMode)

	s := &Server{
		nwdafApp: nwdaf,
		router:   gin.New(),
	}

	// Setup middleware
	s.router.Use(gin.Recovery())
	s.router.Use(gin.LoggerWithConfig(gin.LoggerConfig{
		Output: logger.GinLog.WriterLevel(logrus.DebugLevel),
	}))

	// EventsSubscription routes
	eventsSubRoutes := s.getEventsSubscriptionRoutes()
	eventsSubGroup := s.router.Group(factory.NwdafEventsSubResUriPrefix)
	applyRoutes(eventsSubGroup, eventsSubRoutes)

	// Collector routes (for SMF callbacks)
	collectorRoutes := s.getCollectorRoutes()
	collectorGroup := s.router.Group("/collector")
	applyRoutes(collectorGroup, collectorRoutes)

	// ML Model Provision callback routes (for MTLF notifications)
	mlModelRoutes := s.getMlModelRoutes()
	mlModelGroup := s.router.Group("/mlmodel-notify")
	applyRoutes(mlModelGroup, mlModelRoutes)

	// Daisy async training callback routes
	daisyGroup := s.router.Group("/mtlf")
	applyRoutes(daisyGroup, s.getDaisyCallbackRoutes())

	cfg := nwdaf.Config()
	bindAddr := fmt.Sprintf("%s:%d",
		cfg.Configuration.Sbi.BindingIPv4,
		cfg.Configuration.Sbi.Port)

	logger.SBILog.Infof("Binding addr: [%s]", bindAddr)

	s.httpServer = &http.Server{
		Addr:    bindAddr,
		Handler: s.router,
	}
	s.httpServer.ErrorLog = log.New(logger.SBILog.WriterLevel(logrus.ErrorLevel), "HTTP: ", 0)

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

func (s *Server) Run(traceCtx context.Context, wg *sync.WaitGroup) error {
	wg.Add(1)
	go s.startServer(wg)

	return nil
}

func (s *Server) Shutdown(traceCtx context.Context) {
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

func (s *Server) startServer(wg *sync.WaitGroup) {
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
		// TLS support to be added later
		err = fmt.Errorf("HTTPS not yet supported")
	default:
		err = fmt.Errorf("unsupported scheme: %s", scheme)
	}

	if err != nil && err != http.ErrServerClosed {
		logger.SBILog.Errorf("SBI server error: %v", err)
	}
	logger.SBILog.Infof("SBI server (listen on %s) stopped", s.httpServer.Addr)
}
