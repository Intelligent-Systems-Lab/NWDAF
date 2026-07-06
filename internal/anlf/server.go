package anlf

import (
	"context"
	"log"
	"net"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
	"github.com/free5gc/util/httpwrapper"
	logger_util "github.com/free5gc/util/logger"
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
		case http.MethodGet:
			group.GET(route.Pattern, route.APIFunc)
		case http.MethodPost:
			group.POST(route.Pattern, route.APIFunc)
		case http.MethodPut:
			group.PUT(route.Pattern, route.APIFunc)
		case http.MethodPatch:
			group.PATCH(route.Pattern, route.APIFunc)
		case http.MethodDelete:
			group.DELETE(route.Pattern, route.APIFunc)
		}
	}
}

type processorAPI interface {
	HandleMlModelProvisionNotify(notifications []models.NwdafMlModelProvNotif)
}

type Server struct {
	httpServer *http.Server
	listener   net.Listener
	router     *gin.Engine
	processor  processorAPI
}

func NewServer(cfg *factory.Config, processor processorAPI) (*Server, error) {
	gin.SetMode(gin.ReleaseMode)

	s := &Server{
		router:    logger_util.NewGinWithLogrus(logger.GinLog),
		processor: processor,
	}
	s.router.Use(gin.Recovery())
	applyRoutes(s.router.Group(""), s.getRoutes())

	httpServer, err := httpwrapper.NewHttp2Server(
		cfg.GetAnlfServerBindingAddr(),
		"",
		s.router,
	)
	if err != nil {
		return nil, err
	}
	httpServer.ErrorLog = log.New(anlfLog.WriterLevel(logrus.ErrorLevel), "HTTP2: ", 0)
	s.httpServer = httpServer

	return s, nil
}

func (s *Server) getRoutes() []Route {
	return []Route{
		{
			Name:    "HandleMlModelProvisionNotify",
			Method:  http.MethodPost,
			Pattern: "/mlmodel-notify",
			APIFunc: s.HandleMlModelProvisionNotify,
		},
	}
}

func (s *Server) Run(wg *sync.WaitGroup) error {
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", s.httpServer.Addr)
	if err != nil {
		return err
	}
	s.listener = listener

	wg.Add(1)
	go s.startServer(wg)

	return nil
}

func (s *Server) Shutdown() {
	const defaultShutdownTimeout = 2 * time.Second

	if s.httpServer == nil {
		return
	}

	anlfLog.Infof("Stop AnLF server (listen on %s)", s.httpServer.Addr)
	toCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer cancel()
	if err := s.httpServer.Shutdown(toCtx); err != nil {
		anlfLog.Errorf("Could not close AnLF server: %#v", err)
	}
}

func (s *Server) startServer(wg *sync.WaitGroup) {
	defer func() {
		if p := recover(); p != nil {
			anlfLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}
		wg.Done()
	}()

	anlfLog.Infof("Start AnLF server (listen on %s)", s.httpServer.Addr)

	err := s.httpServer.Serve(s.listener)
	if err != nil && err != http.ErrServerClosed {
		anlfLog.Errorf("AnLF server error: %v", err)
	}
	anlfLog.Infof("AnLF server (listen on %s) stopped", s.httpServer.Addr)
}
