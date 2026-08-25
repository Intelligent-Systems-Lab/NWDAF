package mtlf

import (
	"context"
	"log"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/util/httpwrapper"
	logger_util "github.com/free5gc/util/logger"
)

var mtlfLog = logger.MtlfLog

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

type Server struct {
	httpServer            *http.Server
	listener              net.Listener
	router                *gin.Engine
	processor             any
	publicCallbackBaseURI string
	internalAPIBaseURI    string
	processInstanceID     string
}

func NewServer(cfg *factory.Config, processor any, processInstanceID string) (*Server, error) {
	gin.SetMode(gin.ReleaseMode)

	s := &Server{
		router:                logger_util.NewGinWithLogrus(logger.GinLog),
		processor:             processor,
		publicCallbackBaseURI: strings.TrimRight(cfg.GetSbiUri(), "/"),
		internalAPIBaseURI:    strings.TrimRight(cfg.GetMtlfServerURI(), "/"),
		processInstanceID:     processInstanceID,
	}
	s.router.Use(gin.Recovery())
	routes := s.nwdafContextRoutes()
	routes = append(routes, s.adrfRetrievalRoutes()...)
	routes = append(routes, s.adrfMLModelRoutes()...)
	routes = append(routes, s.nfDiscoveryRoutes()...)
	routes = append(routes, s.udmCollectionRoutes()...)
	routes = append(routes, s.smfEventExposureRoutes()...)
	routes = append(routes, s.adrfStorageRoutes()...)
	routes = append(routes, s.mtlfMLModelRoutes()...)
	applyRoutes(s.router.Group(""), routes)

	httpServer, err := httpwrapper.NewHttp2Server(
		cfg.GetMtlfServerBindingAddr(),
		"",
		s.router,
	)
	if err != nil {
		return nil, err
	}
	httpServer.ErrorLog = log.New(mtlfLog.WriterLevel(logrus.ErrorLevel), "HTTP2: ", 0)
	s.httpServer = httpServer

	return s, nil
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

	mtlfLog.Infof("Stop MTLF server (listen on %s)", s.httpServer.Addr)
	toCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer cancel()
	if err := s.httpServer.Shutdown(toCtx); err != nil {
		mtlfLog.Errorf("Could not close MTLF server: %#v", err)
	}
}

func (s *Server) startServer(wg *sync.WaitGroup) {
	defer func() {
		if p := recover(); p != nil {
			mtlfLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}
		wg.Done()
	}()

	mtlfLog.Infof("Start MTLF server (listen on %s)", s.httpServer.Addr)

	err := s.httpServer.Serve(s.listener)
	if err != nil && err != http.ErrServerClosed {
		mtlfLog.Errorf("MTLF server error: %v", err)
	}
	mtlfLog.Infof("MTLF server (listen on %s) stopped", s.httpServer.Addr)
}
