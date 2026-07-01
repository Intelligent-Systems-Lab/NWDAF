package mtlf

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
)

type Server struct {
	httpServer *http.Server
	listener   net.Listener
}

func NewServer(cfg *factory.Config, service *MtlfService) (*Server, error) {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(gin.LoggerWithConfig(gin.LoggerConfig{
		Output: logger.GinLog.WriterLevel(logrus.DebugLevel),
	}))
	router.POST("/mtlf/training-complete", service.HandleDaisyTrainingComplete)

	httpServer := &http.Server{
		Addr:    cfg.GetMtlfServerBindingAddr(),
		Handler: router,
	}
	httpServer.ErrorLog = log.New(mtlfLog.WriterLevel(logrus.ErrorLevel), "HTTP: ", 0)

	return &Server{httpServer: httpServer}, nil
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
