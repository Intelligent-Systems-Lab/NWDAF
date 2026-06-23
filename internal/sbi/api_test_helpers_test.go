package sbi

import (
	"bytes"
	"context"
	"net/http/httptest"

	"github.com/gin-gonic/gin"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/sbi/processor"
	"github.com/free5gc/nwdaf/pkg/factory"
)

type handlerTestApp struct {
	cfg *factory.Config
}

func (a *handlerTestApp) Config() *factory.Config {
	return a.cfg
}

func (a *handlerTestApp) Context() *nwdaf_context.NWDAFContext {
	return nwdaf_context.GetSelf()
}

func (a *handlerTestApp) Processor() *processor.Processor {
	return nil
}

func (a *handlerTestApp) CancelContext() context.Context {
	return context.Background()
}

func newHandlerTestServer(proc processorAPI) *Server {
	return &Server{
		nwdafApp: &handlerTestApp{
			cfg: &factory.Config{
				Configuration: &factory.Configuration{
					Sbi: &factory.Sbi{
						Scheme:       "http",
						RegisterIPv4: "127.0.0.1",
						Port:         8080,
					},
				},
			},
		},
		processor: proc,
	}
}

func newJSONRequestContext(method, target string, body []byte) (*gin.Context, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, target, bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	return c, recorder
}
