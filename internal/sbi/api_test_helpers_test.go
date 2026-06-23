package sbi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/sbi/processor"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

const malformedRequestSyntaxTitle = "Malformed request syntax"

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

func decodeProblemDetailsResponse(t *testing.T, recorder *httptest.ResponseRecorder) models.ProblemDetails {
	t.Helper()

	if contentType := recorder.Header().Get("Content-Type"); contentType != util.ProblemJSONContentType {
		t.Fatalf("Content-Type = %q, want %q", contentType, util.ProblemJSONContentType)
	}

	var problem models.ProblemDetails
	if err := json.Unmarshal(recorder.Body.Bytes(), &problem); err != nil {
		t.Fatalf("failed to decode problem details: %v", err)
	}

	return problem
}
