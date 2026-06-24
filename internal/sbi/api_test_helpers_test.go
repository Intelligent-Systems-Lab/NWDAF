package sbi

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/mock/gomock"

	"github.com/free5gc/nwdaf/internal/sbi/processor"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/nwdaf/pkg/mockapp"
	"github.com/free5gc/openapi/models"
)

const malformedRequestSyntaxTitle = "Malformed request syntax"

type handlerTestApp struct {
	*mockapp.MockApp
}

func (a handlerTestApp) Processor() *processor.Processor {
	return nil
}

func newHandlerTestServer(t *testing.T, proc processorAPI) *Server {
	t.Helper()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockApp := mockapp.NewMockApp(ctrl)
	mockApp.EXPECT().Config().Return(&factory.Config{
		Configuration: &factory.Configuration{
			Sbi: &factory.Sbi{
				Scheme:       "http",
				RegisterIPv4: "127.0.0.1",
				Port:         8080,
			},
		},
	}).AnyTimes()

	return &Server{
		nwdafApp:  handlerTestApp{MockApp: mockApp},
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
