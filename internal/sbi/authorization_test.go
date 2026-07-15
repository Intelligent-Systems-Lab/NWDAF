package sbi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/mock/gomock"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/nwdaf/pkg/mockapp"
	"github.com/free5gc/openapi/models"
)

type authorizationContextStub struct {
	err         error
	token       string
	serviceName models.ServiceName
	calls       int
}

func (stub *authorizationContextStub) AuthorizationCheck(
	token string,
	serviceName models.ServiceName,
) error {
	stub.calls++
	stub.token = token
	stub.serviceName = serviceName
	return stub.err
}

func TestRouterAuthorizationCheck(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantAbort  bool
	}{
		{name: "authorized", wantStatus: http.StatusOK},
		{name: "unauthorized", err: errors.New("invalid token"), wantStatus: http.StatusUnauthorized, wantAbort: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
			c.Request.Header.Set("Authorization", "Bearer token-value")
			stub := &authorizationContextStub{err: tt.err}
			check := newRouterAuthorizationCheck(models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION)

			check.Check(c, stub)
			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.wantStatus)
			}
			if c.IsAborted() != tt.wantAbort {
				t.Fatalf("IsAborted() = %t, want %t", c.IsAborted(), tt.wantAbort)
			}
			if stub.token != "Bearer token-value" ||
				stub.serviceName != models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION {
				t.Fatalf("authorization input = (%q, %q)", stub.token, stub.serviceName)
			}
			if tt.wantAbort {
				problem := decodeProblemDetailsResponse(t, recorder)
				if problem.Status != http.StatusUnauthorized || problem.Cause != "" {
					t.Fatalf("ProblemDetails = %+v", problem)
				}
			}
		})
	}
}

func TestEventsSubscriptionRoutesUseAuthorizationGroupAndCollectorDoesNot(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	stub := &authorizationContextStub{err: errors.New("invalid token")}
	check := newRouterAuthorizationCheck(models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION)
	eventsGroup := router.Group(factory.NwdafEventsSubResUriPrefix)
	eventsGroup.Use(func(c *gin.Context) { check.Check(c, stub) })
	eventsGroup.POST("/subscriptions", func(c *gin.Context) { c.Status(http.StatusCreated) })
	eventsGroup.PUT("/subscriptions/:subscriptionId", func(c *gin.Context) { c.Status(http.StatusOK) })
	eventsGroup.DELETE("/subscriptions/:subscriptionId", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	collectorGroup := router.Group("/collector")
	collectorGroup.POST("/notify", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	protectedRequests := []struct {
		method string
		path   string
	}{
		{method: http.MethodPost, path: factory.NwdafEventsSubResUriPrefix + "/subscriptions"},
		{method: http.MethodPut, path: factory.NwdafEventsSubResUriPrefix + "/subscriptions/sub-1"},
		{method: http.MethodDelete, path: factory.NwdafEventsSubResUriPrefix + "/subscriptions/sub-1"},
	}
	for _, request := range protectedRequests {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(request.method, request.path, nil))
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s status = %d, want 401", request.method, request.path, recorder.Code)
		}
		if recorder.Header().Get("Content-Type") != util.ProblemJSONContentType {
			t.Fatalf("%s %s Content-Type = %q", request.method, request.path, recorder.Header().Get("Content-Type"))
		}
	}

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/collector/notify", nil))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("collector status = %d, want 204", recorder.Code)
	}
	if stub.calls != len(protectedRequests) {
		t.Fatalf("authorization calls = %d, want %d", stub.calls, len(protectedRequests))
	}
}

func TestEventsSubscriptionAuthorizationAllowsHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	stub := &authorizationContextStub{}
	check := newRouterAuthorizationCheck(models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION)
	eventsGroup := router.Group(factory.NwdafEventsSubResUriPrefix)
	eventsGroup.Use(func(c *gin.Context) { check.Check(c, stub) })
	eventsGroup.POST("/subscriptions", func(c *gin.Context) { c.Status(http.StatusCreated) })

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		factory.NwdafEventsSubResUriPrefix+"/subscriptions",
		nil,
	)
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", recorder.Code)
	}
	if stub.calls != 1 {
		t.Fatalf("authorization calls = %d, want 1", stub.calls)
	}
}

func TestNewServerWiresEventsSubscriptionAuthorizationOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	cfg := &factory.Config{Configuration: &factory.Configuration{
		Sbi: &factory.Sbi{
			Scheme:       "http",
			BindingIPv4:  "127.0.0.1",
			RegisterIPv4: "127.0.0.1",
			Port:         8080,
		},
	}}
	authorizationCtx := &nwdaf_context.NWDAFContext{}
	authorizationCtx.RecordOAuth2Required("http://nrf/nnrf-nfm/v1/nf-instances/nwdaf")
	mockApp := mockapp.NewMockApp(ctrl)
	mockApp.EXPECT().Config().Return(cfg).AnyTimes()
	mockApp.EXPECT().Context().Return(authorizationCtx).AnyTimes()

	server, err := NewServer(handlerTestApp{MockApp: mockApp}, "")
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	protectedRequests := []struct {
		method string
		path   string
	}{
		{method: http.MethodPost, path: factory.NwdafEventsSubResUriPrefix + "/subscriptions"},
		{method: http.MethodPut, path: factory.NwdafEventsSubResUriPrefix + "/subscriptions/sub-1"},
		{method: http.MethodDelete, path: factory.NwdafEventsSubResUriPrefix + "/subscriptions/sub-1"},
	}
	for _, request := range protectedRequests {
		recorder := httptest.NewRecorder()
		server.router.ServeHTTP(recorder, httptest.NewRequest(request.method, request.path, nil))
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s status = %d, want 401", request.method, request.path, recorder.Code)
		}
		if recorder.Header().Get("Content-Type") != util.ProblemJSONContentType {
			t.Fatalf(
				"%s %s Content-Type = %q, want %q",
				request.method,
				request.path,
				recorder.Header().Get("Content-Type"),
				util.ProblemJSONContentType,
			)
		}
	}

	collectorRequest := httptest.NewRequest(
		http.MethodPost,
		"/collector/notify",
		strings.NewReader("{"),
	)
	collectorRequest.Header.Set("Content-Type", "application/json")
	collectorRecorder := httptest.NewRecorder()
	server.router.ServeHTTP(collectorRecorder, collectorRequest)
	if collectorRecorder.Code != http.StatusBadRequest {
		t.Fatalf("collector status = %d, want 400 without producer authorization", collectorRecorder.Code)
	}
}
