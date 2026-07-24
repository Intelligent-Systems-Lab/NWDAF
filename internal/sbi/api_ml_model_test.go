package sbi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/backend"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

type apiMLModelProcessorStub struct {
	body     []byte
	resource string
	response *backend.StandardResponse
	problem  *models.ProblemDetails
}

func (*apiMLModelProcessorStub) HandleCreateSubscription(
	context.Context, *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, string, *models.ProblemDetails) {
	return nil, "", nil
}

func (*apiMLModelProcessorStub) HandleUpdateSubscription(
	context.Context, string, *models.NnwdafEventsSubscription,
) (*models.NnwdafEventsSubscription, *models.ProblemDetails) {
	return nil, nil
}

func (*apiMLModelProcessorStub) HandleDeleteSubscription(string) *models.ProblemDetails { return nil }
func (*apiMLModelProcessorStub) HandleAdrfRetrievalNotify(
	context.Context, []byte,
) (*backend.StandardResponse, error) {
	return &backend.StandardResponse{StatusCode: http.StatusNoContent}, nil
}

func (s *apiMLModelProcessorStub) HandleCreateMLModelProvision(
	_ context.Context, body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func (s *apiMLModelProcessorStub) HandleReplaceMLModelProvision(
	_ context.Context, resource string, body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.resource = resource
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func (s *apiMLModelProcessorStub) HandleDeleteMLModelProvision(
	_ context.Context, resource string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.resource = resource
	return s.response, s.problem
}

func (s *apiMLModelProcessorStub) HandleCreateMLModelMonitorRegistration(
	_ context.Context, body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func (s *apiMLModelProcessorStub) HandleDeleteMLModelMonitorRegistration(
	_ context.Context, resource string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.resource = resource
	return s.response, s.problem
}

func (s *apiMLModelProcessorStub) HandleCreateMLModelMonitorSubscription(
	_ context.Context, body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func (s *apiMLModelProcessorStub) HandleReplaceMLModelMonitorSubscription(
	_ context.Context, resource string, body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.resource = resource
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func (s *apiMLModelProcessorStub) HandleDeleteMLModelMonitorSubscription(
	_ context.Context, resource string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.resource = resource
	return s.response, s.problem
}

func TestMLModelRouteMatricesMatchRelease18Methods(t *testing.T) {
	t.Parallel()

	server := &Server{}
	assertRoutes := func(routes []Route, expected map[string]string) {
		t.Helper()
		if len(routes) != len(expected) {
			t.Fatalf("route count = %d, want %d", len(routes), len(expected))
		}
		for _, route := range routes {
			key := route.Method + " " + route.Pattern
			if _, found := expected[key]; !found {
				t.Fatalf("unexpected route %s", key)
			}
			delete(expected, key)
		}
	}
	assertRoutes(server.getMLModelProvisionRoutes(), map[string]string{
		"POST /subscriptions":                   "create",
		"PUT /subscriptions/:subscriptionId":    "replace",
		"DELETE /subscriptions/:subscriptionId": "delete",
	})
	assertRoutes(server.getMLModelMonitorRoutes(), map[string]string{
		"POST /registrations":                   "create registration",
		"DELETE /registrations/:registrationId": "delete registration",
		"POST /subscriptions":                   "create subscription",
		"PUT /subscriptions/:subscriptionId":    "replace subscription",
		"DELETE /subscriptions/:subscriptionId": "delete subscription",
	})
}

func TestMLModelCreateHandlerPreservesRawBodyAndResponse(t *testing.T) {
	ginMode := gin.Mode()
	gin.SetMode(gin.TestMode)
	t.Cleanup(func() { gin.SetMode(ginMode) })

	body := `{
		"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION","mLEventFilter":{}}],
		"notifUri":"http://consumer.example/callback",
		"futureField":{"release":18}
	}`
	stub := &apiMLModelProcessorStub{response: &backend.StandardResponse{
		StatusCode: http.StatusCreated,
		Location:   "http://nwdaf.example/nnwdaf-mlmodelprovision/v1/subscriptions/sub-1",
		Body:       []byte(body),
	}}
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Request = httptest.NewRequest(http.MethodPost, "/subscriptions", strings.NewReader(body))
	ginContext.Request.Header.Set("Content-Type", "application/json; charset=utf-8")
	(&Server{processor: stub}).HandleCreateMLModelProvision(ginContext)
	if recorder.Code != http.StatusCreated || recorder.Header().Get("Location") == "" ||
		!strings.Contains(recorder.Body.String(), "futureField") {
		t.Fatalf("status=%d Location=%q body=%s", recorder.Code, recorder.Header().Get("Location"), recorder.Body.String())
	}
	if string(stub.body) != body {
		t.Fatalf("forwarded body = %s", stub.body)
	}
}

func TestMLModelHandlersValidateMediaTypeSizeAndMandatoryFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	valid := `{"modelIds":[1],"notificationUri":"http://consumer.example/callback","notifCorrId":"corr-1"}`
	tests := []struct {
		name        string
		contentType string
		body        string
		wantStatus  int
	}{
		{name: "missing media type", body: valid, wantStatus: http.StatusUnsupportedMediaType},
		{name: "wrong media type", contentType: "text/plain", body: valid, wantStatus: http.StatusUnsupportedMediaType},
		{name: "malformed JSON", contentType: "application/json", body: `{`, wantStatus: http.StatusBadRequest},
		{
			name: "missing mandatory field", contentType: "application/json",
			body:       `{"modelIds":[1],"notificationUri":"http://consumer.example/callback"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "oversized", contentType: "application/json",
			body:       strings.Repeat(" ", backend.MaxStandardMLModelBodyBytes+1),
			wantStatus: http.StatusRequestEntityTooLarge,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ginContext, _ := gin.CreateTestContext(recorder)
			ginContext.Request = httptest.NewRequest(
				http.MethodPost, "/subscriptions", strings.NewReader(test.body),
			)
			if test.contentType != "" {
				ginContext.Request.Header.Set("Content-Type", test.contentType)
			}
			(&Server{processor: &apiMLModelProcessorStub{}}).HandleCreateMLModelMonitorSubscription(ginContext)
			ginContext.Writer.WriteHeaderNow()
			if recorder.Code != test.wantStatus {
				t.Fatalf("status=%d want=%d body=%s", recorder.Code, test.wantStatus, recorder.Body.String())
			}
			if recorder.Header().Get("Content-Type") != util.ProblemJSONContentType {
				t.Fatalf("Content-Type=%q", recorder.Header().Get("Content-Type"))
			}
		})
	}
}

func TestMLModelHandlerWritesProblemDetailsAndNoContent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Run("ProblemDetails", func(t *testing.T) {
		stub := &apiMLModelProcessorStub{problem: &models.ProblemDetails{
			Status: http.StatusServiceUnavailable,
			Title:  http.StatusText(http.StatusServiceUnavailable),
		}}
		recorder := httptest.NewRecorder()
		ginContext, _ := gin.CreateTestContext(recorder)
		ginContext.Params = gin.Params{{Key: "registrationId", Value: "reg-1"}}
		ginContext.Request = httptest.NewRequest(http.MethodDelete, "/registrations/reg-1", nil)
		(&Server{processor: stub}).HandleDeleteMLModelMonitorRegistration(ginContext)
		if recorder.Code != http.StatusServiceUnavailable ||
			recorder.Header().Get("Content-Type") != util.ProblemJSONContentType {
			t.Fatalf("status=%d Content-Type=%q", recorder.Code, recorder.Header().Get("Content-Type"))
		}
	})
	t.Run("No Content", func(t *testing.T) {
		stub := &apiMLModelProcessorStub{response: &backend.StandardResponse{StatusCode: http.StatusNoContent}}
		recorder := httptest.NewRecorder()
		ginContext, _ := gin.CreateTestContext(recorder)
		ginContext.Params = gin.Params{{Key: "subscriptionId", Value: "sub-1"}}
		ginContext.Request = httptest.NewRequest(http.MethodDelete, "/subscriptions/sub-1", nil)
		(&Server{processor: stub}).HandleDeleteMLModelProvision(ginContext)
		ginContext.Writer.WriteHeaderNow()
		if recorder.Code != http.StatusNoContent || recorder.Body.Len() != 0 || stub.resource != "sub-1" {
			t.Fatalf("status=%d body=%s resource=%q", recorder.Code, recorder.Body.String(), stub.resource)
		}
	})
}
