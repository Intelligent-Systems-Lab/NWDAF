package mtlf

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/backend"
	"github.com/free5gc/openapi/models"
)

type mlModelGatewayStub struct {
	body           []byte
	subscriptionID string
	response       *backend.StandardResponse
	problem        *models.ProblemDetails
}

func (s *mlModelGatewayStub) HandleMLModelProvisionNotification(
	_ context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.subscriptionID = subscriptionID
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func (s *mlModelGatewayStub) HandleCreateMLModelMonitorSubscriptionFromBackend(
	_ context.Context,
	body []byte,
	ownerRegistrationID string,
	_ *backend.SelectedTarget,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.subscriptionID = ownerRegistrationID
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func (s *mlModelGatewayStub) HandleReplaceMLModelMonitorSubscriptionFromBackend(
	_ context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.subscriptionID = subscriptionID
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func (s *mlModelGatewayStub) HandleDeleteMLModelMonitorSubscriptionFromBackend(
	_ context.Context,
	subscriptionID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.subscriptionID = subscriptionID
	return s.response, s.problem
}

func TestMLModelProvisionNotificationContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := `[{
		"subscriptionId":"11111111-1111-4111-8111-111111111111",
		"eventNotifs":[{
			"event":"UE_COMMUNICATION",
			"mLFileAddr":{"mLModelUrl":"http://mtlf.example/artifact"}
		}]
	}]`
	stub := &mlModelGatewayStub{response: &backend.StandardResponse{
		StatusCode: http.StatusNoContent,
	}}
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Params = gin.Params{{
		Key: "subscriptionId", Value: "11111111-1111-4111-8111-111111111111",
	}}
	ginContext.Request = httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"/internal/v1/ml-model-provision/subscriptions/11111111-1111-4111-8111-111111111111/notifications",
		strings.NewReader(body),
	)
	ginContext.Request.Header.Set("Content-Type", "application/json")

	(&Server{processor: stub}).HandleMLModelProvisionNotification(ginContext)
	ginContext.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusNoContent ||
		stub.subscriptionID != "11111111-1111-4111-8111-111111111111" ||
		string(stub.body) != body {
		t.Fatalf(
			"status=%d subscriptionID=%q body=%s",
			recorder.Code,
			stub.subscriptionID,
			recorder.Body.String(),
		)
	}
}

func TestMLModelMonitorSubscriptionRequiresAndForwardsOwner(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := `{
		"modelIds":[1],
		"notificationUri":"http://mtlf.backend/monitor",
		"notifCorrId":"monitor-correlation"
	}`

	t.Run("missing owner", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ginContext, _ := gin.CreateTestContext(recorder)
		ginContext.Request = httptest.NewRequestWithContext(
			t.Context(),
			http.MethodPost,
			"/internal/v1/ml-model-monitor/subscriptions",
			strings.NewReader(body),
		)
		ginContext.Request.Header.Set("Content-Type", "application/json")

		(&Server{processor: &mlModelGatewayStub{}}).
			HandleCreateMLModelMonitorSubscriptionFromBackend(ginContext)
		ginContext.Writer.WriteHeaderNow()

		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("owner forwarded", func(t *testing.T) {
		stub := &mlModelGatewayStub{response: &backend.StandardResponse{
			StatusCode:  http.StatusCreated,
			Location:    "http://go.internal/subscriptions/monitor-1",
			ContentType: "application/json",
			Body:        []byte(body),
		}}
		recorder := httptest.NewRecorder()
		ginContext, _ := gin.CreateTestContext(recorder)
		ginContext.Request = httptest.NewRequestWithContext(
			t.Context(),
			http.MethodPost,
			"/internal/v1/ml-model-monitor/subscriptions",
			strings.NewReader(body),
		)
		ginContext.Request.Header.Set("Content-Type", "application/json")
		ginContext.Request.Header.Set(backend.MonitorRegistrationIDHeader, "registration-1")

		(&Server{processor: stub}).
			HandleCreateMLModelMonitorSubscriptionFromBackend(ginContext)
		ginContext.Writer.WriteHeaderNow()

		if recorder.Code != http.StatusCreated ||
			stub.subscriptionID != "registration-1" ||
			recorder.Header().Get("Location") == "" {
			t.Fatalf(
				"status=%d owner=%q Location=%q body=%s",
				recorder.Code,
				stub.subscriptionID,
				recorder.Header().Get("Location"),
				recorder.Body.String(),
			)
		}
	})
}

func TestMTLFRouteOwnershipExcludesAnLFOriginatedOperations(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	server := &Server{router: router}
	routes := server.adrfRetrievalRoutes()
	routes = append(routes, server.nfDiscoveryRoutes()...)
	routes = append(routes, server.mtlfMLModelRoutes()...)
	applyRoutes(router.Group(""), routes)

	request := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"/internal/v1/ml-model-provision/subscriptions",
		strings.NewReader(`{}`),
	)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("MTLF edge accepted AnLF-originated operation: status=%d", recorder.Code)
	}
}
