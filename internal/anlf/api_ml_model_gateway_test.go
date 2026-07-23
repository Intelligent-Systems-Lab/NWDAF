package anlf

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

func (s *mlModelGatewayStub) HandleCreateMLModelProvisionFromBackend(
	_ context.Context,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func (s *mlModelGatewayStub) HandleReplaceMLModelProvisionFromBackend(
	_ context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.subscriptionID = subscriptionID
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func (s *mlModelGatewayStub) HandleDeleteMLModelProvisionFromBackend(
	_ context.Context,
	subscriptionID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.subscriptionID = subscriptionID
	return s.response, s.problem
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

func (s *mlModelGatewayStub) HandleCreateMLModelMonitorRegistrationFromBackend(
	_ context.Context,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func (s *mlModelGatewayStub) HandleDeleteMLModelMonitorRegistrationFromBackend(
	_ context.Context,
	registrationID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.subscriptionID = registrationID
	return s.response, s.problem
}

func (s *mlModelGatewayStub) HandleCreateMLModelMonitorSubscriptionFromBackend(
	_ context.Context,
	body []byte,
	ownerRegistrationID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.body = append([]byte(nil), body...)
	s.subscriptionID = ownerRegistrationID
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

func (s *mlModelGatewayStub) HandleMLModelMonitorNotification(
	_ context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.subscriptionID = subscriptionID
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func TestMLModelProvisionGatewayCreateAndNotificationContracts(t *testing.T) {
	gin.SetMode(gin.TestMode)
	createBody := `{
		"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION","mLEventFilter":{}}],
		"notifUri":"http://anlf.backend/provision"
	}`
	create := &mlModelGatewayStub{response: &backend.StandardResponse{
		StatusCode: http.StatusCreated,
		Location: "http://go.internal/nnwdaf-mlmodelprovision/v1/subscriptions/" +
			"11111111-1111-4111-8111-111111111111",
		ContentType: "application/json",
		Body:        []byte(createBody),
	}}
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Request = httptest.NewRequest(
		http.MethodPost,
		"/internal/v1/ml-model-provision/subscriptions",
		strings.NewReader(createBody),
	)
	ginContext.Request.Header.Set("Content-Type", "application/json")
	(&Server{mlModel: create}).HandleCreateMLModelProvisionFromBackend(ginContext)
	ginContext.Writer.WriteHeaderNow()
	if recorder.Code != http.StatusCreated ||
		!strings.Contains(recorder.Header().Get("Location"), "11111111-") {
		t.Fatalf(
			"create status=%d Location=%q body=%s",
			recorder.Code,
			recorder.Header().Get("Location"),
			recorder.Body.String(),
		)
	}

	notificationBody := `[{
		"subscriptionId":"11111111-1111-4111-8111-111111111111",
		"eventNotifs":[{
			"event":"UE_COMMUNICATION",
			"mLFileAddr":{"mLModelUrl":"http://mtlf.example/artifact"}
		}]
	}]`
	notification := &mlModelGatewayStub{response: &backend.StandardResponse{
		StatusCode: http.StatusNoContent,
	}}
	recorder = httptest.NewRecorder()
	ginContext, _ = gin.CreateTestContext(recorder)
	ginContext.Params = gin.Params{{
		Key: "subscriptionId", Value: "11111111-1111-4111-8111-111111111111",
	}}
	ginContext.Request = httptest.NewRequest(
		http.MethodPost,
		"/internal/v1/ml-model-provision/subscriptions/11111111-1111-4111-8111-111111111111/notifications",
		strings.NewReader(notificationBody),
	)
	ginContext.Request.Header.Set("Content-Type", "application/json")
	(&Server{mlModel: notification}).HandleMLModelProvisionNotification(ginContext)
	ginContext.Writer.WriteHeaderNow()
	if recorder.Code != http.StatusNoContent ||
		notification.subscriptionID != "11111111-1111-4111-8111-111111111111" {
		t.Fatalf(
			"notify status=%d subscriptionID=%s body=%s",
			recorder.Code,
			notification.subscriptionID,
			recorder.Body.String(),
		)
	}
}

func TestMLModelProvisionGatewayRejectsMalformedStandardBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Request = httptest.NewRequest(
		http.MethodPost,
		"/internal/v1/ml-model-provision/subscriptions",
		strings.NewReader(`{"notifUri":"http://anlf.backend/provision"}`),
	)
	ginContext.Request.Header.Set("Content-Type", "application/json")
	(&Server{mlModel: &mlModelGatewayStub{}}).HandleCreateMLModelProvisionFromBackend(ginContext)
	ginContext.Writer.WriteHeaderNow()
	if recorder.Code != http.StatusBadRequest ||
		!strings.HasPrefix(
			recorder.Header().Get("Content-Type"),
			"application/problem+json",
		) {
		t.Fatalf(
			"status=%d Content-Type=%q body=%s",
			recorder.Code,
			recorder.Header().Get("Content-Type"),
			recorder.Body.String(),
		)
	}
}

func TestMLModelMonitorGatewayRequiresAndForwardsOwnerRegistration(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := `{
		"modelIds":[1],
		"notificationUri":"http://mtlf.backend/monitor",
		"notifCorrId":"monitor-correlation"
	}`

	t.Run("missing owner", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ginContext, _ := gin.CreateTestContext(recorder)
		ginContext.Request = httptest.NewRequest(
			http.MethodPost,
			"/internal/v1/ml-model-monitor/subscriptions",
			strings.NewReader(body),
		)
		ginContext.Request.Header.Set("Content-Type", "application/json")

		(&Server{mlModel: &mlModelGatewayStub{}}).
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
		ginContext.Request = httptest.NewRequest(
			http.MethodPost,
			"/internal/v1/ml-model-monitor/subscriptions",
			strings.NewReader(body),
		)
		ginContext.Request.Header.Set("Content-Type", "application/json")
		ginContext.Request.Header.Set(
			backend.MonitorRegistrationIDHeader,
			"registration-1",
		)

		(&Server{mlModel: stub}).
			HandleCreateMLModelMonitorSubscriptionFromBackend(ginContext)
		ginContext.Writer.WriteHeaderNow()

		if recorder.Code != http.StatusCreated ||
			stub.subscriptionID != "registration-1" {
			t.Fatalf(
				"status=%d owner=%q body=%s",
				recorder.Code,
				stub.subscriptionID,
				recorder.Body.String(),
			)
		}
	})
}
