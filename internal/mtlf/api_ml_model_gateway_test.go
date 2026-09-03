package mtlf

import (
	"context"
	"encoding/json"
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

func (s *mlModelGatewayStub) HandleCreateMLModelTrainingFromBackend(
	_ context.Context,
	body []byte,
	_ *backend.SelectedTarget,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func (s *mlModelGatewayStub) HandleReplaceMLModelTrainingFromBackend(
	_ context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.subscriptionID = subscriptionID
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func (s *mlModelGatewayStub) HandlePatchMLModelTrainingFromBackend(
	_ context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.subscriptionID = subscriptionID
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func (s *mlModelGatewayStub) HandleDeleteMLModelTrainingFromBackend(
	_ context.Context,
	subscriptionID string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.subscriptionID = subscriptionID
	return s.response, s.problem
}

func (s *mlModelGatewayStub) HandleMLModelTrainingNotification(
	_ context.Context,
	subscriptionID string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.subscriptionID = subscriptionID
	s.body = append([]byte(nil), body...)
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

func TestMLModelTrainingGatewayPreservesCandidateBodyAndErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)
	valid := `{
		"mLEventSubscs":[{
			"mLEvent":"UE_COMMUNICATION",
			"mLEventFilter":{},
			"modelInterInfo":"bundle-v1"
		}],
		"notifUri":"http://backend.example/training-callback",
		"notifCorreId":"candidate-client-a",
		"suppFeats":"4",
		"mlCorreId":"hierarchical-fl-001",
		"mLPreFlag":true,
		"mLModelTrainInfos":[{
			"dataAvReq":{"inpEvents":[{"upfEvent":"USER_DATA_USAGE_TRENDS"}]},
			"timeAvReq":"PT5M"
		}],
		"x-flTopology":{
			"nfInstanceId":"10000000-0000-4000-8000-000000000001"
		}
	}`
	stub := &mlModelGatewayStub{response: &backend.StandardResponse{
		StatusCode:  http.StatusCreated,
		Location:    "http://go.internal/internal/v1/ml-model-training/subscriptions/sub-1",
		ContentType: "application/json",
		Body:        []byte(valid),
	}}
	recorder, ginContext := newMLModelTrainingGatewayContext(
		t, http.MethodPost, "application/json", valid,
	)
	(&Server{processor: stub}).HandleCreateMLModelTrainingFromBackend(ginContext)
	ginContext.Writer.WriteHeaderNow()
	if recorder.Code != http.StatusCreated || string(stub.body) != valid ||
		!strings.Contains(recorder.Body.String(), "x-flTopology") {
		t.Fatalf("status=%d forwarded=%s body=%s", recorder.Code, stub.body, recorder.Body.String())
	}

	invalid := `{"x-flTopology":{"policy":{"unknown":true}}}`
	stub.body = nil
	recorder, ginContext = newMLModelTrainingGatewayContext(
		t, http.MethodPatch, "application/merge-patch+json", invalid,
	)
	ginContext.Params = gin.Params{{Key: "subscriptionId", Value: "sub-1"}}
	(&Server{processor: stub}).HandlePatchMLModelTrainingFromBackend(ginContext)
	ginContext.Writer.WriteHeaderNow()
	if recorder.Code != http.StatusBadRequest || len(stub.body) != 0 {
		t.Fatalf("status=%d forwarded=%s body=%s", recorder.Code, stub.body, recorder.Body.String())
	}
	var problem models.ProblemDetails
	if err := json.Unmarshal(recorder.Body.Bytes(), &problem); err != nil {
		t.Fatal(err)
	}
	if len(problem.InvalidParams) != 1 ||
		problem.InvalidParams[0].Param != "x-flTopology.policy.unknown" {
		t.Fatalf("problem = %+v", problem)
	}

	invalidNotify := `{
		"notifCorreId":"candidate-client-a",
		"mlCorreId":"hierarchical-fl-001",
		"x-flTopologyReport":{
			"nfInstanceId":"10000000-0000-4000-8000-000000000001",
			"children":[{
				"nfInstanceId":"10000000-0000-4000-8000-000000000101",
				"status":"FAILED",
				"statusTimestamp":"2026-09-02T06:29:10Z"
			}]
		}
	}`
	stub.body = nil
	recorder, ginContext = newMLModelTrainingGatewayContext(
		t, http.MethodPost, "application/json", invalidNotify,
	)
	(&Server{processor: stub}).HandleMLModelTrainingNotification(ginContext)
	ginContext.Writer.WriteHeaderNow()
	if recorder.Code != http.StatusBadRequest || len(stub.body) != 0 {
		t.Fatalf("notify status=%d forwarded=%s body=%s", recorder.Code, stub.body, recorder.Body.String())
	}
	problem = models.ProblemDetails{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &problem); err != nil {
		t.Fatal(err)
	}
	if len(problem.InvalidParams) != 1 ||
		problem.InvalidParams[0].Param != "x-flTopologyReport.children[0].statusCause" {
		t.Fatalf("notify problem = %+v", problem)
	}
}

func newMLModelTrainingGatewayContext(
	t *testing.T,
	method string,
	mediaType string,
	body string,
) (*httptest.ResponseRecorder, *gin.Context) {
	t.Helper()
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Request = httptest.NewRequestWithContext(
		t.Context(), method, "/internal/v1/ml-model-training/subscriptions", strings.NewReader(body),
	)
	ginContext.Request.Header.Set("Content-Type", mediaType)
	return recorder, ginContext
}
