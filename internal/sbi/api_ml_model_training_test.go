package sbi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/backend"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

type apiMLModelTrainingProcessorStub struct {
	*apiMLModelProcessorStub
	body     []byte
	resource string
	response *backend.StandardResponse
	problem  *models.ProblemDetails
}

func (s *apiMLModelTrainingProcessorStub) HandleCreateMLModelTraining(
	_ context.Context,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func (s *apiMLModelTrainingProcessorStub) HandleReplaceMLModelTraining(
	_ context.Context,
	resource string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.resource = resource
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func (s *apiMLModelTrainingProcessorStub) HandlePatchMLModelTraining(
	_ context.Context,
	resource string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.resource = resource
	s.body = append([]byte(nil), body...)
	return s.response, s.problem
}

func (s *apiMLModelTrainingProcessorStub) HandleDeleteMLModelTraining(
	_ context.Context,
	resource string,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.resource = resource
	return s.response, s.problem
}

func TestMLModelTrainingHandlerPreservesCandidateBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := apiCandidateTrainingBody()
	stub := &apiMLModelTrainingProcessorStub{response: &backend.StandardResponse{
		StatusCode:  http.StatusCreated,
		Location:    "http://nwdaf.example/nnwdaf-mlmodeltraining/v1/subscriptions/sub-1",
		ContentType: "application/json",
		Body:        []byte(body),
	}}
	recorder, ginContext := newTrainingHandlerContext(
		t, http.MethodPost, "/subscriptions", "application/json", body,
	)
	(&Server{processor: stub}).HandleCreateMLModelTraining(ginContext)

	if recorder.Code != http.StatusCreated || recorder.Header().Get("Location") == "" ||
		string(stub.body) != body || !strings.Contains(recorder.Body.String(), "flTopology") {
		t.Fatalf(
			"status=%d Location=%q forwarded=%s body=%s",
			recorder.Code, recorder.Header().Get("Location"), stub.body, recorder.Body.String(),
		)
	}
}

func TestMLModelTrainingHandlersPreserveCandidateInvalidPaths(t *testing.T) {
	gin.SetMode(gin.TestMode)
	invalidFull := strings.Replace(
		apiCandidateTrainingBody(),
		`"nfInstanceId":"10000000-0000-4000-8000-000000000001"`,
		`"nfInstanceId":"10000000-0000-4000-8000-000000000001","unknown":true`,
		1,
	)
	tests := []struct {
		name        string
		method      string
		mediaType   string
		body        string
		invalidPath string
		handle      func(*Server, *gin.Context)
	}{
		{
			name: "create", method: http.MethodPost, mediaType: "application/json",
			body: invalidFull, invalidPath: "flTopology.unknown",
			handle: func(server *Server, context *gin.Context) {
				server.HandleCreateMLModelTraining(context)
			},
		},
		{
			name: "replace", method: http.MethodPut, mediaType: "application/json",
			body: invalidFull, invalidPath: "flTopology.unknown",
			handle: func(server *Server, context *gin.Context) {
				server.HandleReplaceMLModelTraining(context)
			},
		},
		{
			name: "patch", method: http.MethodPatch, mediaType: "application/merge-patch+json",
			body:        `{"flTopology":{"policy":{"unknown":true}}}`,
			invalidPath: "flTopology.policy.unknown",
			handle: func(server *Server, context *gin.Context) {
				server.HandlePatchMLModelTraining(context)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stub := &apiMLModelTrainingProcessorStub{}
			recorder, ginContext := newTrainingHandlerContext(
				t, test.method, "/subscriptions/sub-1", test.mediaType, test.body,
			)
			ginContext.Params = gin.Params{{Key: "subscriptionId", Value: "sub-1"}}
			test.handle(&Server{processor: stub}, ginContext)
			ginContext.Writer.WriteHeaderNow()

			if recorder.Code != http.StatusBadRequest ||
				recorder.Header().Get("Content-Type") != util.ProblemJSONContentType ||
				len(stub.body) != 0 {
				t.Fatalf(
					"status=%d Content-Type=%q forwarded=%s body=%s",
					recorder.Code, recorder.Header().Get("Content-Type"), stub.body, recorder.Body.String(),
				)
			}
			var problem models.ProblemDetails
			if err := json.Unmarshal(recorder.Body.Bytes(), &problem); err != nil {
				t.Fatal(err)
			}
			if problem.Cause != "INVALID_MSG_FORMAT" || len(problem.InvalidParams) != 1 ||
				problem.InvalidParams[0].Param != test.invalidPath {
				t.Fatalf("problem = %+v", problem)
			}
		})
	}
}

func newTrainingHandlerContext(
	t *testing.T,
	method string,
	path string,
	mediaType string,
	body string,
) (*httptest.ResponseRecorder, *gin.Context) {
	t.Helper()
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Request = httptest.NewRequestWithContext(
		t.Context(), method, path, strings.NewReader(body),
	)
	ginContext.Request.Header.Set("Content-Type", mediaType)
	return recorder, ginContext
}

func apiCandidateTrainingBody() string {
	return `{
		"mLEventSubscs":[{
			"mLEvent":"UE_COMMUNICATION",
			"mLEventFilter":{},
			"modelInterInfo":"bundle-v1"
		}],
		"notifUri":"http://server.example/training-callback",
		"notifCorreId":"candidate-client-a",
		"suppFeats":"4",
		"mlCorreId":"hierarchical-fl-001",
		"mLPreFlag":true,
		"mLModelTrainInfos":[{
			"dataAvReq":{"inpEvents":[{"upfEvent":"USER_DATA_USAGE_TRENDS"}]},
			"timeAvReq":"PT5M"
		}],
		"flTopology":{
			"nfInstanceId":"10000000-0000-4000-8000-000000000001"
		}
	}`
}
