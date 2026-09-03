package sbi

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/backend"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

type apiMLModelCallbackProcessorStub struct {
	*apiMLModelProcessorStub
	body     []byte
	resource string
	response *backend.StandardResponse
}

func (s *apiMLModelCallbackProcessorStub) HandleMLModelProvisionNotification(
	context.Context, string, []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return s.response, nil
}

func (s *apiMLModelCallbackProcessorStub) HandleMLModelMonitorNotification(
	context.Context, string, []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	return s.response, nil
}

func (s *apiMLModelCallbackProcessorStub) HandleMLModelTrainingNotification(
	_ context.Context,
	resource string,
	body []byte,
) (*backend.StandardResponse, *models.ProblemDetails) {
	s.resource = resource
	s.body = append([]byte(nil), body...)
	return s.response, nil
}

func TestMLModelTrainingCallbackPreservesCandidatePathAndValidBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	invalid := `{
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
	stub := &apiMLModelCallbackProcessorStub{}
	recorder, ginContext := newTrainingHandlerContext(
		t, http.MethodPost, "/ml-model-training/route-1", "application/json", invalid,
	)
	ginContext.Params = gin.Params{{Key: "localRouteId", Value: "route-1"}}
	(&Server{processor: stub}).HandleMLModelTrainingCallback(ginContext)
	ginContext.Writer.WriteHeaderNow()

	if recorder.Code != http.StatusBadRequest ||
		recorder.Header().Get("Content-Type") != util.ProblemJSONContentType || len(stub.body) != 0 {
		t.Fatalf("status=%d forwarded=%s body=%s", recorder.Code, stub.body, recorder.Body.String())
	}
	var problem models.ProblemDetails
	if err := json.Unmarshal(recorder.Body.Bytes(), &problem); err != nil {
		t.Fatal(err)
	}
	if len(problem.InvalidParams) != 1 ||
		problem.InvalidParams[0].Param != "x-flTopologyReport.children[0].statusCause" {
		t.Fatalf("problem = %+v", problem)
	}

	valid := `{
		"notifCorreId":"candidate-client-a",
		"mlCorreId":"hierarchical-fl-001",
		"x-flTopologyReport":{
			"nfInstanceId":"10000000-0000-4000-8000-000000000001"
		}
	}`
	stub.response = &backend.StandardResponse{StatusCode: http.StatusNoContent}
	recorder, ginContext = newTrainingHandlerContext(
		t, http.MethodPost, "/ml-model-training/route-1", "application/json", valid,
	)
	ginContext.Params = gin.Params{{Key: "localRouteId", Value: "route-1"}}
	(&Server{processor: stub}).HandleMLModelTrainingCallback(ginContext)
	ginContext.Writer.WriteHeaderNow()
	if recorder.Code != http.StatusNoContent || string(stub.body) != valid || stub.resource != "route-1" {
		t.Fatalf("status=%d resource=%q forwarded=%s", recorder.Code, stub.resource, stub.body)
	}
}
