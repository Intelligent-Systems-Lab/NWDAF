package mtlf

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/openapi/models"
)

const validAdrfMLModelRecord = `{
	"nfInstanceId":"11111111-1111-4111-8111-111111111111",
	"mlModelInfo":[{
		"modelUniqueId":42,
		"mlFileAddr":{"mLModelUrl":"http://root.example/models/round-a"},
		"mlStorageSize":128,
		"allowConsumerList":[{"nfInstanceId":"22222222-2222-4222-8222-222222222222"}]
	}]
}`

const testAdrfMLModelTarget = "http://adrf.example"

type adrfMLModelContractStub struct {
	target       string
	storeTransID string
	body         []byte
	response     *consumer.StandardAdrfResponse
	err          error
}

func (s *adrfMLModelContractStub) StoreAdrfMLModelRecord(
	context.Context,
	string,
	[]byte,
) (*consumer.StandardAdrfResponse, error) {
	return s.response, s.err
}

func (s *adrfMLModelContractStub) RetrieveAdrfMLModelRecord(
	context.Context,
	string,
	string,
	[]int64,
) (*consumer.StandardAdrfResponse, error) {
	return s.response, s.err
}

func (s *adrfMLModelContractStub) UpdateAdrfMLModelRecord(
	_ context.Context,
	target string,
	storeTransID string,
	body []byte,
) (*consumer.StandardAdrfResponse, error) {
	s.target = target
	s.storeTransID = storeTransID
	s.body = append([]byte(nil), body...)
	return s.response, s.err
}

func (s *adrfMLModelContractStub) DeleteAdrfMLModelRecord(
	_ context.Context,
	target string,
	storeTransID string,
) (*consumer.StandardAdrfResponse, error) {
	s.target = target
	s.storeTransID = storeTransID
	return s.response, s.err
}

func TestMTLFAdrfMLModelRecordMutationRoutesForwardContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	stub := &adrfMLModelContractStub{response: &consumer.StandardAdrfResponse{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Body:        []byte(validAdrfMLModelRecord),
	}}
	server := &Server{processor: stub}
	router := gin.New()
	applyRoutes(router.Group(""), server.adrfMLModelRoutes())

	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPut,
		"/internal/v1/adrf-mlmodelmanagement/mlmodel-store-records/round-record-a",
		strings.NewReader(validAdrfMLModelRecord),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Target-Api-Root", testAdrfMLModelTarget)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || stub.target != testAdrfMLModelTarget ||
		stub.storeTransID != "round-record-a" || string(stub.body) != validAdrfMLModelRecord {
		t.Fatalf(
			"status=%d target=%q storeTransId=%q forwarded=%s response=%s",
			recorder.Code,
			stub.target,
			stub.storeTransID,
			string(stub.body),
			recorder.Body.String(),
		)
	}

	stub.response = &consumer.StandardAdrfResponse{StatusCode: http.StatusNoContent}
	request = httptest.NewRequestWithContext(
		context.Background(),
		http.MethodDelete,
		"/internal/v1/adrf-mlmodelmanagement/mlmodel-store-records/round-record-a",
		nil,
	)
	request.Header.Set("Target-Api-Root", testAdrfMLModelTarget)
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent || stub.target != testAdrfMLModelTarget ||
		stub.storeTransID != "round-record-a" {
		t.Fatalf(
			"status=%d target=%q storeTransId=%q response=%s",
			recorder.Code,
			stub.target,
			stub.storeTransID,
			recorder.Body.String(),
		)
	}
}

func TestMTLFAdrfMLModelRecordMutationRoutesRejectInvalidInput(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server := &Server{processor: &adrfMLModelContractStub{}}
	router := gin.New()
	applyRoutes(router.Group(""), server.adrfMLModelRoutes())

	tests := []struct {
		name        string
		method      string
		path        string
		body        string
		contentType string
		target      string
	}{
		{
			name:        "missing update body contract",
			method:      http.MethodPut,
			path:        "/internal/v1/adrf-mlmodelmanagement/mlmodel-store-records/round-record-a",
			body:        `{}`,
			contentType: "application/json",
			target:      testAdrfMLModelTarget,
		},
		{
			name:        "blank transaction id",
			method:      http.MethodPut,
			path:        "/internal/v1/adrf-mlmodelmanagement/mlmodel-store-records/%20",
			body:        validAdrfMLModelRecord,
			contentType: "application/json",
			target:      testAdrfMLModelTarget,
		},
		{
			name:   "invalid target",
			method: http.MethodDelete,
			path:   "/internal/v1/adrf-mlmodelmanagement/mlmodel-store-records/round-record-a",
			target: "http://adrf.example/unexpected",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequestWithContext(
				context.Background(),
				test.method,
				test.path,
				strings.NewReader(test.body),
			)
			if test.contentType != "" {
				request.Header.Set("Content-Type", test.contentType)
			}
			request.Header.Set("Target-Api-Root", test.target)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestMTLFAdrfMLModelRecordMutationRoutesPreserveProblemDetails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	stub := &adrfMLModelContractStub{err: &consumer.StandardAdrfError{
		StatusCode: http.StatusNotFound,
		ProblemDetails: models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "RESOURCE_NOT_FOUND",
		},
	}}
	server := &Server{processor: stub}
	router := gin.New()
	applyRoutes(router.Group(""), server.adrfMLModelRoutes())
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodDelete,
		"/internal/v1/adrf-mlmodelmanagement/mlmodel-store-records/missing-record",
		nil,
	)
	request.Header.Set("Target-Api-Root", testAdrfMLModelTarget)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	var problem models.ProblemDetails
	if err := json.Unmarshal(recorder.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode ProblemDetails: %v body=%s", err, recorder.Body.String())
	}
	if recorder.Code != http.StatusNotFound || problem.Cause != "RESOURCE_NOT_FOUND" {
		t.Fatalf("status=%d problem=%+v", recorder.Code, problem)
	}
}
