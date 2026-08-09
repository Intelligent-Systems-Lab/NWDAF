package anlf

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

const (
	testDescriptorID = "22222222-2222-4222-8222-222222222222"
	testDescriptor   = `{
  "correlationId":"22222222-2222-4222-8222-222222222222",
  "state":"ACTIVE",
  "storedDataSpec":{
    "dataSpec":{"smfDataSub":{
      "supi":"imsi-001010000000001",
      "notifId":"22222222-2222-4222-8222-222222222222",
      "notifUri":"http://anlf.example/callback",
      "eventSubs":[{"event":"UPF_EVENT"}]
    }},
    "timePeriod":{"startTime":"2026-08-10T01:00:00Z","stopTime":"2026-08-10T02:00:00Z"}
  },
  "mlEventSubscription":{"mLEvent":"UE_COMMUNICATION"},
  "sourceNfInstanceId":"11111111-1111-4111-8111-111111111111",
  "adrfInstanceId":"33333333-3333-4333-8333-333333333333",
  "retainUntil":"2026-08-11T02:00:00Z"
}`
)

type trainingDataDescriptorProcessorStub struct {
	putID    string
	putBody  []byte
	deleteID string
	err      error
}

func (s *trainingDataDescriptorProcessorStub) PutTrainingDataDescriptor(
	_ context.Context,
	id string,
	body []byte,
) error {
	s.putID = id
	s.putBody = append([]byte(nil), body...)
	return s.err
}

func (s *trainingDataDescriptorProcessorStub) DeleteTrainingDataDescriptor(
	_ context.Context,
	id string,
) error {
	s.deleteID = id
	return s.err
}

func TestTrainingDataDescriptorPutAndDeleteRelay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	processor := &trainingDataDescriptorProcessorStub{}
	server := &Server{processor: processor}
	router := gin.New()
	applyRoutes(router.Group(""), server.trainingDataDescriptorRoutes())

	put := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPut,
		"/internal/v1/anlf/training-data-descriptors/"+testDescriptorID,
		strings.NewReader(testDescriptor),
	)
	put.Header.Set("Content-Type", "application/json")
	putRecorder := httptest.NewRecorder()
	router.ServeHTTP(putRecorder, put)
	if putRecorder.Code != http.StatusNoContent {
		t.Fatalf("PUT status = %d, body=%s", putRecorder.Code, putRecorder.Body.String())
	}
	if processor.putID != testDescriptorID || len(processor.putBody) == 0 {
		t.Fatalf("PUT relay = id=%q body=%q", processor.putID, processor.putBody)
	}

	deleteRequest := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodDelete,
		"/internal/v1/anlf/training-data-descriptors/"+testDescriptorID,
		nil,
	)
	deleteRecorder := httptest.NewRecorder()
	router.ServeHTTP(deleteRecorder, deleteRequest)
	if deleteRecorder.Code != http.StatusNoContent || processor.deleteID != testDescriptorID {
		t.Fatalf("DELETE status=%d relayed=%q", deleteRecorder.Code, processor.deleteID)
	}
}

func TestTrainingDataDescriptorRejectsMismatchedIdentifier(t *testing.T) {
	gin.SetMode(gin.TestMode)
	processor := &trainingDataDescriptorProcessorStub{}
	server := &Server{processor: processor}
	router := gin.New()
	applyRoutes(router.Group(""), server.trainingDataDescriptorRoutes())

	request := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPut,
		"/internal/v1/anlf/training-data-descriptors/44444444-4444-4444-8444-444444444444",
		strings.NewReader(testDescriptor),
	)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest || processor.putID != "" {
		t.Fatalf("status=%d relayed=%q body=%s", recorder.Code, processor.putID, recorder.Body.String())
	}
}

func TestTrainingDataDescriptorAcceptsMongoFallbackWithoutAdrfIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	processor := &trainingDataDescriptorProcessorStub{}
	server := &Server{processor: processor}
	router := gin.New()
	applyRoutes(router.Group(""), server.trainingDataDescriptorRoutes())
	payload := strings.Replace(
		testDescriptor,
		`,\n  "adrfInstanceId":"33333333-3333-4333-8333-333333333333"`,
		"",
		1,
	)

	request := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPut,
		"/internal/v1/anlf/training-data-descriptors/"+testDescriptorID,
		strings.NewReader(payload),
	)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNoContent || processor.putID != testDescriptorID {
		t.Fatalf("status=%d relayed=%q body=%s", recorder.Code, processor.putID, recorder.Body.String())
	}
}
