package anlf

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/backend"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/openapi/models"
)

type nfDiscoveryProcessorStub struct {
	result *consumer.NFDiscoveryResult
	err    error
	calls  int
}

func (s *nfDiscoveryProcessorStub) HandleNFDiscovery(
	context.Context, backend.NFDiscoveryQuery,
) (*consumer.NFDiscoveryResult, error) {
	s.calls++
	return s.result, s.err
}

func TestHandleNFDiscoveryRequiresStandardQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)
	processor := &nfDiscoveryProcessorStub{
		result: &consumer.NFDiscoveryResult{
			SearchResult: models.SearchResult{ValidityPeriod: 10},
		},
	}
	server := &Server{processor: processor}

	invalidRecorder := httptest.NewRecorder()
	invalidContext, _ := gin.CreateTestContext(invalidRecorder)
	invalidContext.Request = httptest.NewRequest(
		http.MethodGet,
		"/internal/v1/nrf/nf-instances?target-nf-type=SMF",
		nil,
	)
	server.HandleNFDiscovery(invalidContext)
	if invalidRecorder.Code != http.StatusBadRequest || processor.calls != 0 {
		t.Fatalf("invalid status=%d calls=%d", invalidRecorder.Code, processor.calls)
	}

	validRecorder := httptest.NewRecorder()
	validContext, _ := gin.CreateTestContext(validRecorder)
	validContext.Request = httptest.NewRequest(
		http.MethodGet,
		"/internal/v1/nrf/nf-instances?target-nf-type=SMF&requester-nf-type=NWDAF&service-names=nsmf-event-exposure",
		nil,
	)
	server.HandleNFDiscovery(validContext)
	if validRecorder.Code != http.StatusOK || processor.calls != 1 {
		t.Fatalf("valid status=%d calls=%d body=%s", validRecorder.Code, processor.calls, validRecorder.Body.String())
	}
}
