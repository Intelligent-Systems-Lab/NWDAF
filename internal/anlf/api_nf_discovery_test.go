package anlf

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/openapi/models"
)

type nfDiscoveryProcessorStub struct {
	result *models.SearchResult
	err    error
	calls  int
}

func (*nfDiscoveryProcessorStub) HandleMlModelProvisionNotify([]contract.ModelProvisionNotification) {
}

func (*nfDiscoveryProcessorStub) HandleAnalyticsReport(string, *contract.AnalyticsReport) error {
	return nil
}

func (*nfDiscoveryProcessorStub) HandleModelAccuracyReport(*contract.ModelAccuracyReport) error {
	return nil
}

func (*nfDiscoveryProcessorStub) HandleRuntimeCompletion(*contract.RuntimeCompletionEvent) error {
	return nil
}

func (s *nfDiscoveryProcessorStub) HandleSmfNFDiscovery(context.Context) (*models.SearchResult, error) {
	s.calls++
	return s.result, s.err
}

func TestHandleSmfNFDiscoveryRequiresStandardQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)
	processor := &nfDiscoveryProcessorStub{result: &models.SearchResult{ValidityPeriod: 10}}
	server := &Server{processor: processor}

	invalidRecorder := httptest.NewRecorder()
	invalidContext, _ := gin.CreateTestContext(invalidRecorder)
	invalidContext.Request = httptest.NewRequest(
		http.MethodGet,
		"/internal/v1/nrf/nf-instances?target-nf-type=SMF",
		nil,
	)
	server.HandleSmfNFDiscovery(invalidContext)
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
	server.HandleSmfNFDiscovery(validContext)
	if validRecorder.Code != http.StatusOK || processor.calls != 1 {
		t.Fatalf("valid status=%d calls=%d body=%s", validRecorder.Code, processor.calls, validRecorder.Body.String())
	}
}
