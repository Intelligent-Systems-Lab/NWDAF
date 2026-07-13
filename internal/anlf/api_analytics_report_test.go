package anlf

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/anlf/processor"
)

type analyticsReportProcessorStub struct{ err error }

func (*analyticsReportProcessorStub) HandleMlModelProvisionNotify([]contract.ModelProvisionNotification) {
}

func (p *analyticsReportProcessorStub) HandleAnalyticsReport(string, *contract.AnalyticsReport) error {
	return p.err
}

func (*analyticsReportProcessorStub) HandleModelAccuracyReport(*contract.ModelAccuracyReport) error {
	return nil
}

func (*analyticsReportProcessorStub) HandleRuntimeCompletion(*contract.RuntimeCompletionEvent) error {
	return nil
}

func TestHandleAnalyticsReportStatusMapping(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := `{"report_id":"report-1","report_sequence":1,"runtime_revision":1,` +
		`"generated_at":"2026-07-10T12:00:00Z","event_notifications":[` +
		`{"event":"UE_COMMUNICATION","ue_communications":[]}]}`
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "success", want: http.StatusNoContent},
		{name: "not found", err: processor.ErrSubscriptionNotFound, want: http.StatusNotFound},
		{name: "stale", err: processor.ErrStaleAnalyticsReport, want: http.StatusConflict},
		{name: "invalid", err: processor.ErrInvalidAnalyticsReport, want: http.StatusBadRequest},
		{name: "in flight", err: processor.ErrAnalyticsReportInFlight, want: http.StatusServiceUnavailable},
		{name: "delivery", err: errors.New("network"), want: http.StatusBadGateway},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Params = gin.Params{{Key: "subscriptionId", Value: "sub-1"}}
			ctx.Request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
			ctx.Request.Header.Set("Content-Type", "application/json")
			server := &Server{processor: &analyticsReportProcessorStub{err: test.err}}
			server.HandleAnalyticsReport(ctx)
			ctx.Writer.WriteHeaderNow()
			if recorder.Code != test.want {
				t.Fatalf("status = %d, want %d", recorder.Code, test.want)
			}
		})
	}
}
