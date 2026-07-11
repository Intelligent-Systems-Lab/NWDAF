package anlf

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
)

type modelAccuracyProcessorStub struct{ err error }

func (*modelAccuracyProcessorStub) HandleMlModelProvisionNotify([]contract.ModelProvisionNotification) {
}

func (*modelAccuracyProcessorStub) HandleAnalyticsReport(string, *contract.AnalyticsReport) error {
	return nil
}

func (p *modelAccuracyProcessorStub) HandleModelAccuracyReport(*contract.ModelAccuracyReport) error {
	return p.err
}

func TestHandleModelAccuracyReportStatusMapping(t *testing.T) {
	body := `{"report_id":"report-1","report_sequence":1,"generated_at":"2026-07-11T00:00:00Z",` +
		`"model_identity":{"provider_id":"mtlf-a","model_unique_id":42},"generation":2,` +
		`"monitoring_context":{"analytics_event":"UE_COMMUNICATION","scope_id":"scope-a"},` +
		`"accuracy_information":{"metrics":{"WAPE":0.1},"deviation":0.1,"sample_count":2,` +
		`"inference_count":2,"actual_traffic_scale":100,"predicted_traffic_scale":110,` +
		`"window_start":"2026-07-11T00:00:00Z","window_end":"2026-07-11T00:01:00Z"},` +
		`"retrain_context":{"subscription_ids":["sub-a"],"observation_source_ids":["corr-a"]}}`
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "accepted", want: http.StatusNoContent},
		{name: "stale", err: contract.ErrStaleModelGeneration, want: http.StatusConflict},
		{name: "temporary", err: errors.New("temporary"), want: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodPost, "/model-accuracy-reports", strings.NewReader(body))
			ctx.Request.Header.Set("Content-Type", "application/json")
			server := &Server{processor: &modelAccuracyProcessorStub{err: test.err}}

			server.HandleModelAccuracyReport(ctx)
			ctx.Writer.WriteHeaderNow()

			if recorder.Code != test.want {
				t.Fatalf("status = %d, want %d", recorder.Code, test.want)
			}
		})
	}
}
