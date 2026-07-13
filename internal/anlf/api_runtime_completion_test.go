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

type runtimeCompletionProcessorStub struct {
	err   error
	event *contract.RuntimeCompletionEvent
}

func (*runtimeCompletionProcessorStub) HandleMlModelProvisionNotify(
	[]contract.ModelProvisionNotification,
) {
}

func (*runtimeCompletionProcessorStub) HandleAnalyticsReport(string, *contract.AnalyticsReport) error {
	return nil
}

func (*runtimeCompletionProcessorStub) HandleModelAccuracyReport(*contract.ModelAccuracyReport) error {
	return nil
}

func (p *runtimeCompletionProcessorStub) HandleRuntimeCompletion(event *contract.RuntimeCompletionEvent) error {
	p.event = event
	return p.err
}

func TestHandleRuntimeCompletionStatusMapping(t *testing.T) {
	validBody := `{"completion_id":"completion-1","subscription_id":"sub-1",` +
		`"runtime_revision":2,"reason":"MAX_REPORTS_REACHED",` +
		`"completed_at":"2026-07-14T00:00:00Z","last_report_sequence":1}`
	tests := []struct {
		name   string
		pathID string
		body   string
		err    error
		want   int
	}{
		{name: "accepted", pathID: "sub-1", body: validBody, want: http.StatusNoContent},
		{
			name: "future revision", pathID: "sub-1", body: validBody,
			err: processor.ErrFutureRuntimeRevision, want: http.StatusConflict,
		},
		{
			name: "internal failure", pathID: "sub-1", body: validBody,
			err: errors.New("failed"), want: http.StatusInternalServerError,
		},
		{name: "path mismatch", pathID: "sub-2", body: validBody, want: http.StatusBadRequest},
		{
			name: "invalid reason", pathID: "sub-1",
			body: strings.Replace(validBody, "MAX_REPORTS_REACHED", "FAILED", 1),
			want: http.StatusBadRequest,
		},
		{
			name: "blank completion ID", pathID: "sub-1",
			body: strings.Replace(validBody, "completion-1", "   ", 1),
			want: http.StatusBadRequest,
		},
		{name: "malformed", pathID: "sub-1", body: `{`, want: http.StatusBadRequest},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Params = gin.Params{{Key: "subscriptionId", Value: test.pathID}}
			ctx.Request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(test.body))
			ctx.Request.Header.Set("Content-Type", "application/json")
			stub := &runtimeCompletionProcessorStub{err: test.err}
			server := &Server{processor: stub}

			server.HandleRuntimeCompletion(ctx)
			ctx.Writer.WriteHeaderNow()

			if recorder.Code != test.want {
				t.Fatalf("status = %d, want %d", recorder.Code, test.want)
			}
			if test.want == http.StatusNoContent && stub.event == nil {
				t.Fatal("completion event was not dispatched")
			}
		})
	}
}
