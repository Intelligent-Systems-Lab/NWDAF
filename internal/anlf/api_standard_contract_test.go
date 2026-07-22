package anlf

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/internal/sbi/notifier"
	"github.com/free5gc/nwdaf/internal/util"
	"github.com/free5gc/openapi/models"
)

type standardContractProcessorStub struct {
	notificationErr error
	notifications   []models.NnwdafEventsSubscriptionNotification
	rawNotification []byte
	adrfResponse    *consumer.StandardAdrfResponse
	adrfBody        []byte
}

func (*standardContractProcessorStub) HandleMlModelProvisionNotify([]contract.ModelProvisionNotification) {
}

func (*standardContractProcessorStub) HandleAnalyticsReport(string, *contract.AnalyticsReport) error {
	return nil
}

func (*standardContractProcessorStub) HandleModelAccuracyReport(*contract.ModelAccuracyReport) error {
	return nil
}

func (*standardContractProcessorStub) HandleRuntimeCompletion(*contract.RuntimeCompletionEvent) error {
	return nil
}

func (p *standardContractProcessorStub) HandleEventsSubscriptionNotification(
	notifications []models.NnwdafEventsSubscriptionNotification,
	rawBody []byte,
) error {
	p.notifications = notifications
	p.rawNotification = rawBody
	return p.notificationErr
}

func (p *standardContractProcessorStub) StoreAdrfDataRecord(
	_ context.Context,
	body []byte,
) (*consumer.StandardAdrfResponse, error) {
	p.adrfBody = body
	return p.adrfResponse, nil
}

func TestEventsSubscriptionNotificationHTTPContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	validBody := `[{"subscriptionId":"sub-a","notifCorrId":"corr-a",` +
		`"eventNotifications":[{"event":"UE_COMMUNICATION"}]}]`
	tests := []struct {
		name         string
		contentType  string
		body         string
		error        error
		wantStatus   int
		wantLocation string
	}{
		{
			name: "JSON media parameters", contentType: "application/json; charset=utf-8",
			body: validBody, wantStatus: http.StatusNoContent,
		},
		{name: "missing media type", body: validBody, wantStatus: http.StatusUnsupportedMediaType},
		{
			name: "unsupported media type", contentType: "text/plain",
			body: validBody, wantStatus: http.StatusUnsupportedMediaType,
		},
		{name: "malformed JSON", contentType: "application/json", body: `{`, wantStatus: http.StatusBadRequest},
		{
			name:        "oversized body",
			contentType: "application/json",
			body:        strings.Repeat(" ", maxEventsSubscriptionNotificationBodyBytes+1),
			wantStatus:  http.StatusRequestEntityTooLarge,
		},
		{
			name:        "redirect response",
			contentType: "application/json",
			body:        validBody,
			error: &notifier.CallbackDeliveryError{
				StatusCode: http.StatusTemporaryRedirect,
				Location:   "http://consumer.example/redirected",
			},
			wantStatus:   http.StatusTemporaryRedirect,
			wantLocation: "http://consumer.example/redirected",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			context, _ := gin.CreateTestContext(recorder)
			context.Request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(test.body))
			if test.contentType != "" {
				context.Request.Header.Set("Content-Type", test.contentType)
			}
			processor := &standardContractProcessorStub{notificationErr: test.error}
			(&Server{processor: processor}).HandleEventsSubscriptionNotification(context)
			context.Writer.WriteHeaderNow()
			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, test.wantStatus, recorder.Body.String())
			}
			if recorder.Header().Get("Location") != test.wantLocation {
				t.Fatalf("Location = %q, want %q", recorder.Header().Get("Location"), test.wantLocation)
			}
			if test.wantStatus >= http.StatusBadRequest &&
				recorder.Header().Get("Content-Type") != util.ProblemJSONContentType {
				t.Fatalf("Content-Type = %q", recorder.Header().Get("Content-Type"))
			}
		})
	}
}

func TestAdrfStorageHTTPContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := `{"dataSub":[{"smfDataSub":{}}],"dataNotif":{"upfEventNotifs":[{}]}}`
	t.Run("missing media type", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		context, _ := gin.CreateTestContext(recorder)
		context.Request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		(&Server{processor: &standardContractProcessorStub{}}).StoreAdrfDataRecord(context)
		context.Writer.WriteHeaderNow()
		if recorder.Code != http.StatusUnsupportedMediaType ||
			recorder.Header().Get("Content-Type") != util.ProblemJSONContentType {
			t.Fatalf(
				"status=%d Content-Type=%q body=%s",
				recorder.Code, recorder.Header().Get("Content-Type"), recorder.Body.String(),
			)
		}
	})
	t.Run("malformed JSON", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		context, _ := gin.CreateTestContext(recorder)
		context.Request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{`))
		context.Request.Header.Set("Content-Type", "application/json")
		(&Server{processor: &standardContractProcessorStub{}}).StoreAdrfDataRecord(context)
		context.Writer.WriteHeaderNow()
		if recorder.Code != http.StatusBadRequest ||
			recorder.Header().Get("Content-Type") != util.ProblemJSONContentType {
			t.Fatalf(
				"status=%d Content-Type=%q body=%s",
				recorder.Code, recorder.Header().Get("Content-Type"), recorder.Body.String(),
			)
		}
	})
	t.Run("oversized body", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		context, _ := gin.CreateTestContext(recorder)
		context.Request = httptest.NewRequest(
			http.MethodPost,
			"/",
			strings.NewReader(strings.Repeat(" ", maxAdrfStorageBodyBytes+1)),
		)
		context.Request.Header.Set("Content-Type", "application/json")
		(&Server{processor: &standardContractProcessorStub{}}).StoreAdrfDataRecord(context)
		context.Writer.WriteHeaderNow()
		if recorder.Code != http.StatusRequestEntityTooLarge ||
			recorder.Header().Get("Content-Type") != util.ProblemJSONContentType {
			t.Fatalf(
				"status=%d Content-Type=%q body=%s",
				recorder.Code, recorder.Header().Get("Content-Type"), recorder.Body.String(),
			)
		}
	})
	t.Run("forwards standard response", func(t *testing.T) {
		accepted := map[string]any{"recordId": "record-a"}
		responseBody, err := json.Marshal(accepted)
		if err != nil {
			t.Fatal(err)
		}
		processor := &standardContractProcessorStub{adrfResponse: &consumer.StandardAdrfResponse{
			StatusCode:  http.StatusCreated,
			Location:    "http://adrf.example/data-store-records/record-a",
			ContentType: "application/json",
			Body:        responseBody,
		}}
		recorder := httptest.NewRecorder()
		context, _ := gin.CreateTestContext(recorder)
		context.Request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		context.Request.Header.Set("Content-Type", "application/json; charset=utf-8")
		(&Server{processor: processor}).StoreAdrfDataRecord(context)
		context.Writer.WriteHeaderNow()
		if recorder.Code != http.StatusCreated || recorder.Header().Get("Location") == "" ||
			!strings.Contains(recorder.Body.String(), "record-a") {
			t.Fatalf("status=%d Location=%q body=%s", recorder.Code, recorder.Header().Get("Location"), recorder.Body.String())
		}
		if string(processor.adrfBody) != body {
			t.Fatalf("forwarded body = %s", processor.adrfBody)
		}
	})
}
