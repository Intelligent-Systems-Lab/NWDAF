package anlf

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/openapi/models"
)

func TestReadSmfEventExposureBodyRequiresNfIDForUpfEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		body        string
		contentType string
		wantCause   string
		wantStatus  int32
	}{
		{
			name:       "missing media type",
			body:       `{}`,
			wantCause:  "UNSUPPORTED_MEDIA_TYPE",
			wantStatus: http.StatusUnsupportedMediaType,
		},
		{
			name:        "unsupported media type",
			body:        `{}`,
			contentType: "text/plain",
			wantCause:   "UNSUPPORTED_MEDIA_TYPE",
			wantStatus:  http.StatusUnsupportedMediaType,
		},
		{
			name:        "malformed JSON",
			body:        `{`,
			contentType: "application/json",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "oversized body",
			body:        strings.Repeat(" ", maxSmfEventExposureBodyBytes+1),
			contentType: "application/json",
			wantCause:   "REQUEST_TOO_LARGE",
			wantStatus:  http.StatusRequestEntityTooLarge,
		},
		{
			name:        "missing NF ID",
			contentType: "application/json; charset=utf-8",
			body: `{"notifId":"corr-a","notifUri":"http://py/callbacks/upf-event-exposure",` +
				`"eventSubs":[{"event":"UPF_EVENT","upfEvents":[{"type":"USER_DATA_USAGE_MEASURES"}]}]}`,
			wantCause:  "MANDATORY_IE_MISSING",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:        "complete Release 18 UPF event subscription",
			contentType: "application/json; charset=utf-8",
			body: `{"nfId":"nwdaf-a","notifId":"corr-a",` +
				`"notifUri":"http://py/callbacks/upf-event-exposure",` +
				`"eventSubs":[{"event":"UPF_EVENT","upfEvents":[{"type":"USER_DATA_USAGE_MEASURES"}]}]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			context, _ := gin.CreateTestContext(httptest.NewRecorder())
			context.Request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(test.body))
			if test.contentType != "" {
				context.Request.Header.Set("Content-Type", test.contentType)
			}

			body, problem := readSmfEventExposureBody(context)

			if test.wantStatus == 0 {
				if problem != nil || len(body) == 0 {
					t.Fatalf("body=%s problem=%+v", body, problem)
				}
				return
			}
			if problem == nil || problem.Status != test.wantStatus || problem.Cause != test.wantCause {
				t.Fatalf("problem=%+v, want status %d cause %q", problem, test.wantStatus, test.wantCause)
			}
		})
	}
}

func TestWriteSmfEventExposureResponsePreservesRedirectLocation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)

	server := &Server{}
	responseBody, err := json.Marshal(models.ProblemDetails{
		Status: http.StatusTemporaryRedirect,
		Cause:  "SYSTEM_FAILURE",
	})
	if err != nil {
		t.Fatalf("marshal redirect body: %v", err)
	}
	server.writeSmfEventExposureResponse(
		c,
		&consumer.StandardSmfResponse{
			StatusCode:  http.StatusTemporaryRedirect,
			Location:    "http://smf.example/redirected",
			ContentType: "application/problem+json",
			Body:        responseBody,
		},
		&consumer.StandardSmfError{
			StatusCode: http.StatusTemporaryRedirect,
			ProblemDetails: models.ProblemDetails{
				Status: http.StatusTemporaryRedirect,
			},
		},
		true,
	)

	if recorder.Code != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusTemporaryRedirect)
	}
	if location := recorder.Header().Get("Location"); location != "http://smf.example/redirected" {
		t.Fatalf("Location = %q", location)
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "application/problem+json" {
		t.Fatalf("Content-Type = %q", contentType)
	}
	if !strings.Contains(recorder.Body.String(), "SYSTEM_FAILURE") {
		t.Fatalf("redirect body = %q", recorder.Body.String())
	}
}

func TestWriteSmfEventExposureCreateRejectsRedirect(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)

	server := &Server{}
	server.writeSmfEventExposureResponse(
		c,
		&consumer.StandardSmfResponse{
			StatusCode: http.StatusTemporaryRedirect,
			Location:   "http://smf.example/redirected",
		},
		&consumer.StandardSmfError{StatusCode: http.StatusTemporaryRedirect},
		false,
	)

	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadGateway)
	}
}
