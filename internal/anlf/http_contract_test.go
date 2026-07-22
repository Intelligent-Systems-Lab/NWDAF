package anlf

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestReadStandardJSONBodyContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		body        string
		maxBytes    int64
		wantCause   string
	}{
		{name: "media parameters accepted", contentType: "application/json; charset=utf-8", body: `{}`, maxBytes: 2},
		{name: "missing media type", body: `{}`, maxBytes: 2, wantCause: "UNSUPPORTED_MEDIA_TYPE"},
		{
			name: "unsupported media type", contentType: "text/plain",
			body: `{}`, maxBytes: 2, wantCause: "UNSUPPORTED_MEDIA_TYPE",
		},
		{name: "oversized", contentType: "application/json", body: `{ }`, maxBytes: 2, wantCause: "REQUEST_TOO_LARGE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			context, _ := gin.CreateTestContext(httptest.NewRecorder())
			context.Request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(test.body))
			if test.contentType != "" {
				context.Request.Header.Set("Content-Type", test.contentType)
			}
			body, problem := readStandardJSONBody(context, test.maxBytes, "too large")
			if test.wantCause == "" {
				if problem != nil || string(body) != test.body {
					t.Fatalf("body=%q problem=%+v", body, problem)
				}
				return
			}
			if problem == nil || problem.Cause != test.wantCause {
				t.Fatalf("problem=%+v, want cause %q", problem, test.wantCause)
			}
		})
	}
}
