package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLiveAnlfBackendReadiness(t *testing.T) {
	endpoint := os.Getenv("ANLF_BACKEND_LIVE_ENDPOINT")
	if endpoint == "" {
		t.Skip("ANLF_BACKEND_LIVE_ENDPOINT is not set")
	}
	if err := NewClient(endpoint, 5*time.Second).CheckReadiness(context.Background()); err != nil {
		t.Fatalf("CheckReadiness() error = %v", err)
	}
}

func TestClientCheckReadiness(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/health/ready" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		writer.Header().Set("Content-Type", "application/json")
		if _, err := writer.Write([]byte(`{"status":"ready"}`)); err != nil {
			t.Errorf("Write() error = %v", err)
		}
	}))
	t.Cleanup(server.Close)

	if err := NewClient(server.URL, time.Second).CheckReadiness(context.Background()); err != nil {
		t.Fatalf("CheckReadiness() error = %v", err)
	}
}

func TestClientCheckReadinessRejectsFailureAndMalformedResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "not ready", status: http.StatusServiceUnavailable, body: `{"status":"not_ready"}`},
		{name: "malformed success", status: http.StatusOK, body: `{invalid`},
		{name: "oversized", status: http.StatusServiceUnavailable, body: strings.Repeat("x", maxBackendHealthBodyBytes+1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(test.status)
				if _, err := writer.Write([]byte(test.body)); err != nil {
					t.Errorf("Write() error = %v", err)
				}
			}))
			t.Cleanup(server.Close)
			if err := NewClient(server.URL, time.Second).CheckReadiness(context.Background()); err == nil {
				t.Fatal("CheckReadiness() error = nil")
			}
		})
	}
}

func TestClientCheckReadinessHonorsTimeoutAndCancellation(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
		writer.WriteHeader(http.StatusGatewayTimeout)
	}))
	t.Cleanup(server.Close)

	err := NewClient(server.URL, 20*time.Millisecond).CheckReadiness(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = NewClient(server.URL, time.Second).CheckReadiness(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
}
