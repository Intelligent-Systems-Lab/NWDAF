package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	backendcontract "github.com/free5gc/nwdaf/internal/backend"
)

const testMtlfProcessInstanceID = "c11ed8a5-f093-459f-82dd-4a0fb36fb55d"

func TestNewBackendClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		endpoint string
		timeout  time.Duration
		wantErr  bool
	}{
		{name: "valid", endpoint: "http://127.0.0.1:9092", timeout: time.Second},
		{name: "path rejected", endpoint: "http://127.0.0.1:9092/internal", timeout: time.Second, wantErr: true},
		{name: "userinfo rejected", endpoint: "http://user@127.0.0.1:9092", timeout: time.Second, wantErr: true},
		{name: "invalid timeout", endpoint: "http://127.0.0.1:9092", timeout: 0, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewBackendClient(test.endpoint, test.timeout, nil)
			if (err != nil) != test.wantErr {
				t.Fatalf("NewBackendClient() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestBackendClientSync(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/internal/v1/sync" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		writer.Header().Set("Content-Type", "application/json")
		if _, writeErr := writer.Write([]byte(
			`{"processInstanceId":"` + testMtlfProcessInstanceID +
				`","snapshotAccepted":true,"mongodbAvailable":false,"sourceSelection":{}}`,
		)); writeErr != nil {
			t.Errorf("Write() error = %v", writeErr)
		}
	}))
	t.Cleanup(server.Close)
	client, err := NewBackendClient(server.URL, time.Second, server.Client())
	if err != nil {
		t.Fatalf("NewBackendClient() error = %v", err)
	}
	response, err := client.Sync(context.Background(), backendcontract.SyncRequest{})
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if response.ProcessInstanceID != testMtlfProcessInstanceID {
		t.Fatalf("processInstanceId = %q", response.ProcessInstanceID)
	}
}

func TestBackendClientCheckReadiness(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/health/ready" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		writer.Header().Set("Content-Type", "application/json")
		body := []byte(
			`{"status":"ready","processInstanceId":"` + testMtlfProcessInstanceID + `"}`,
		)
		if _, writeErr := writer.Write(body); writeErr != nil {
			t.Errorf("Write() error = %v", writeErr)
		}
	}))
	t.Cleanup(server.Close)

	backend, err := NewBackendClient(server.URL, time.Second, server.Client())
	if err != nil {
		t.Fatalf("NewBackendClient() error = %v", err)
	}
	health, err := backend.CheckReadiness(context.Background())
	if err != nil {
		t.Fatalf("CheckReadiness() error = %v", err)
	}
	if health.ProcessInstanceID != testMtlfProcessInstanceID {
		t.Fatalf("processInstanceId = %q", health.ProcessInstanceID)
	}
}

func TestBackendClientCheckReadinessReturnsTypedStatusError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusServiceUnavailable)
		if _, writeErr := writer.Write(
			[]byte(`{"code":"NOT_READY","message":"reconciliation pending"}`),
		); writeErr != nil {
			t.Errorf("Write() error = %v", writeErr)
		}
	}))
	t.Cleanup(server.Close)

	backend, err := NewBackendClient(server.URL, time.Second, server.Client())
	if err != nil {
		t.Fatalf("NewBackendClient() error = %v", err)
	}
	_, err = backend.CheckReadiness(context.Background())
	var requestErr *BackendRequestError
	if !errors.As(err, &requestErr) {
		t.Fatalf("CheckReadiness() error = %T %v", err, err)
	}
	if requestErr.StatusCode != http.StatusServiceUnavailable || requestErr.Code != "NOT_READY" {
		t.Fatalf("BackendRequestError = %#v", requestErr)
	}
}

func TestBackendClientCheckReadinessBoundsResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
		if _, writeErr := writer.Write(
			[]byte(strings.Repeat("x", maxBackendReadinessBodyBytes+1)),
		); writeErr != nil {
			t.Errorf("Write() error = %v", writeErr)
		}
	}))
	t.Cleanup(server.Close)

	backend, err := NewBackendClient(server.URL, time.Second, server.Client())
	if err != nil {
		t.Fatalf("NewBackendClient() error = %v", err)
	}
	_, err = backend.CheckReadiness(context.Background())
	var requestErr *BackendRequestError
	if !errors.As(err, &requestErr) || requestErr.Code != "RESPONSE_TOO_LARGE" {
		t.Fatalf("CheckReadiness() error = %T %v", err, err)
	}
}

func TestBackendClientCheckReadinessHonorsTimeout(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
		writer.WriteHeader(http.StatusGatewayTimeout)
	}))
	t.Cleanup(server.Close)

	backend, err := NewBackendClient(server.URL, 20*time.Millisecond, server.Client())
	if err != nil {
		t.Fatalf("NewBackendClient() error = %v", err)
	}
	if _, err = backend.CheckReadiness(context.Background()); err == nil {
		t.Fatal("CheckReadiness() error = nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CheckReadiness() error = %v, want context deadline cause", err)
	}
}

func TestBackendClientCheckReadinessPreservesParentCancellation(t *testing.T) {
	t.Parallel()

	backend, err := NewBackendClient("http://127.0.0.1:9092", time.Second, nil)
	if err != nil {
		t.Fatalf("NewBackendClient() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = backend.CheckReadiness(ctx)
	var requestErr *BackendRequestError
	if !errors.As(err, &requestErr) {
		t.Fatalf("CheckReadiness() error = %T %v", err, err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CheckReadiness() error = %v, want context cancellation cause", err)
	}
}

func TestBackendClientCheckReadinessRequiresContext(t *testing.T) {
	t.Parallel()

	backend, err := NewBackendClient("http://127.0.0.1:9092", time.Second, nil)
	if err != nil {
		t.Fatalf("NewBackendClient() error = %v", err)
	}
	//nolint:staticcheck // This test verifies the client's defensive nil-context handling.
	if _, err = backend.CheckReadiness(nil); err == nil {
		t.Fatal("CheckReadiness() error = nil")
	}
}
