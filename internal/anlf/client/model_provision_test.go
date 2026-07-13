package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func provisionEvent(eventID string) contract.ModelProvisionEvent {
	return contract.ModelProvisionEvent{
		EventID: eventID,
		Source:  "DAISY_RETRAIN",
		ModelIdentity: contract.ModelIdentity{
			ProviderID: "provider-a", ModelUniqueID: 42,
		},
		Artifact:       contract.ModelArtifact{MLModelURL: "http://models/latest"},
		AnalyticsEvent: "UE_COMMUNICATION",
	}
}

func TestApplyModelProvisionEventRejectsEmptyEventIDWithoutRequest(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount++
	}))
	defer server.Close()

	_, err := NewClient(server.URL).ApplyModelProvisionEvent(context.Background(), provisionEvent(""))

	if err == nil {
		t.Fatal("expected empty event ID to be rejected")
	}
	if requestCount != 0 {
		t.Fatalf("request count = %d, want 0", requestCount)
	}
}

func TestApplyModelProvisionEventRetriesStaleWithSameBody(t *testing.T) {
	var mu sync.Mutex
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		defer func() {
			if err := request.Body.Close(); err != nil {
				t.Errorf("close request: %v", err)
			}
		}()
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		bodies = append(bodies, append([]byte(nil), body...))
		attempt := len(bodies)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if attempt == 1 {
			w.WriteHeader(http.StatusConflict)
			if _, writeErr := w.Write([]byte(`{"detail":"STALE_RUNTIME_STATE"}`)); writeErr != nil {
				t.Errorf("write stale response: %v", writeErr)
			}
			return
		}
		if _, writeErr := w.Write(
			[]byte(`{"status":"APPLIED","affected_runtime_count":2,"generation":3}`),
		); writeErr != nil {
			t.Errorf("write success response: %v", writeErr)
		}
	}))
	defer server.Close()

	response, err := NewClient(server.URL).ApplyModelProvisionEvent(
		context.Background(), provisionEvent("daisy:task-1"),
	)
	if err != nil {
		t.Fatalf("ApplyModelProvisionEvent() error = %v", err)
	}
	if response.Generation != 3 || len(bodies) != 2 {
		t.Fatalf("response = %+v bodies = %d", response, len(bodies))
	}
	if !bytes.Equal(bodies[0], bodies[1]) {
		t.Fatalf("retry body changed: first=%s second=%s", bodies[0], bodies[1])
	}
}

func TestApplyModelProvisionEventDoesNotRetryPermanentConflict(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusConflict)
		if _, err := w.Write([]byte(`{"detail":"EVENT_ID_CONFLICT"}`)); err != nil {
			t.Errorf("write conflict response: %v", err)
		}
	}))
	defer server.Close()

	_, err := NewClient(server.URL).ApplyModelProvisionEvent(
		context.Background(), provisionEvent("daisy:task-1"),
	)

	if err == nil {
		t.Fatal("expected event ID conflict")
	}
	if requestCount != 1 {
		t.Fatalf("request count = %d, want 1", requestCount)
	}
}

func TestApplyModelProvisionEventRetriesTransportFailure(t *testing.T) {
	attempts := 0
	client := NewClient("http://pyanlf.example")
	client.httpClient = &http.Client{Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
		attempts++
		if attempts == 1 {
			return nil, errors.New("connection reset")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(
				strings.NewReader(`{"status":"APPLIED","affected_runtime_count":1,"generation":2}`),
			),
			Header: make(http.Header),
		}, nil
	})}

	response, err := client.ApplyModelProvisionEvent(
		context.Background(), provisionEvent("daisy:task-1"),
	)
	if err != nil {
		t.Fatalf("ApplyModelProvisionEvent() error = %v", err)
	}
	if attempts != 2 || response.Generation != 2 {
		t.Fatalf("attempts = %d response = %+v", attempts, response)
	}
}

func TestApplyModelProvisionEventRetriesServerFailure(t *testing.T) {
	attempts := 0
	client := NewClient("http://pyanlf.example")
	client.httpClient = &http.Client{Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
		attempts++
		statusCode := http.StatusServiceUnavailable
		body := `{"detail":"MODEL_PREPARATION_FAILED"}`
		if attempts == 2 {
			statusCode = http.StatusOK
			body = `{"status":"APPLIED","affected_runtime_count":1,"generation":2}`
		}
		return &http.Response{
			StatusCode: statusCode,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	})}

	response, err := client.ApplyModelProvisionEvent(
		context.Background(), provisionEvent("daisy:task-1"),
	)
	if err != nil {
		t.Fatalf("ApplyModelProvisionEvent() error = %v", err)
	}
	if attempts != 2 || response.Generation != 2 {
		t.Fatalf("attempts = %d response = %+v", attempts, response)
	}
}
