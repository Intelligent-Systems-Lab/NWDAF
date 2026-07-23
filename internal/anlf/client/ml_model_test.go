package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAnLFBackendMLModelClientUsesStandardMethodsAndRawBodies(t *testing.T) {
	t.Parallel()

	const resourceID = "33333333-3333-4333-8333-333333333333"
	createBody := []byte(`{
		"modelIds":[7],
		"notificationUri":"http://go.internal/callback",
		"notifCorrId":"corr-7",
		"futureField":{"release":18}
	}`)
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests++
		switch requests {
		case 1:
			if request.Method != http.MethodPost || request.URL.Path != mlModelMonitorSubscriptionsPath {
				t.Errorf("create request = %s %s", request.Method, request.URL.Path)
			}
			var raw map[string]json.RawMessage
			if err := json.NewDecoder(request.Body).Decode(&raw); err != nil {
				t.Errorf("decode create request: %v", err)
			}
			if _, found := raw["futureField"]; !found {
				t.Error("create request dropped unknown field")
			}
			response.Header().Set("Content-Type", "application/json")
			response.Header().Set("Location", "http://"+request.Host+request.URL.Path+"/"+resourceID)
			response.WriteHeader(http.StatusCreated)
			if _, err := response.Write(createBody); err != nil {
				t.Errorf("write create response: %v", err)
			}
		case 2:
			if request.Method != http.MethodPut || !strings.HasSuffix(request.URL.Path, "/"+resourceID) {
				t.Errorf("replace request = %s %s", request.Method, request.URL.Path)
			}
			response.WriteHeader(http.StatusNoContent)
		case 3:
			if request.Method != http.MethodDelete || !strings.HasSuffix(request.URL.Path, "/"+resourceID) {
				t.Errorf("delete request = %s %s", request.Method, request.URL.Path)
			}
			response.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, time.Second)
	client.httpClient = server.Client()
	created, err := client.CreateMLModelMonitorSubscription(context.Background(), createBody)
	if err != nil || created.StatusCode != http.StatusCreated ||
		!strings.Contains(string(created.Body), "futureField") {
		t.Fatalf("create response=%+v error=%v", created, err)
	}
	replaced, err := client.ReplaceMLModelMonitorSubscription(context.Background(), resourceID, createBody)
	if err != nil || replaced.StatusCode != http.StatusNoContent {
		t.Fatalf("replace response=%+v error=%v", replaced, err)
	}
	deleted, err := client.DeleteMLModelMonitorSubscription(context.Background(), resourceID)
	if err != nil || deleted.StatusCode != http.StatusNoContent || requests != 3 {
		t.Fatalf("delete response=%+v error=%v requests=%d", deleted, err, requests)
	}
}
