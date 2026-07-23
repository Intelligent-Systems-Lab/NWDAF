package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/free5gc/nwdaf/internal/backend"
	"github.com/free5gc/openapi/models"
)

func TestMTLFBackendMLModelClientPreservesRawContract(t *testing.T) {
	t.Parallel()

	const resourceID = "11111111-1111-4111-8111-111111111111"
	body := []byte(`{
		"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION","mLEventFilter":{},"futureNested":true}],
		"notifUri":"http://go.internal/callback",
		"futureTopLevel":{"release":18}
	}`)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != mlModelProvisionSubscriptionsPath {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		var raw map[string]json.RawMessage
		if err := json.NewDecoder(request.Body).Decode(&raw); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if _, found := raw["futureTopLevel"]; !found {
			t.Error("request dropped unknown Release 18 field")
		}
		response.Header().Set("Content-Type", "application/json; charset=utf-8")
		response.Header().Set("Location", serverURL(request)+mlModelProvisionSubscriptionsPath+"/"+resourceID)
		response.WriteHeader(http.StatusCreated)
		if _, err := response.Write(body); err != nil {
			t.Errorf("write provision response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewBackendClient(server.URL, time.Second, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.CreateMLModelProvisionSubscription(context.Background(), body)
	if err != nil {
		t.Fatalf("CreateMLModelProvisionSubscription() error = %v", err)
	}
	if result.StatusCode != http.StatusCreated || !strings.Contains(string(result.Body), "futureTopLevel") {
		t.Fatalf("response = %+v", result)
	}
	if id, parseErr := backend.ResourceIDFromLocation(result.Location); parseErr != nil || id != resourceID {
		t.Fatalf("resource ID=%q error=%v", id, parseErr)
	}
}

func TestMTLFBackendMLModelClientRejectsMalformedSuccessAndPreservesProblemDetails(t *testing.T) {
	t.Parallel()

	t.Run("malformed success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			response.Header().Set("Content-Type", "application/json")
			response.WriteHeader(http.StatusCreated)
			if _, err := response.Write([]byte(`{"modelId":1}`)); err != nil {
				t.Errorf("write malformed success: %v", err)
			}
		}))
		defer server.Close()
		client, err := NewBackendClient(server.URL, time.Second, server.Client())
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.CreateMLModelMonitorRegistration(
			context.Background(),
			[]byte(`{"consumerId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","modelId":1}`),
		)
		var contractError *backend.ContractError
		if err == nil || !strings.Contains(err.Error(), "invalid success representation") ||
			!errors.As(err, &contractError) {
			t.Fatalf("error = %T %v", err, err)
		}
	})

	t.Run("ProblemDetails", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			response.Header().Set("Content-Type", "application/problem+json")
			response.WriteHeader(http.StatusBadRequest)
			if err := json.NewEncoder(response).Encode(models.ProblemDetails{
				Status: http.StatusBadRequest, Cause: "INVALID_REQUEST",
			}); err != nil {
				t.Errorf("encode ProblemDetails: %v", err)
			}
		}))
		defer server.Close()
		client, err := NewBackendClient(server.URL, time.Second, server.Client())
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.CreateMLModelMonitorRegistration(
			context.Background(),
			[]byte(`{"consumerId":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","modelId":1}`),
		)
		var standardError *backend.StandardError
		if !errors.As(err, &standardError) || standardError.ProblemDetails.Cause != "INVALID_REQUEST" {
			t.Fatalf("error = %T %v", err, err)
		}
	})
}

func serverURL(request *http.Request) string {
	return "http://" + request.Host
}
