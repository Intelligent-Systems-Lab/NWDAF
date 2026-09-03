package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
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

func TestMTLFBackendMLModelTrainingClientPreservesCandidateContract(t *testing.T) {
	t.Parallel()

	const resourceID = "11111111-1111-4111-8111-111111111111"
	subscription := []byte(`{
		"mLEventSubscs":[{
			"mLEvent":"UE_COMMUNICATION",
			"mLEventFilter":{},
			"modelInterInfo":"bundle-v1"
		}],
		"notifUri":"http://go.internal/training-callback",
		"notifCorreId":"candidate-client-a",
		"suppFeats":"4",
		"mlCorreId":"hierarchical-fl-001",
		"mLPreFlag":true,
		"mLModelTrainInfos":[{
			"dataAvReq":{"inpEvents":[{"upfEvent":"USER_DATA_USAGE_TRENDS"}]},
			"timeAvReq":"PT5M"
		}],
		"x-flTopology":{
			"nfInstanceId":"10000000-0000-4000-8000-000000000001"
		}
	}`)
	patch := []byte(`{"x-retainedResultReq":true}`)
	notification := []byte(`{
		"notifCorreId":"candidate-client-a",
		"mlCorreId":"hierarchical-fl-001",
		"x-retainedResultStatus":"NOT_FOUND"
	}`)
	received := make(map[string][]byte)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		key := request.Method + " " + request.URL.Path
		received[key] = body
		switch key {
		case http.MethodPost + " " + mlModelTrainingSubscriptionsPath:
			response.Header().Set("Content-Type", "application/json")
			response.Header().Set(
				"Location", serverURL(request)+mlModelTrainingSubscriptionsPath+"/"+resourceID,
			)
			response.WriteHeader(http.StatusCreated)
			if _, writeErr := response.Write(subscription); writeErr != nil {
				t.Errorf("write response body: %v", writeErr)
			}
		case http.MethodPut + " " + mlModelTrainingSubscriptionsPath + "/" + resourceID,
			http.MethodPatch + " " + mlModelTrainingSubscriptionsPath + "/" + resourceID,
			http.MethodPost + " " + mlModelTrainingNotificationsPath:
			response.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s", key)
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := NewBackendClient(server.URL, time.Second, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.CreateMLModelTrainingSubscription(context.Background(), subscription); err != nil {
		t.Fatal(err)
	}
	if _, err = client.ReplaceMLModelTrainingSubscription(
		context.Background(), resourceID, subscription,
	); err != nil {
		t.Fatal(err)
	}
	if _, err = client.PatchMLModelTrainingSubscription(
		context.Background(), resourceID, patch,
	); err != nil {
		t.Fatal(err)
	}
	if _, err = client.DeliverMLModelTrainingNotification(
		context.Background(), notification,
	); err != nil {
		t.Fatal(err)
	}

	expected := map[string][]byte{
		http.MethodPost + " " + mlModelTrainingSubscriptionsPath:                     subscription,
		http.MethodPut + " " + mlModelTrainingSubscriptionsPath + "/" + resourceID:   subscription,
		http.MethodPatch + " " + mlModelTrainingSubscriptionsPath + "/" + resourceID: patch,
		http.MethodPost + " " + mlModelTrainingNotificationsPath:                     notification,
	}
	for key, body := range expected {
		if !bytes.Equal(received[key], body) {
			t.Fatalf("%s body=%s want=%s", key, received[key], body)
		}
	}
}

func serverURL(request *http.Request) string {
	return "http://" + request.Host
}
