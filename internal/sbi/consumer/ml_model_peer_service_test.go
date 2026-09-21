package consumer

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/free5gc/nwdaf/internal/backend"
)

func TestCreatePeerMLModelProvisionUsesLosslessStandardRequest(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"mLEventSubscs":[{"mLEvent":"UE_COMMUNICATION","mLEventFilter":{}}],
		"notifUri":"http://nwdaf-a.example/callback",
		"notifCorreId":"corr-a",
		"suppFeats":"8"
	}`)
	server := httptest.NewServer(http.HandlerFunc(func(
		response http.ResponseWriter,
		request *http.Request,
	) {
		if request.Method != http.MethodPost ||
			request.URL.Path != "/nnwdaf-mlmodelprovision/v1/subscriptions" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		for _, header := range []string{
			backend.TargetNFInstanceIDHeader,
			backend.TargetNFServiceInstanceIDHeader,
			backend.TargetAPIRootHeader,
			backend.TargetSelectionSourceHeader,
		} {
			if request.Header.Get(header) != "" {
				t.Errorf("private header %s crossed the peer boundary", header)
			}
		}
		received, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		if !bytes.Equal(received, body) {
			t.Errorf("request body was changed:\n%s", received)
		}
		response.Header().Set(
			"Location",
			"/nnwdaf-mlmodelprovision/v1/subscriptions/peer-provision",
		)
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusCreated)
		if _, err = response.Write(body); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer server.Close()

	client := &Consumer{mlModelPeerHTTPClient: server.Client()}
	result, err := client.CreatePeerMLModelProvision(
		context.Background(),
		backend.SelectedTarget{
			NFInstanceID:        "33333333-3333-4333-8333-333333333333",
			NFServiceInstanceID: "provision-service-c",
			ServiceName:         "nnwdaf-mlmodelprovision",
			APIRoot:             server.URL,
			SelectionSource:     backend.SelectionSourceConfigured,
		},
		body,
	)
	if err != nil {
		t.Fatalf("CreatePeerMLModelProvision() error = %v", err)
	}
	if result.StatusCode != http.StatusCreated ||
		result.Location != "/nnwdaf-mlmodelprovision/v1/subscriptions/peer-provision" ||
		string(result.Body) != string(body) {
		t.Fatalf("CreatePeerMLModelProvision() = %+v", result)
	}
}

func TestCreatePeerMLModelTrainingPreservesCandidateContract(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"mLEventSubscs":[{
			"mLEvent":"UE_COMMUNICATION",
			"mLEventFilter":{},
			"modelInterInfo":"bundle-v1"
		}],
		"notifUri":"http://nwdaf-a.example/training-callback",
		"notifCorreId":"candidate-client-a",
		"suppFeats":"4",
		"mlCorreId":"hierarchical-fl-001",
		"mLPreFlag":true,
		"mLModelTrainInfos":[{
			"dataAvReq":{"inpEvents":[{"upfEvent":"USER_DATA_USAGE_TRENDS"}]},
			"timeAvReq":"PT5M"
		}],
		"flTopology":{
			"nfInstanceId":"10000000-0000-4000-8000-000000000001"
		}
	}`)
	server := httptest.NewServer(http.HandlerFunc(func(
		response http.ResponseWriter,
		request *http.Request,
	) {
		if request.Method != http.MethodPost ||
			request.URL.Path != "/nnwdaf-mlmodeltraining/v1/subscriptions" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		for _, header := range []string{
			backend.TargetNFInstanceIDHeader,
			backend.TargetNFServiceInstanceIDHeader,
			backend.TargetAPIRootHeader,
			backend.TargetSelectionSourceHeader,
		} {
			if request.Header.Get(header) != "" {
				t.Errorf("private header %s crossed the peer boundary", header)
			}
		}
		received, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		if !bytes.Equal(received, body) {
			t.Errorf("request body was changed:\n%s", received)
		}
		response.Header().Set(
			"Location", "/nnwdaf-mlmodeltraining/v1/subscriptions/peer-training",
		)
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusCreated)
		if _, writeErr := response.Write(body); writeErr != nil {
			t.Errorf("write response body: %v", writeErr)
		}
	}))
	defer server.Close()

	client := &Consumer{mlModelPeerHTTPClient: server.Client()}
	result, err := client.CreatePeerMLModelTraining(
		context.Background(),
		backend.SelectedTarget{
			NFInstanceID:        "10000000-0000-4000-8000-000000000001",
			NFServiceInstanceID: "training-service-c",
			ServiceName:         "nnwdaf-mlmodeltraining",
			APIRoot:             server.URL,
			SelectionSource:     backend.SelectionSourceConfigured,
		},
		body,
	)
	if err != nil {
		t.Fatalf("CreatePeerMLModelTraining() error = %v", err)
	}
	if result.StatusCode != http.StatusCreated ||
		result.Location != "/nnwdaf-mlmodeltraining/v1/subscriptions/peer-training" ||
		!bytes.Equal(result.Body, body) {
		t.Fatalf("CreatePeerMLModelTraining() = %+v", result)
	}
}

func TestPeerMLModelTrainingMutationsPreserveCandidateContract(t *testing.T) {
	t.Parallel()

	replacement := []byte(`{
		"mLEventSubscs":[{
			"mLEvent":"UE_COMMUNICATION",
			"mLEventFilter":{},
			"modelInterInfo":"bundle-v1"
		}],
		"notifUri":"http://nwdaf-a.example/training-callback",
		"notifCorreId":"candidate-client-a",
		"suppFeats":"4",
		"mlCorreId":"hierarchical-fl-001",
		"flTopology":{
			"nfInstanceId":"10000000-0000-4000-8000-000000000001"
		},
		"retainedResultReq":true
	}`)
	patch := []byte(`{
		"flTopology":{"policy":{"minTrainNodes":1}},
		"retainedResultReq":true
	}`)
	received := make(map[string][]byte)
	server := httptest.NewServer(http.HandlerFunc(func(
		response http.ResponseWriter,
		request *http.Request,
	) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		received[request.Method] = body
		if request.Method == http.MethodPatch &&
			request.Header.Get("Content-Type") != "application/merge-patch+json" {
			t.Errorf("PATCH Content-Type = %q", request.Header.Get("Content-Type"))
		}
		response.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := &Consumer{mlModelPeerHTTPClient: server.Client()}
	location := server.URL + "/nnwdaf-mlmodeltraining/v1/subscriptions/peer-training"
	if _, err := client.ReplacePeerMLModelTraining(
		context.Background(), location, replacement,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PatchPeerMLModelTraining(
		context.Background(), location, patch,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DeletePeerMLModelTraining(
		context.Background(), location,
	); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(received[http.MethodPut], replacement) {
		t.Fatalf("PUT body=%s want=%s", received[http.MethodPut], replacement)
	}
	if !bytes.Equal(received[http.MethodPatch], patch) {
		t.Fatalf("PATCH body=%s want=%s", received[http.MethodPatch], patch)
	}
	if len(received[http.MethodDelete]) != 0 {
		t.Fatalf("DELETE body=%s", received[http.MethodDelete])
	}
}
