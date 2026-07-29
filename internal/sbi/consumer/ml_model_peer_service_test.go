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
