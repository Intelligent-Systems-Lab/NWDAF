package consumer

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h2non/gock"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

const (
	testNFInstanceID    = "11111111-1111-4111-8111-111111111111"
	testAccessTokenPath = "/oauth2/token"
	testNFDiscoveryPath = "/nnrf-disc/v1/nf-instances"
)

func TestRegisterNFInstanceCreatedUsesGeneratedContract(t *testing.T) {
	t.Parallel()

	var received models.NrfNfManagementNfProfile
	var server *httptest.Server
	server = newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			t.Errorf("HTTP protocol = %s, want HTTP/2", r.Proto)
		}
		if r.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", r.Method)
		}
		wantPath := "/nnrf-nfm/v1/nf-instances/" + testNFInstanceID
		if r.URL.Path != wantPath {
			t.Errorf("path = %q, want %q", r.URL.Path, wantPath)
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode request: %v", err)
		}
		returnedProfile := received
		returnedProfile.HeartBeatTimer = 10
		writeRegistrationResponse(t, w, http.StatusCreated, &returnedProfile, server.URL+wantPath)
	}))
	defer server.Close()

	ctx := newNFManagementTestContext(t, server.URL)
	result, err := newTestNrfService().RegisterNFInstance(context.Background(), ctx)
	if err != nil {
		t.Fatalf("RegisterNFInstance() error = %v", err)
	}
	if result.ResourceURI != server.URL+"/nnrf-nfm/v1/nf-instances/"+testNFInstanceID {
		t.Fatalf("ResourceURI = %q", result.ResourceURI)
	}
	if !result.RemoteRegistered || result.HeartBeatTimer != 10 {
		t.Fatalf("RegistrationResult = %+v, want remote registration and heartbeat metadata", result)
	}
	if received.NfInstanceId != testNFInstanceID ||
		received.NfType != models.NrfNfManagementNfType_NWDAF {
		t.Fatalf("received profile identity = %q/%q", received.NfInstanceId, received.NfType)
	}
	if len(received.NfServices) != 1 || len(received.NfServiceList) != 0 || len(received.PlmnList) != 0 {
		t.Fatalf(
			"received service representation nfServices=%d nfServiceList=%d plmnList=%d",
			len(received.NfServices),
			len(received.NfServiceList),
			len(received.PlmnList),
		)
	}
	if received.NwdafInfo == nil || len(received.NwdafInfo.NwdafEvents) != 1 ||
		received.NwdafInfo.NwdafEvents[0] != models.NwdafEvent_UE_COMMUNICATION {
		t.Fatalf("received NwdafInfo = %+v, want UE_COMMUNICATION", received.NwdafInfo)
	}
}

func TestNRFHTTPSClientPreservesCertificateVerification(t *testing.T) {
	t.Parallel()

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.EnableHTTP2 = true
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()

	httpClient, err := newNRFHTTPClient(server.URL)
	if err != nil {
		t.Fatalf("newNRFHTTPClient() error = %v", err)
	}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	response, requestErr := httpClient.Do(request)
	if requestErr == nil {
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Errorf("close unexpected HTTPS response body: %v", closeErr)
		}
		t.Fatal("HTTPS request error = nil, want untrusted certificate rejection")
	} else {
		var unknownAuthority x509.UnknownAuthorityError
		if !errors.As(requestErr, &unknownAuthority) {
			t.Fatalf("HTTPS request error = %v, want unknown certificate authority", requestErr)
		}
		if isRetryableRegistrationError(requestErr) {
			t.Fatalf("certificate verification error classified as retryable: %v", requestErr)
		}
	}
}

func TestRegisterNFInstanceUsesGeneratedClientInterception(t *testing.T) {
	defer gock.Off()

	nrfURI := "http://127.0.0.10:8000"
	httpClient, err := newNRFHTTPClient(nrfURI)
	if err != nil {
		t.Fatalf("newNRFHTTPClient() error = %v", err)
	}
	gock.InterceptClient(httpClient)
	defer gock.RestoreClient(httpClient)

	profile := newNFManagementTestContext(t, nrfURI).NFProfile()
	gock.New(nrfURI).
		Put("/nnrf-nfm/v1/nf-instances/"+testNFInstanceID).
		Reply(http.StatusCreated).
		AddHeader("Content-Type", "application/json").
		AddHeader("Location", nrfURI+"/nnrf-nfm/v1/nf-instances/"+testNFInstanceID).
		JSON(profile)

	service := newTestNrfService()
	service.httpClientFactory = func(string) (*http.Client, error) {
		return httpClient, nil
	}
	if _, err = service.RegisterNFInstance(
		context.Background(),
		newNFManagementTestContext(t, nrfURI),
	); err != nil {
		t.Fatalf("RegisterNFInstance() error = %v", err)
	}
	if !gock.IsDone() {
		t.Fatalf("pending generated-client HTTP mocks: %v", gock.Pending())
	}
}

func TestRegisterNFInstanceAcceptsReplacementWithoutLocation(t *testing.T) {
	t.Parallel()

	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		profile := decodeRegistrationProfile(t, r)
		writeRegistrationResponse(t, w, http.StatusOK, &profile, "")
	}))
	defer server.Close()

	result, err := newTestNrfService().RegisterNFInstance(
		context.Background(),
		newNFManagementTestContext(t, server.URL),
	)
	if err != nil {
		t.Fatalf("RegisterNFInstance() error = %v", err)
	}
	if result.ResourceURI != "" {
		t.Fatalf("ResourceURI = %q, want empty replacement location", result.ResourceURI)
	}
}

func TestRegisterNFInstanceRetriesServerFailureThenRecovers(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		profile := decodeRegistrationProfile(t, r)
		if attempts.Add(1) == 1 {
			writeProblemResponse(t, w, http.StatusServiceUnavailable)
			return
		}
		writeRegistrationResponse(t, w, http.StatusOK, &profile, "")
	}))
	defer server.Close()

	service := newTestNrfService()
	result, err := service.RegisterNFInstance(
		context.Background(),
		newNFManagementTestContext(t, server.URL),
	)
	if err != nil {
		t.Fatalf("RegisterNFInstance() error = %v", err)
	}
	if result.OAuth2Required {
		t.Fatal("OAuth2Required = true, want false")
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}
}

func TestRegisterNFInstanceRetriesTransportFailureThenRecovers(t *testing.T) {
	t.Parallel()

	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		profile := decodeRegistrationProfile(t, r)
		writeRegistrationResponse(t, w, http.StatusOK, &profile, "")
	}))
	defer server.Close()

	httpClient, err := newNRFHTTPClient(server.URL)
	if err != nil {
		t.Fatalf("newNRFHTTPClient() error = %v", err)
	}
	baseTransport := httpClient.Transport
	var attempts atomic.Int32
	httpClient.Transport = roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("simulated transport failure")
		}
		return baseTransport.RoundTrip(request)
	})

	service := newTestNrfService()
	service.httpClientFactory = func(string) (*http.Client, error) {
		return httpClient, nil
	}
	if _, err = service.RegisterNFInstance(
		context.Background(),
		newNFManagementTestContext(t, server.URL),
	); err != nil {
		t.Fatalf("RegisterNFInstance() error = %v", err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("transport attempts = %d, want 2", attempts.Load())
	}
}

func TestRegisterNFInstanceCancellationInterruptsRetryWait(t *testing.T) {
	t.Parallel()

	requestObserved := make(chan struct{}, 1)
	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestObserved <- struct{}{}
		writeProblemResponse(t, w, http.StatusServiceUnavailable)
	}))
	defer server.Close()

	service := newTestNrfService()
	service.initialRetryDelay = time.Hour
	service.maximumRetryDelay = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		_, err := service.RegisterNFInstance(ctx, newNFManagementTestContext(t, server.URL))
		resultCh <- err
	}()

	select {
	case <-requestObserved:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("registration request was not observed")
	}

	select {
	case err := <-resultCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RegisterNFInstance() error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("registration retry did not wake after cancellation")
	}
}

func TestRegisterNFInstanceCancellationInterruptsInFlightRequest(t *testing.T) {
	t.Parallel()

	requestObserved := make(chan struct{}, 1)
	server := newH2CTestServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		requestObserved <- struct{}{}
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		_, err := newTestNrfService().RegisterNFInstance(
			ctx,
			newNFManagementTestContext(t, server.URL),
		)
		resultCh <- err
	}()

	select {
	case <-requestObserved:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("registration request was not observed")
	}

	select {
	case err := <-resultCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RegisterNFInstance() error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("in-flight registration did not return after cancellation")
	}
}

func TestRegisterNFInstanceTreatsClientFailureAsTerminal(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		writeProblemResponse(t, w, http.StatusBadRequest)
	}))
	defer server.Close()

	result, err := newTestNrfService().RegisterNFInstance(
		context.Background(),
		newNFManagementTestContext(t, server.URL),
	)
	if err == nil || !strings.Contains(err.Error(), "status=400") {
		t.Fatalf("RegisterNFInstance() error = %v, want terminal status 400", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want 1", attempts.Load())
	}
	if result.RemoteRegistered {
		t.Fatalf("RegistrationResult = %+v, HTTP rejection is not a remote registration", result)
	}
}

func TestRegisterNFInstanceTreatsUnexpectedSuccessStatusAsTerminal(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	result, err := newTestNrfService().RegisterNFInstance(
		context.Background(),
		newNFManagementTestContext(t, server.URL),
	)
	if err == nil || !strings.Contains(err.Error(), "unexpected NRF registration success: status=202") {
		t.Fatalf("RegisterNFInstance() error = %v, want unexpected success status", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want 1", attempts.Load())
	}
	if result.RemoteRegistered {
		t.Fatalf("RegistrationResult = %+v, unsupported success status is not a validated registration", result)
	}
}

func TestRegisterNFInstanceRejectsRedirectWithoutFollowingIt(t *testing.T) {
	for _, statusCode := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		statusCode := statusCode
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			t.Parallel()

			var redirectedRequests atomic.Int32
			redirectTarget := newH2CTestServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				redirectedRequests.Add(1)
			}))
			defer redirectTarget.Close()

			server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Location", redirectTarget.URL)
				w.WriteHeader(statusCode)
				if _, writeErr := w.Write([]byte(`{"cause":"TEMPORARY_REDIRECTION"}`)); writeErr != nil {
					t.Errorf("write redirect response: %v", writeErr)
				}
			}))
			defer server.Close()

			_, err := newTestNrfService().RegisterNFInstance(
				context.Background(),
				newNFManagementTestContext(t, server.URL),
			)
			if !errors.Is(err, ErrUnsupportedNRFRedirect) {
				t.Fatalf("RegisterNFInstance() error = %v, want unsupported redirect", err)
			}
			if redirectedRequests.Load() != 0 {
				t.Fatalf("redirected requests = %d, want 0", redirectedRequests.Load())
			}
		})
	}
}

func TestRegisterNFInstanceAcceptsOAuth2Requirement(t *testing.T) {
	t.Parallel()

	var server *httptest.Server
	server = newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		profile := decodeRegistrationProfile(t, r)
		profile.CustomInfo = map[string]interface{}{"oauth2": true}
		writeRegistrationResponse(t, w, http.StatusCreated, &profile, server.URL+r.URL.Path)
	}))
	defer server.Close()

	result, err := newTestNrfService().RegisterNFInstance(
		context.Background(),
		newNFManagementTestContext(t, server.URL),
	)
	if err != nil {
		t.Fatalf("RegisterNFInstance() error = %v", err)
	}
	if !result.OAuth2Required || result.ResourceURI == "" {
		t.Fatalf("RegistrationResult = %+v", result)
	}
}

func TestRegisterNFInstancePreservesOAuth2RequirementOnIdentityMismatch(t *testing.T) {
	t.Parallel()

	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		profile := decodeRegistrationProfile(t, r)
		profile.NfInstanceId = "22222222-2222-4222-8222-222222222222"
		profile.CustomInfo = map[string]interface{}{"oauth2": true}
		writeRegistrationResponse(t, w, http.StatusCreated, &profile, "")
	}))
	defer server.Close()

	result, err := newTestNrfService().RegisterNFInstance(
		context.Background(),
		newNFManagementTestContext(t, server.URL),
	)
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("RegisterNFInstance() error = %v, want identity mismatch", err)
	}
	if !result.RemoteRegistered || !result.OAuth2Required {
		t.Fatalf("RegistrationResult = %+v, want protected remote registration", result)
	}
}

func TestRegisterNFInstanceMarksMalformedSuccessBodyAsRemoteRegistration(t *testing.T) {
	t.Parallel()

	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		if _, err := w.Write([]byte(`{"incomplete"`)); err != nil {
			t.Errorf("write malformed registration response: %v", err)
		}
	}))
	defer server.Close()

	result, err := newTestNrfService().RegisterNFInstance(
		context.Background(),
		newNFManagementTestContext(t, server.URL),
	)
	if err == nil {
		t.Fatal("RegisterNFInstance() error = nil, want malformed response error")
	}
	if !result.RemoteRegistered || result.OAuth2Required {
		t.Fatalf("RegistrationResult = %+v, want non-OAuth remote registration", result)
	}
}

func TestRegisterNFInstanceRejectsMismatchedReturnedIdentity(t *testing.T) {
	t.Parallel()

	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		profile := decodeRegistrationProfile(t, r)
		profile.NfInstanceId = "22222222-2222-4222-8222-222222222222"
		writeRegistrationResponse(t, w, http.StatusOK, &profile, "")
	}))
	defer server.Close()

	result, err := newTestNrfService().RegisterNFInstance(
		context.Background(),
		newNFManagementTestContext(t, server.URL),
	)
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("RegisterNFInstance() error = %v, want identity mismatch", err)
	}
	if !result.RemoteRegistered {
		t.Fatalf("RegistrationResult = %+v, want remote-success marker", result)
	}
}

func TestDeregisterNFInstanceContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
	}{
		{name: "deleted", statusCode: http.StatusNoContent},
		{name: "already absent", statusCode: http.StatusNotFound},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var method string
			var requestPath string
			server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				method = r.Method
				requestPath = r.URL.Path
				if tt.statusCode == http.StatusNoContent {
					w.WriteHeader(tt.statusCode)
					return
				}
				writeProblemResponse(t, w, tt.statusCode)
			}))
			defer server.Close()

			err := newTestNrfService().DeregisterNFInstance(
				context.Background(),
				newNFManagementTestContext(t, server.URL),
			)
			if err != nil {
				t.Fatalf("DeregisterNFInstance() error = %v", err)
			}
			if method != http.MethodDelete {
				t.Fatalf("method = %s, want DELETE", method)
			}
			wantPath := "/nnrf-nfm/v1/nf-instances/" + testNFInstanceID
			if requestPath != wantPath {
				t.Fatalf("path = %q, want %q", requestPath, wantPath)
			}
		})
	}
}

func TestDeregisterNFInstanceRespectsCancellation(t *testing.T) {
	t.Parallel()

	requestObserved := make(chan struct{}, 1)
	server := newH2CTestServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		requestObserved <- struct{}{}
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- newTestNrfService().DeregisterNFInstance(
			ctx,
			newNFManagementTestContext(t, server.URL),
		)
	}()

	select {
	case <-requestObserved:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("deregistration request was not observed")
	}

	select {
	case err := <-resultCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("DeregisterNFInstance() error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("deregistration did not return after cancellation")
	}
}

func TestDeregisterNFInstanceObtainsAccessTokenWhenOAuth2Required(t *testing.T) {
	t.Parallel()

	requests := make([]string, 0, 2)
	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			t.Errorf("HTTP protocol = %s, want HTTP/2", r.Proto)
		}
		switch r.URL.Path {
		case testAccessTokenPath:
			requests = append(requests, "token")
			if r.Method != http.MethodPost {
				t.Errorf("access token method = %s, want POST", r.Method)
			}
			if err := r.ParseForm(); err != nil {
				t.Errorf("parse access token form: %v", err)
			}
			wantForm := map[string]string{
				"grant_type":   "client_credentials",
				"nfInstanceId": testNFInstanceID,
				"nfType":       string(models.NrfNfManagementNfType_NWDAF),
				"targetNfType": string(models.NrfNfManagementNfType_NRF),
				"scope":        string(models.ServiceName_NNRF_NFM),
			}
			for key, want := range wantForm {
				if got := r.Form.Get(key); got != want {
					t.Errorf("access token form %s = %q, want %q", key, got, want)
				}
			}
			if got := r.Form.Get("targetNfInstanceId"); got != "" {
				t.Errorf("targetNfInstanceId = %q, want omitted", got)
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(models.NrfAccessTokenAccessTokenRsp{
				AccessToken: "protected-token",
				TokenType:   "Bearer",
				ExpiresIn:   300,
				Scope:       string(models.ServiceName_NNRF_NFM),
			}); err != nil {
				t.Errorf("encode access token response: %v", err)
			}
		case "/nnrf-nfm/v1/nf-instances/" + testNFInstanceID:
			requests = append(requests, "deregister")
			if r.Method != http.MethodDelete {
				t.Errorf("deregistration method = %s, want DELETE", r.Method)
			}
			if got := r.Header.Get("Authorization"); got != "Bearer protected-token" {
				t.Errorf("Authorization = %q, want bearer token", got)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	ctx := newNFManagementTestContext(t, server.URL)
	ctx.RecordOAuth2Required(server.URL + "/nnrf-nfm/v1/nf-instances/" + testNFInstanceID)
	if err := newTestNrfService().DeregisterNFInstance(context.Background(), ctx); err != nil {
		t.Fatalf("DeregisterNFInstance() error = %v", err)
	}
	if len(requests) != 2 || requests[0] != "token" || requests[1] != "deregister" {
		t.Fatalf("request order = %v, want [token deregister]", requests)
	}
}

func TestAccessTokenRequestRejectsInvalidResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
		body       string
		location   string
		wantErr    string
	}{
		{
			name:       "empty access token",
			statusCode: http.StatusOK,
			body:       `{"access_token":"","token_type":"Bearer"}`,
			wantErr:    "access_token is empty",
		},
		{
			name:       "server failure",
			statusCode: http.StatusInternalServerError,
			body:       `{"detail":"sensitive-response-content"}`,
			wantErr:    "status=500",
		},
		{
			name:       "unsupported redirect",
			statusCode: http.StatusTemporaryRedirect,
			body:       `{}`,
			location:   "http://other-nrf.example/oauth2/token",
			wantErr:    "NRF redirect is unsupported",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tt.location != "" {
					w.Header().Set("Location", tt.location)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.statusCode)
				if tt.body != "" {
					if _, err := w.Write([]byte(tt.body)); err != nil {
						t.Errorf("write response: %v", err)
					}
				}
			}))
			defer server.Close()

			_, err := newTestNrfService().getTokenContext(
				context.Background(),
				newNFManagementTestContext(t, server.URL),
				models.ServiceName_NNRF_NFM,
				models.NrfNfManagementNfType_NRF,
			)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("getTokenContext() error = %v, want %q", err, tt.wantErr)
			}
			if strings.Contains(err.Error(), "sensitive-response-content") {
				t.Fatalf("getTokenContext() leaked response body: %v", err)
			}
		})
	}
}

func TestAccessTokenRequestPreservesCallerCancellation(t *testing.T) {
	t.Parallel()

	requestObserved := make(chan struct{}, 1)
	server := newH2CTestServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		requestObserved <- struct{}{}
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		_, err := newTestNrfService().getTokenContext(
			ctx,
			newNFManagementTestContext(t, server.URL),
			models.ServiceName_NNRF_NFM,
			models.NrfNfManagementNfType_NRF,
		)
		resultCh <- err
	}()

	select {
	case <-requestObserved:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("access token request was not observed")
	}
	select {
	case err := <-resultCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("getTokenContext() error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("access token request did not return after cancellation")
	}
}

func TestAccessTokenRequestReturnsTransportFailure(t *testing.T) {
	t.Parallel()

	transportErr := errors.New("simulated access token transport failure")
	service := newTestNrfService()
	service.httpClientFactory = func(string) (*http.Client, error) {
		return &http.Client{
			Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return nil, transportErr
			}),
		}, nil
	}

	_, err := service.getTokenContext(
		context.Background(),
		newNFManagementTestContext(t, "http://127.0.0.10:8000"),
		models.ServiceName_NNRF_NFM,
		models.NrfNfManagementNfType_NRF,
	)
	if !errors.Is(err, transportErr) {
		t.Fatalf("getTokenContext() error = %v, want transport failure", err)
	}
	if !strings.Contains(err.Error(), "access token request failed") {
		t.Fatalf("getTokenContext() error = %v, want access token request context", err)
	}
}

func TestAccessTokenRequestCanceledBeforeDispatch(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := newTestNrfService().getTokenContext(
		ctx,
		newNFManagementTestContext(t, server.URL),
		models.ServiceName_NNRF_NFM,
		models.NrfNfManagementNfType_NRF,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("getTokenContext() error = %v, want context cancellation", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("access token requests dispatched = %d, want 0", got)
	}
}

func TestDiscoverSmfEventExposureUsesGeneratedContractAndSelectsAllUsableRoots(t *testing.T) {
	var receivedQuery url.Values
	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != testNFDiscoveryPath {
			t.Errorf("path = %q, want NFDiscovery collection", r.URL.Path)
		}
		receivedQuery = r.URL.Query()
		writeDiscoveryResponse(t, w, models.SearchResult{
			ValidityPeriod: 100,
			NfInstances: []models.NrfNfDiscoveryNfProfile{
				{},
				discoveryProfile(models.NrfNfDiscoveryNfService{
					ServiceName:     models.ServiceName_NSMF_PDUSESSION,
					Scheme:          models.UriScheme_HTTP,
					NfServiceStatus: models.NfServiceStatus_REGISTERED,
					ApiPrefix:       "http://wrong-service.example",
				}),
				discoveryProfile(discoveryService("http://smf-a.example", models.NfServiceStatus_REGISTERED)),
				discoveryProfile(discoveryService("http://smf-a.example/", models.NfServiceStatus_REGISTERED)),
				{
					Fqdn: "smf-b.example",
					NfServices: []models.NrfNfDiscoveryNfService{
						{
							ServiceName:     models.ServiceName_NSMF_EVENT_EXPOSURE,
							Scheme:          models.UriScheme_HTTPS,
							NfServiceStatus: models.NfServiceStatus_REGISTERED,
						},
					},
				},
				discoveryProfile(discoveryService("http://suspended.example", models.NfServiceStatus_SUSPENDED)),
				discoveryProfile(models.NrfNfDiscoveryNfService{
					ServiceName:     models.ServiceName_NSMF_EVENT_EXPOSURE,
					Scheme:          models.UriScheme_HTTP,
					NfServiceStatus: models.NfServiceStatus_REGISTERED,
					IpEndPoints: []models.IpEndPoint{
						{Ipv4Address: "192.0.2.40", Port: 8080},
					},
				}),
			},
		})
	}))
	defer server.Close()

	service := newTestNrfService()
	roots, err := service.DiscoverSmfEventExposure(
		context.Background(),
		newNFManagementTestContext(t, server.URL),
	)
	if err != nil {
		t.Fatalf("DiscoverSmfEventExposure() error = %v", err)
	}
	wantRoots := []string{"http://smf-a.example", "https://smf-b.example", "http://192.0.2.40:8080"}
	if !slices.Equal(roots, wantRoots) {
		t.Fatalf("service roots = %v, want %v", roots, wantRoots)
	}
	if got := receivedQuery.Get("target-nf-type"); got != string(models.NrfNfManagementNfType_SMF) {
		t.Errorf("target-nf-type = %q", got)
	}
	if got := receivedQuery.Get("requester-nf-type"); got != string(models.NrfNfManagementNfType_NWDAF) {
		t.Errorf("requester-nf-type = %q", got)
	}
	if got := receivedQuery.Get("service-names"); got != string(models.ServiceName_NSMF_EVENT_EXPOSURE) {
		t.Errorf("service-names = %q", got)
	}
	for _, absent := range []string{
		"requester-nf-instance-id", "target-plmn-list", "snssais", "dnn", "supi", "preferred-locality",
	} {
		if got := receivedQuery.Get(absent); got != "" {
			t.Errorf("unexpected query parameter %s=%q", absent, got)
		}
	}
}

func TestNFDiscoveryClientConstructionReusesPerNrfURI(t *testing.T) {
	service := newTestNrfService()
	service.httpClientFactory = func(string) (*http.Client, error) {
		return &http.Client{}, nil
	}

	first, err := service.getNFDiscoveryClient("http://nrf-a.example")
	if err != nil {
		t.Fatalf("first getNFDiscoveryClient() error = %v", err)
	}
	second, err := service.getNFDiscoveryClient("http://nrf-a.example")
	if err != nil {
		t.Fatalf("second getNFDiscoveryClient() error = %v", err)
	}
	other, err := service.getNFDiscoveryClient("https://nrf-b.example")
	if err != nil {
		t.Fatalf("other getNFDiscoveryClient() error = %v", err)
	}
	if first != second {
		t.Fatal("same NRF URI returned different NFDiscovery clients")
	}
	if first == other {
		t.Fatal("distinct NRF URIs returned the same NFDiscovery client")
	}
	if got := len(service.nfDiscoveryClients); got != 2 {
		t.Fatalf("NFDiscovery client count = %d, want 2", got)
	}
}

func TestDiscoverSmfEventExposureRejectsMissingNrfURI(t *testing.T) {
	_, err := newTestNrfService().DiscoverSmfEventExposure(
		context.Background(),
		&nwdaf_context.NWDAFContext{},
	)
	if err == nil || !strings.Contains(err.Error(), "requires NRF URI") {
		t.Fatalf("DiscoverSmfEventExposure() error = %v, want missing NRF URI", err)
	}
}

func TestDiscoverSmfEventExposureRejectsEmptyErrorAndRedirectResponses(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		result     models.SearchResult
		wantErr    string
		redirect   bool
	}{
		{name: "empty success", statusCode: http.StatusOK, result: models.SearchResult{}, wantErr: "no usable"},
		{name: "bad request", statusCode: http.StatusBadRequest, wantErr: "status=400"},
		{name: "unauthorized", statusCode: http.StatusUnauthorized, wantErr: "status=401"},
		{name: "forbidden", statusCode: http.StatusForbidden, wantErr: "status=403"},
		{name: "not found", statusCode: http.StatusNotFound, wantErr: "status=404"},
		{name: "server failure", statusCode: http.StatusInternalServerError, wantErr: "status=500"},
		{
			name:       "unsupported temporary redirect",
			statusCode: http.StatusTemporaryRedirect,
			wantErr:    "NRF redirect is unsupported",
			redirect:   true,
		},
		{
			name:       "unsupported permanent redirect",
			statusCode: http.StatusPermanentRedirect,
			wantErr:    "NRF redirect is unsupported",
			redirect:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tt.redirect {
					w.Header().Set("Location", "http://other-nrf.example/nnrf-disc/v1/nf-instances")
				}
				if tt.statusCode == http.StatusOK {
					writeDiscoveryResponse(t, w, tt.result)
					return
				}
				writeProblemResponse(t, w, tt.statusCode)
			}))
			defer server.Close()

			_, err := newTestNrfService().DiscoverSmfEventExposure(
				context.Background(),
				newNFManagementTestContext(t, server.URL),
			)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("DiscoverSmfEventExposure() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestDiscoverSmfEventExposureRejectsMalformedResponseWithoutCaching(t *testing.T) {
	var requests atomic.Int32
	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(`{"validityPeriod":`)); err != nil {
			t.Errorf("write malformed response: %v", err)
		}
	}))
	defer server.Close()

	service := newTestNrfService()
	nwdafCtx := newNFManagementTestContext(t, server.URL)
	for call := 1; call <= 2; call++ {
		if _, err := service.DiscoverSmfEventExposure(context.Background(), nwdafCtx); err == nil {
			t.Fatalf("malformed discovery call %d error = nil", call)
		}
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("malformed discovery requests = %d, want 2 uncached requests", got)
	}
}

func TestDiscoverSmfEventExposureReturnsTransportFailure(t *testing.T) {
	transportErr := errors.New("simulated discovery transport failure")
	service := newTestNrfService()
	service.httpClientFactory = func(string) (*http.Client, error) {
		return &http.Client{
			Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return nil, transportErr
			}),
		}, nil
	}

	_, err := service.DiscoverSmfEventExposure(
		context.Background(),
		newNFManagementTestContext(t, "http://127.0.0.10:8000"),
	)
	if !errors.Is(err, transportErr) {
		t.Fatalf("DiscoverSmfEventExposure() error = %v, want transport failure", err)
	}
}

func TestDiscoverSmfEventExposureObtainsOAuthToken(t *testing.T) {
	var requestOrder []string
	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case testAccessTokenPath:
			requestOrder = append(requestOrder, "token")
			if err := r.ParseForm(); err != nil {
				t.Errorf("parse token form: %v", err)
			}
			wantForm := map[string]string{
				"grant_type":   "client_credentials",
				"nfInstanceId": testNFInstanceID,
				"nfType":       string(models.NrfNfManagementNfType_NWDAF),
				"targetNfType": string(models.NrfNfManagementNfType_NRF),
				"scope":        string(models.ServiceName_NNRF_DISC),
			}
			for key, want := range wantForm {
				if got := r.Form.Get(key); got != want {
					t.Errorf("token form %s = %q, want %q", key, got, want)
				}
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(models.NrfAccessTokenAccessTokenRsp{
				AccessToken: "discovery-token",
				TokenType:   "Bearer",
				ExpiresIn:   300,
				Scope:       string(models.ServiceName_NNRF_DISC),
			}); err != nil {
				t.Errorf("encode discovery access token response: %v", err)
			}
		case testNFDiscoveryPath:
			requestOrder = append(requestOrder, "discovery")
			if got := r.Header.Get("Authorization"); got != "Bearer discovery-token" {
				t.Errorf("Authorization = %q, want discovery bearer", got)
			}
			writeDiscoveryResponse(t, w, models.SearchResult{
				NfInstances: []models.NrfNfDiscoveryNfProfile{
					discoveryProfile(discoveryService("http://smf.example", models.NfServiceStatus_REGISTERED)),
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	nwdafCtx := newNFManagementTestContext(t, server.URL)
	nwdafCtx.RecordOAuth2Required(server.URL + "/nnrf-nfm/v1/nf-instances/" + testNFInstanceID)
	if _, err := newTestNrfService().DiscoverSmfEventExposure(context.Background(), nwdafCtx); err != nil {
		t.Fatalf("DiscoverSmfEventExposure() error = %v", err)
	}
	if !slices.Equal(requestOrder, []string{"token", "discovery"}) {
		t.Fatalf("request order = %v", requestOrder)
	}
}

func TestDiscoverSmfEventExposureTokenFailurePreventsDiscoveryDispatch(t *testing.T) {
	var discoveryRequests atomic.Int32
	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case testAccessTokenPath:
			writeProblemResponse(t, w, http.StatusForbidden)
		case testNFDiscoveryPath:
			discoveryRequests.Add(1)
			writeDiscoveryResponse(t, w, models.SearchResult{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	nwdafCtx := newNFManagementTestContext(t, server.URL)
	nwdafCtx.RecordOAuth2Required(server.URL + "/nnrf-nfm/v1/nf-instances/" + testNFInstanceID)
	_, err := newTestNrfService().DiscoverSmfEventExposure(context.Background(), nwdafCtx)
	if err == nil || !strings.Contains(err.Error(), "status=403") {
		t.Fatalf("DiscoverSmfEventExposure() error = %v, want token rejection", err)
	}
	if got := discoveryRequests.Load(); got != 0 {
		t.Fatalf("discovery requests = %d, want 0 after token failure", got)
	}
}

func TestDiscoverSmfEventExposureCacheExpiryAndNoStaleFallback(t *testing.T) {
	var requests atomic.Int32
	serverFailure := atomic.Bool{}
	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		if serverFailure.Load() {
			writeProblemResponse(t, w, http.StatusServiceUnavailable)
			return
		}
		writeDiscoveryResponse(t, w, models.SearchResult{
			ValidityPeriod: 10,
			NfInstances: []models.NrfNfDiscoveryNfProfile{
				discoveryProfile(discoveryService("http://smf.example", models.NfServiceStatus_REGISTERED)),
			},
		})
	}))
	defer server.Close()

	current := time.Unix(1_700_000_000, 0)
	service := newTestNrfService()
	service.now = func() time.Time { return current }
	nwdafCtx := newNFManagementTestContext(t, server.URL)
	for i := 0; i < 2; i++ {
		if _, err := service.DiscoverSmfEventExposure(context.Background(), nwdafCtx); err != nil {
			t.Fatalf("cached discovery call %d error = %v", i+1, err)
		}
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests before expiry = %d, want 1", got)
	}

	current = current.Add(11 * time.Second)
	serverFailure.Store(true)
	if _, err := service.DiscoverSmfEventExposure(context.Background(), nwdafCtx); err == nil {
		t.Fatal("expired discovery refresh error = nil, want NRF failure")
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests after expiry = %d, want 2", got)
	}
	if _, err := service.DiscoverSmfEventExposure(context.Background(), nwdafCtx); err == nil {
		t.Fatal("second failed refresh error = nil, want NRF failure")
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("requests after uncached failure = %d, want 3", got)
	}
}

func TestDiscoverSmfEventExposureDoesNotCacheZeroValidity(t *testing.T) {
	var requests atomic.Int32
	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writeDiscoveryResponse(t, w, models.SearchResult{
			NfInstances: []models.NrfNfDiscoveryNfProfile{
				discoveryProfile(discoveryService("http://smf.example", models.NfServiceStatus_REGISTERED)),
			},
		})
	}))
	defer server.Close()

	service := newTestNrfService()
	nwdafCtx := newNFManagementTestContext(t, server.URL)
	for i := 0; i < 2; i++ {
		if _, err := service.DiscoverSmfEventExposure(context.Background(), nwdafCtx); err != nil {
			t.Fatalf("discovery call %d error = %v", i+1, err)
		}
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("zero-validity NRF requests = %d, want 2", got)
	}
}

func TestDiscoverSmfEventExposureCoalescesConcurrentMissAndAllowsCanceledWaiter(t *testing.T) {
	requestObserved := make(chan struct{}, 1)
	releaseRequest := make(chan struct{})
	var requests atomic.Int32
	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		requestObserved <- struct{}{}
		<-releaseRequest
		writeDiscoveryResponse(t, w, models.SearchResult{
			ValidityPeriod: 10,
			NfInstances: []models.NrfNfDiscoveryNfProfile{
				discoveryProfile(discoveryService("http://smf.example", models.NfServiceStatus_REGISTERED)),
			},
		})
	}))
	defer server.Close()

	service := newTestNrfService()
	nwdafCtx := newNFManagementTestContext(t, server.URL)
	ownerResult := make(chan error, 1)
	go func() {
		_, err := service.DiscoverSmfEventExposure(context.Background(), nwdafCtx)
		ownerResult <- err
	}()
	select {
	case <-requestObserved:
	case <-time.After(time.Second):
		t.Fatal("owner discovery request was not observed")
	}

	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	waiterResult := make(chan error, 1)
	go func() {
		_, err := service.DiscoverSmfEventExposure(waiterCtx, nwdafCtx)
		waiterResult <- err
	}()
	cancelWaiter()
	select {
	case err := <-waiterResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not return")
	}
	successfulWaiter := make(chan error, 1)
	go func() {
		_, err := service.DiscoverSmfEventExposure(context.Background(), nwdafCtx)
		successfulWaiter <- err
	}()

	close(releaseRequest)
	if err := <-ownerResult; err != nil {
		t.Fatalf("owner discovery error = %v", err)
	}
	if err := <-successfulWaiter; err != nil {
		t.Fatalf("coalesced waiter discovery error = %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("NRF requests = %d, want one coalesced request", got)
	}
}

func TestDiscoverSmfEventExposureOwnerCancellationReleasesWaiters(t *testing.T) {
	requestObserved := make(chan struct{}, 1)
	var requests atomic.Int32
	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			requestObserved <- struct{}{}
			<-r.Context().Done()
			return
		}
		writeDiscoveryResponse(t, w, models.SearchResult{
			ValidityPeriod: 10,
			NfInstances: []models.NrfNfDiscoveryNfProfile{
				discoveryProfile(discoveryService("http://smf.example", models.NfServiceStatus_REGISTERED)),
			},
		})
	}))
	defer server.Close()

	service := newTestNrfService()
	nwdafCtx := newNFManagementTestContext(t, server.URL)
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	ownerResult := make(chan error, 1)
	go func() {
		_, err := service.DiscoverSmfEventExposure(ownerCtx, nwdafCtx)
		ownerResult <- err
	}()
	select {
	case <-requestObserved:
	case <-time.After(time.Second):
		t.Fatal("owner discovery request was not observed")
	}

	waiterCtx := &doneObservedContext{
		Context:  context.Background(),
		observed: make(chan struct{}),
	}
	waiterResult := make(chan error, 1)
	go func() {
		_, err := service.DiscoverSmfEventExposure(waiterCtx, nwdafCtx)
		waiterResult <- err
	}()
	select {
	case <-waiterCtx.observed:
	case <-time.After(time.Second):
		t.Fatal("waiter did not begin waiting for the owner result")
	}
	cancelOwner()
	for name, result := range map[string]<-chan error{
		"owner":  ownerResult,
		"waiter": waiterResult,
	} {
		select {
		case err := <-result:
			if err == nil {
				t.Fatalf("%s discovery error = nil after owner cancellation", name)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s discovery did not return after owner cancellation", name)
		}
	}

	if _, err := service.DiscoverSmfEventExposure(context.Background(), nwdafCtx); err != nil {
		t.Fatalf("discovery after owner cancellation error = %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("NRF requests = %d, want failed owner request plus retry", got)
	}
}

func TestDiscoverSmfEventExposureCacheSeparatesNrfURI(t *testing.T) {
	newServer := func(endpoint string, requests *atomic.Int32) *httptest.Server {
		return newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests.Add(1)
			writeDiscoveryResponse(t, w, models.SearchResult{
				ValidityPeriod: 10,
				NfInstances: []models.NrfNfDiscoveryNfProfile{
					discoveryProfile(discoveryService(endpoint, models.NfServiceStatus_REGISTERED)),
				},
			})
		}))
	}
	var requestsA atomic.Int32
	var requestsB atomic.Int32
	serverA := newServer("http://smf-a.example", &requestsA)
	defer serverA.Close()
	serverB := newServer("http://smf-b.example", &requestsB)
	defer serverB.Close()

	service := newTestNrfService()
	for _, nwdafCtx := range []*nwdaf_context.NWDAFContext{
		newNFManagementTestContext(t, serverA.URL),
		newNFManagementTestContext(t, serverA.URL),
		newNFManagementTestContext(t, serverB.URL),
		newNFManagementTestContext(t, serverB.URL),
	} {
		if _, err := service.DiscoverSmfEventExposure(context.Background(), nwdafCtx); err != nil {
			t.Fatalf("DiscoverSmfEventExposure() error = %v", err)
		}
	}
	if got := requestsA.Load(); got != 1 {
		t.Fatalf("NRF A requests = %d, want 1", got)
	}
	if got := requestsB.Load(); got != 1 {
		t.Fatalf("NRF B requests = %d, want 1", got)
	}
}

func TestDiscoverSmfEventExposureCacheSeparatesOAuthMode(t *testing.T) {
	var discoveryRequests atomic.Int32
	var tokenRequests atomic.Int32
	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case testAccessTokenPath:
			tokenRequests.Add(1)
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(models.NrfAccessTokenAccessTokenRsp{
				AccessToken: "oauth-mode-token",
				TokenType:   "Bearer",
				ExpiresIn:   300,
				Scope:       string(models.ServiceName_NNRF_DISC),
			}); err != nil {
				t.Errorf("encode access token response: %v", err)
			}
		case testNFDiscoveryPath:
			discoveryRequests.Add(1)
			writeDiscoveryResponse(t, w, models.SearchResult{
				ValidityPeriod: 10,
				NfInstances: []models.NrfNfDiscoveryNfProfile{
					discoveryProfile(discoveryService("http://smf.example", models.NfServiceStatus_REGISTERED)),
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	service := newTestNrfService()
	nwdafCtx := newNFManagementTestContext(t, server.URL)
	for call := 0; call < 2; call++ {
		if _, err := service.DiscoverSmfEventExposure(context.Background(), nwdafCtx); err != nil {
			t.Fatalf("OAuth-disabled discovery call %d error = %v", call+1, err)
		}
	}
	nwdafCtx.RecordOAuth2Required(server.URL + "/nnrf-nfm/v1/nf-instances/" + testNFInstanceID)
	for call := 0; call < 2; call++ {
		if _, err := service.DiscoverSmfEventExposure(context.Background(), nwdafCtx); err != nil {
			t.Fatalf("OAuth-enabled discovery call %d error = %v", call+1, err)
		}
	}
	if got := discoveryRequests.Load(); got != 2 {
		t.Fatalf("discovery requests = %d, want one per OAuth mode", got)
	}
	if got := tokenRequests.Load(); got != 1 {
		t.Fatalf("token requests = %d, want one OAuth cache miss", got)
	}
}

func TestDiscoverSmfEventExposureCanceledBeforeDispatch(t *testing.T) {
	var requests atomic.Int32
	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writeDiscoveryResponse(t, w, models.SearchResult{})
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := newTestNrfService().DiscoverSmfEventExposure(
		ctx,
		newNFManagementTestContext(t, server.URL),
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DiscoverSmfEventExposure() error = %v, want context cancellation", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("discovery requests dispatched = %d, want 0", got)
	}
}

func discoveryProfile(service models.NrfNfDiscoveryNfService) models.NrfNfDiscoveryNfProfile {
	return models.NrfNfDiscoveryNfProfile{
		NfType:     models.NrfNfManagementNfType_SMF,
		NfStatus:   models.NrfNfManagementNfStatus_REGISTERED,
		NfServices: []models.NrfNfDiscoveryNfService{service},
	}
}

type doneObservedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *doneObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func discoveryService(apiPrefix string, status models.NfServiceStatus) models.NrfNfDiscoveryNfService {
	return models.NrfNfDiscoveryNfService{
		ServiceName:     models.ServiceName_NSMF_EVENT_EXPOSURE,
		Scheme:          models.UriScheme_HTTP,
		NfServiceStatus: status,
		ApiPrefix:       apiPrefix,
	}
}

func writeDiscoveryResponse(t *testing.T, w http.ResponseWriter, result models.SearchResult) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		t.Errorf("encode discovery response: %v", err)
	}
}

func newTestNrfService() *NrfService {
	service := newNrfService()
	service.initialRetryDelay = time.Millisecond
	service.maximumRetryDelay = 2 * time.Millisecond
	return service
}

func newNFManagementTestContext(t *testing.T, nrfURI string) *nwdaf_context.NWDAFContext {
	t.Helper()
	ctx := &nwdaf_context.NWDAFContext{NfId: testNFInstanceID}
	if err := ctx.ConfigureNFManagement(
		nrfURI,
		"",
		"NWDAF",
		"http://192.0.2.10:8080",
		"http",
		"192.0.2.10",
		8080,
		true,
	); err != nil {
		t.Fatalf("ConfigureNFManagement() error = %v", err)
	}
	return ctx
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func newH2CTestServer(handler http.Handler) *httptest.Server {
	return httptest.NewServer(h2c.NewHandler(handler, &http2.Server{}))
}

func decodeRegistrationProfile(t *testing.T, r *http.Request) models.NrfNfManagementNfProfile {
	t.Helper()
	var profile models.NrfNfManagementNfProfile
	if err := json.NewDecoder(r.Body).Decode(&profile); err != nil {
		t.Errorf("decode registration profile: %v", err)
	}
	return profile
}

func writeRegistrationResponse(
	t *testing.T,
	w http.ResponseWriter,
	statusCode int,
	profile *models.NrfNfManagementNfProfile,
	location string,
) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if location != "" {
		w.Header().Set("Location", location)
	}
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(profile); err != nil {
		t.Errorf("encode registration response: %v", err)
	}
}

func writeProblemResponse(t *testing.T, w http.ResponseWriter, statusCode int) {
	t.Helper()
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(models.ProblemDetails{Status: int32(statusCode)}); err != nil {
		t.Errorf("encode problem response: %v", err)
	}
}
