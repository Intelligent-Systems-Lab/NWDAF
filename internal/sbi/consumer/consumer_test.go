package consumer

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"testing"

	"github.com/free5gc/nwdaf/internal/compat/nsmf"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

type testConsumerApp struct {
	cfg *factory.Config
	ctx *nwdaf_context.NWDAFContext
}

func newTestConsumerApp(cfg *factory.Config) *testConsumerApp {
	nwdaf_context.Init()
	return &testConsumerApp{
		cfg: cfg,
		ctx: nwdaf_context.GetSelf(),
	}
}

func (a *testConsumerApp) SetLogEnable(bool) {}

func (a *testConsumerApp) SetLogLevel(string) {}

func (a *testConsumerApp) SetReportCaller(bool) {}

func (a *testConsumerApp) Start() {}

func (a *testConsumerApp) Terminate() {}

func (a *testConsumerApp) Config() *factory.Config {
	return a.cfg
}

func (a *testConsumerApp) Context() *nwdaf_context.NWDAFContext {
	return a.ctx
}

// TestNewConsumer tests Consumer initialization
func TestNewConsumer(t *testing.T) {
	c, err := NewConsumer(newTestConsumerApp(nil))
	if err != nil {
		t.Errorf("NewConsumer() error = %v", err)
	}
	if c == nil {
		t.Fatal("NewConsumer() returned nil")
	} else if c.SmfService() == nil {
		t.Error("SMF service should be initialized")
	} else if c.MLModelProvisionService() == nil {
		t.Error("ML Model Provision service should be initialized")
	}
}

// TestConsumerContext tests Consumer.Context() method
func TestConsumerContext(t *testing.T) {
	app := newTestConsumerApp(nil)
	c, err := NewConsumer(app)
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}

	ctx := c.Context()
	if ctx == nil {
		t.Error("Context() returned nil")
	} else if ctx != app.ctx {
		t.Error("Context() should return the app-owned NWDAF context")
	}
}

// TestNsmfServiceHTTPClient tests HTTP client is properly initialized
func TestNsmfServiceHTTPClient(t *testing.T) {
	c, err := NewConsumer(newTestConsumerApp(nil))
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}

	// Get the HTTP client - should be a single instance
	client := c.SmfService().HTTPClient()
	if client == nil {
		t.Fatal("HTTPClient() returned nil")
	} else {
		// Same client should be returned (single instance)
		client2 := c.SmfService().HTTPClient()
		if client != client2 {
			t.Error("HTTPClient() should return the same instance")
		}

		// Verify timeout is set
		if client.Timeout == 0 {
			t.Error("HTTP client timeout should be set")
		}
	}
}

func TestConsumerRequestsSmfTokenForStandardRequest(t *testing.T) {
	var requestOrder []string
	var tokenScopes []string
	server := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case testAccessTokenPath:
			requestOrder = append(requestOrder, "token")
			if err := r.ParseForm(); err != nil {
				t.Errorf("parse token form: %v", err)
			}
			if got := r.Form.Get("targetNfType"); got != string(models.NrfNfManagementNfType_SMF) {
				t.Errorf("targetNfType = %q, want SMF", got)
			}
			if got := r.Form.Get("targetNfInstanceId"); got != "" {
				t.Errorf("targetNfInstanceId = %q, want omitted", got)
			}
			tokenScopes = append(tokenScopes, r.Form.Get("scope"))
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(models.NrfAccessTokenAccessTokenRsp{
				AccessToken: "smf-service-token",
				TokenType:   "Bearer",
				ExpiresIn:   300,
				Scope:       string(models.ServiceName_NSMF_EVENT_EXPOSURE),
			}); err != nil {
				t.Errorf("encode SMF access token response: %v", err)
			}
		case SmfEventExposurePath:
			requestOrder = append(requestOrder, "create")
			if got := r.Header.Get("Authorization"); got != "Bearer smf-service-token" {
				t.Errorf("subscription Authorization = %q", got)
			}
			w.Header().Set("Location", SmfEventExposurePath+"/smf-sub-1")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			if err := json.NewEncoder(w).Encode(models.NsmfEventExposure{
				SubId: "smf-sub-1", NotifId: "corr-a", NotifUri: "http://py/callback",
				EventSubs: []models.SmfEventExposureEventSubscription{{
					Event: models.SmfEvent(nsmf.EventUPFEvent),
				}},
			}); err != nil {
				t.Errorf("encode create response: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	app := newTestConsumerApp(nil)
	app.ctx = newNFManagementTestContext(t, server.URL)
	app.ctx.RecordOAuth2Required(server.URL + "/nnrf-nfm/v1/nf-instances/" + testNFInstanceID)
	consumerClient, err := NewConsumer(app)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	consumerClient.smfService.(*NsmfService).httpClient = server.Client()

	body := []byte(
		`{"notifId":"corr-a","notifUri":"http://py/callback",` +
			`"eventSubs":[{"event":"UPF_EVENT"}]}`,
	)
	if _, err = consumerClient.CreateSmfEventExposure(context.Background(), server.URL, body); err != nil {
		t.Fatalf("CreateSmfEventExposure() error = %v", err)
	}
	wantOrder := []string{"token", "create"}
	if !slices.Equal(requestOrder, wantOrder) {
		t.Fatalf("request order = %v, want %v", requestOrder, wantOrder)
	}
	wantScopes := []string{string(models.ServiceName_NSMF_EVENT_EXPOSURE)}
	if !slices.Equal(tokenScopes, wantScopes) {
		t.Fatalf("token scopes = %v, want %v", tokenScopes, wantScopes)
	}
}
