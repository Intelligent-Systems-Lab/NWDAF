package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/free5gc/nwdaf/internal/backend"
	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/logger"
	mtlfclient "github.com/free5gc/nwdaf/internal/mtlf/client"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/openapi/models"
)

func TestBuildBackendSyncRequestIncludesSmfResourceAssociations(t *testing.T) {
	nwdaf_context.Init()
	nwdafContext := nwdaf_context.GetSelf()
	if !nwdafContext.AddSmfPeerResourceRoute(&nwdaf_context.SmfPeerResourceRoute{
		SubscriptionID:           "peer-a",
		ResourceLocation:         "http://smf.example/subscriptions/peer-a",
		TargetAPIBaseURI:         "http://smf.example",
		CorrelationID:            "corr-a",
		NwdafSubscriptionIDs:     []string{"sub-a", "sub-b"},
		AcceptedSubscriptionJSON: json.RawMessage(`{"subId":"peer-a"}`),
	}) {
		t.Fatal("could not add SMF peer route")
	}
	app := &NwdafApp{
		nwdafCtx: nwdafContext,
		cfg: &factory.Config{Configuration: &factory.Configuration{
			Sbi: &factory.Sbi{Scheme: "http", RegisterIPv4: "127.0.0.1", Port: 8000},
			Anlf: &factory.AnlfConfig{Server: &factory.AuxiliaryServerConfig{
				RegisterIPv4: "127.0.0.1", Port: 9091,
			}},
		}},
	}

	snapshot := app.buildBackendSyncRequest(backend.KindAnLF)
	if len(snapshot.SmfResources) != 1 {
		t.Fatalf("SMF resources = %d", len(snapshot.SmfResources))
	}
	if got := snapshot.SmfResources[0].NwdafSubscriptionIDs; len(got) != 2 || got[0] != "sub-a" || got[1] != "sub-b" {
		t.Fatalf("SMF resource associations = %v", got)
	}
}

type fakeNFManagement struct {
	registerFn   func(context.Context) (consumer.RegistrationResult, error)
	deregisterFn func(context.Context) error
	registers    int
	deregisters  int
}

func (f *fakeNFManagement) RegisterNFInstance(ctx context.Context) (consumer.RegistrationResult, error) {
	f.registers++
	if f.registerFn != nil {
		return f.registerFn(ctx)
	}
	return consumer.RegistrationResult{}, nil
}

func (f *fakeNFManagement) DeregisterNFInstance(ctx context.Context) error {
	f.deregisters++
	if f.deregisterFn != nil {
		return f.deregisterFn(ctx)
	}
	return nil
}

// NewApp initializes package-global NWDAF and Gin state, so lifecycle tests
// must remain sequential until production supports multiple app instances.
func TestStartOwnedServersStartsAndStopsAllListeners(t *testing.T) {
	cfg := newLifecycleTestConfig(t, takeFreePort(t), takeFreePort(t), takeFreePort(t))
	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}

	startErr := app.startOwnedServers()
	if startErr != nil {
		t.Fatalf("startOwnedServers() error = %v", startErr)
	}

	assertPortOpen(t, cfg.GetSbiBindingAddr())
	assertPortOpen(t, cfg.GetAnlfServerBindingAddr())
	assertPortOpen(t, cfg.GetMtlfServerBindingAddr())

	app.stopOwnedServers()
	waitForWaitGroup(t, &app.wg)
}

func TestNewAppDoesNotRequireRunningBackends(t *testing.T) {
	cfg := newLifecycleTestConfig(t, takeFreePort(t), takeFreePort(t), takeFreePort(t))
	cfg.Configuration.AnlfBackend = &factory.AnlfBackendConfig{
		Enabled: true, Endpoint: "http://127.0.0.1:1", RequestTimeout: 1,
	}
	cfg.Configuration.MtlfBackend = &factory.MtlfBackendConfig{
		Enabled: true, Endpoint: "http://127.0.0.1:2", RequestTimeout: 1,
	}

	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}
	if app.anlfAvailability == nil || app.mtlfAvailability == nil {
		t.Fatal("configured backend availability trackers were not constructed")
	}
	app.Terminate()
}

func TestBackendMonitorsRunIndependentlyAndStopWithAppContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	startedAnlf := make(chan struct{})
	startedMtlf := make(chan struct{})
	var anlfOnce sync.Once
	var mtlfOnce sync.Once
	app := &NwdafApp{ctx: ctx, cancel: cancel}
	app.anlfAvailability = backend.NewAvailabilityMonitor(func(context.Context) (backend.ProbeResult, error) {
		anlfOnce.Do(func() { close(startedAnlf) })
		return backend.ProbeResult{}, nil
	})
	app.mtlfAvailability = backend.NewAvailabilityMonitor(func(context.Context) (backend.ProbeResult, error) {
		mtlfOnce.Do(func() { close(startedMtlf) })
		return backend.ProbeResult{Selection: "mongodb"}, nil
	})

	app.startBackendAvailabilityMonitors()
	select {
	case <-startedAnlf:
	case <-time.After(time.Second):
		t.Fatal("AnLF backend monitor did not probe")
	}
	select {
	case <-startedMtlf:
	case <-time.After(time.Second):
		t.Fatal("MTLF backend monitor did not probe")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && (!app.anlfAvailability.Usable() || !app.mtlfAvailability.Usable()) {
		time.Sleep(time.Millisecond)
	}
	if !app.anlfAvailability.Usable() || !app.mtlfAvailability.Usable() {
		t.Fatalf(
			"monitor snapshots: AnLF=%+v MTLF=%+v",
			app.anlfAvailability.Snapshot(),
			app.mtlfAvailability.Snapshot(),
		)
	}
	cancel()
	waitForWaitGroup(t, &app.wg)
}

func TestMtlfProbeUsesUnifiedSyncDataSourceAvailability(t *testing.T) {
	var inventories []backend.DataSourceAvailability
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/health/ready":
			if _, writeErr := writer.Write([]byte(
				`{"status":"ready","processInstanceId":"c11ed8a5-f093-459f-82dd-4a0fb36fb55d"}`,
			)); writeErr != nil {
				t.Errorf("Write() error = %v", writeErr)
			}
		case "/internal/v1/sync":
			var payload backend.SyncRequest
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Errorf("decode sync: %v", err)
			}
			inventories = append(inventories, payload.DataSourceAvailability)
			if _, writeErr := writer.Write([]byte(
				`{"processInstanceId":"c11ed8a5-f093-459f-82dd-4a0fb36fb55d",` +
					`"snapshotAccepted":true,"mongodbAvailable":false,"sourceSelection":{}}`,
			)); writeErr != nil {
				t.Errorf("Write() error = %v", writeErr)
			}
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(func() {
		server.CloseClientConnections()
		server.Close()
	})
	client, err := mtlfclient.NewBackendClient(server.URL, time.Second, server.Client())
	if err != nil {
		t.Fatalf("NewBackendClient() error = %v", err)
	}
	app := &NwdafApp{
		cfg: &factory.Config{Configuration: &factory.Configuration{
			Adrf: &factory.AdrfConfig{Url: "http://127.0.0.1:9888"},
		}},
		mtlfBackendClient: client,
		consumer: &consumer.Consumer{
			Adrf: consumer.NewAdrfClient("http://127.0.0.1:9888"),
		},
	}
	app.anlfMongoAvailable = true
	if _, err = app.probeMtlfBackend(context.Background()); err != nil {
		t.Fatalf("first probe error = %v", err)
	}
	app.anlfMongoAvailable = false
	if _, err = app.probeMtlfBackend(context.Background()); err != nil {
		t.Fatalf("second probe error = %v", err)
	}
	if len(inventories) != 2 || !inventories[0].ADRF || !inventories[0].MongoDB ||
		!inventories[1].ADRF || inventories[1].MongoDB {
		t.Fatalf("sync inventories = %v", inventories)
	}
}

func TestMtlfMonitorCancellationInterruptsSyncRequest(t *testing.T) {
	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/health/ready":
			writer.Header().Set("Content-Type", "application/json")
			if _, writeErr := writer.Write([]byte(
				`{"status":"ready","processInstanceId":"c11ed8a5-f093-459f-82dd-4a0fb36fb55d"}`,
			)); writeErr != nil {
				t.Errorf("Write() error = %v", writeErr)
			}
		case "/internal/v1/sync":
			close(syncStarted)
			select {
			case <-request.Context().Done():
			case <-releaseSync:
			}
			writer.WriteHeader(http.StatusGatewayTimeout)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(func() {
		server.CloseClientConnections()
		server.Close()
	})
	client, err := mtlfclient.NewBackendClient(server.URL, time.Second, server.Client())
	if err != nil {
		t.Fatalf("NewBackendClient() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	app := &NwdafApp{
		ctx:               ctx,
		cfg:               &factory.Config{Configuration: &factory.Configuration{}},
		mtlfBackendClient: client,
	}
	app.mtlfAvailability = backend.NewAvailabilityMonitor(app.probeMtlfBackend)
	app.startBackendAvailabilityMonitors()
	select {
	case <-syncStarted:
	case <-time.After(time.Second):
		t.Fatal("backend sync request did not start")
	}
	cancel()
	waitForWaitGroup(t, &app.wg)
	close(releaseSync)
}

func TestStartOwnedServersStartsAndStopsHttpsSbiListener(t *testing.T) {
	certPemPath, certKeyPath := writeTempTLSCertPair(t)

	cfg := newLifecycleTestConfig(t, takeFreePort(t), takeFreePort(t), takeFreePort(t))
	cfg.Configuration.Sbi.Scheme = "https"
	cfg.Configuration.Sbi.Tls = &factory.Tls{
		Pem: certPemPath,
		Key: certKeyPath,
	}

	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}

	startErr := app.startOwnedServers()
	if startErr != nil {
		t.Fatalf("startOwnedServers() error = %v", startErr)
	}

	assertTLSPortOpen(t, cfg.GetSbiBindingAddr())
	assertPortOpen(t, cfg.GetAnlfServerBindingAddr())
	assertPortOpen(t, cfg.GetMtlfServerBindingAddr())

	app.stopOwnedServers()
	waitForWaitGroup(t, &app.wg)
}

func TestStartOwnedServersCleansUpOnAuxiliaryBindFailure(t *testing.T) {
	sbiBlocker, sbiPort := takeOccupiedPort(t)
	defer closeListener(t, sbiBlocker)

	anlfPort := takeFreePort(t)
	mtlfPort := takeFreePort(t)
	cfg := newLifecycleTestConfig(t, sbiPort, anlfPort, mtlfPort)
	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}

	startErr := app.startOwnedServers()
	if startErr == nil {
		app.stopOwnedServers()
		t.Fatal("startOwnedServers() error = nil, want bind failure")
	}

	assertPortClosedEventually(t, cfg.GetAnlfServerBindingAddr())
	assertPortClosedEventually(t, cfg.GetMtlfServerBindingAddr())
	waitForWaitGroup(t, &app.wg)
}

func TestStartOwnedServersCleansUpOnMissingHttpsTLSConfig(t *testing.T) {
	cfg := newLifecycleTestConfig(t, takeFreePort(t), takeFreePort(t), takeFreePort(t))
	cfg.Configuration.Sbi.Scheme = "https"

	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}

	startErr := app.startOwnedServers()
	if startErr == nil {
		app.stopOwnedServers()
		t.Fatal("startOwnedServers() error = nil, want missing TLS config failure")
	}

	assertPortClosedEventually(t, cfg.GetAnlfServerBindingAddr())
	assertPortClosedEventually(t, cfg.GetMtlfServerBindingAddr())
	waitForWaitGroup(t, &app.wg)
}

func TestStartOwnedServersCleansUpOnUnsupportedSbiScheme(t *testing.T) {
	cfg := newLifecycleTestConfig(t, takeFreePort(t), takeFreePort(t), takeFreePort(t))
	cfg.Configuration.Sbi.Scheme = "ftp"

	if _, err := NewApp(context.Background(), cfg); err == nil {
		t.Fatal("NewApp() error = nil, want unsupported scheme failure")
	}
}

func TestStartRuntimeRegistersBeforeStartingOwnedListeners(t *testing.T) {
	cfg := newLifecycleTestConfig(t, takeFreePort(t), takeFreePort(t), takeFreePort(t))
	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}

	registrationObservedClosedListeners := false
	fake := &fakeNFManagement{
		registerFn: func(context.Context) (consumer.RegistrationResult, error) {
			registrationObservedClosedListeners = !portIsOpen(cfg.GetSbiBindingAddr()) &&
				!portIsOpen(cfg.GetAnlfServerBindingAddr()) &&
				!portIsOpen(cfg.GetMtlfServerBindingAddr())
			return consumer.RegistrationResult{ResourceURI: "http://nrf/nnrf-nfm/v1/nf-instances/id"}, nil
		},
	}
	app.nrfManagement = fake

	if startErr := app.startRuntime(); startErr != nil {
		t.Fatalf("startRuntime() error = %v", startErr)
	}
	if !registrationObservedClosedListeners {
		t.Fatal("NRF registration did not occur before listener startup")
	}
	assertPortOpen(t, cfg.GetSbiBindingAddr())
	assertPortOpen(t, cfg.GetAnlfServerBindingAddr())
	assertPortOpen(t, cfg.GetMtlfServerBindingAddr())

	app.Terminate()
	waitForWaitGroup(t, &app.wg)
	if fake.deregisters != 1 {
		t.Fatalf("deregister calls = %d, want 1", fake.deregisters)
	}
}

func TestStartRuntimeLeavesListenersClosedOnRegistrationFailure(t *testing.T) {
	cfg := newLifecycleTestConfig(t, takeFreePort(t), takeFreePort(t), takeFreePort(t))
	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}
	fake := &fakeNFManagement{
		registerFn: func(context.Context) (consumer.RegistrationResult, error) {
			return consumer.RegistrationResult{}, errors.New("terminal registration failure")
		},
	}
	app.nrfManagement = fake

	if startErr := app.startRuntime(); startErr == nil {
		t.Fatal("startRuntime() error = nil, want registration failure")
	}
	if portIsOpen(cfg.GetSbiBindingAddr()) || portIsOpen(cfg.GetAnlfServerBindingAddr()) ||
		portIsOpen(cfg.GetMtlfServerBindingAddr()) {
		t.Fatal("owned listener opened after terminal registration failure")
	}
	if fake.deregisters != 0 {
		t.Fatalf("deregister calls = %d, want 0", fake.deregisters)
	}
}

func TestRunTreatsRegistrationCancellationAsGracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := newLifecycleTestConfig(t, takeFreePort(t), takeFreePort(t), takeFreePort(t))
	app, err := NewApp(ctx, cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}

	registerStarted := make(chan struct{})
	app.nrfManagement = &fakeNFManagement{
		registerFn: func(ctx context.Context) (consumer.RegistrationResult, error) {
			close(registerStarted)
			<-ctx.Done()
			return consumer.RegistrationResult{}, ctx.Err()
		},
	}
	runResult := make(chan error, 1)
	go func() {
		runResult <- app.Run()
	}()

	select {
	case <-registerStarted:
	case <-time.After(time.Second):
		t.Fatal("registration did not start")
	}
	if portIsOpen(cfg.GetSbiBindingAddr()) || portIsOpen(cfg.GetAnlfServerBindingAddr()) ||
		portIsOpen(cfg.GetMtlfServerBindingAddr()) {
		t.Fatal("owned listener opened while NRF registration was in progress")
	}
	cancel()

	select {
	case runErr := <-runResult:
		if runErr != nil {
			t.Fatalf("Run() error = %v, want graceful shutdown", runErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return after registration cancellation")
	}
}

func TestStartRuntimeKeepsListenersClosedUntilRegistrationRecovers(t *testing.T) {
	cfg := newLifecycleTestConfig(t, takeFreePort(t), takeFreePort(t), takeFreePort(t))
	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}
	t.Cleanup(app.Terminate)

	attemptObserved := make(chan int)
	advanceRegistration := make(chan struct{})
	app.nrfManagement = &fakeNFManagement{
		registerFn: func(ctx context.Context) (consumer.RegistrationResult, error) {
			for attempt := 1; attempt <= 2; attempt++ {
				select {
				case attemptObserved <- attempt:
				case <-ctx.Done():
					return consumer.RegistrationResult{}, ctx.Err()
				}
				select {
				case <-advanceRegistration:
				case <-ctx.Done():
					return consumer.RegistrationResult{}, ctx.Err()
				}
			}
			return consumer.RegistrationResult{
				ResourceURI: "http://nrf/nnrf-nfm/v1/nf-instances/id",
			}, nil
		},
	}

	startResult := make(chan error, 1)
	go func() {
		startResult <- app.startRuntime()
	}()

	for wantAttempt := 1; wantAttempt <= 2; wantAttempt++ {
		select {
		case gotAttempt := <-attemptObserved:
			if gotAttempt != wantAttempt {
				t.Fatalf("registration attempt = %d, want %d", gotAttempt, wantAttempt)
			}
		case <-time.After(time.Second):
			t.Fatalf("registration attempt %d was not observed", wantAttempt)
		}
		if portIsOpen(cfg.GetSbiBindingAddr()) || portIsOpen(cfg.GetAnlfServerBindingAddr()) ||
			portIsOpen(cfg.GetMtlfServerBindingAddr()) {
			t.Fatalf("owned listener opened during registration attempt %d", wantAttempt)
		}
		advanceRegistration <- struct{}{}
	}

	select {
	case startErr := <-startResult:
		if startErr != nil {
			t.Fatalf("startRuntime() error = %v", startErr)
		}
	case <-time.After(time.Second):
		t.Fatal("startRuntime() did not complete after registration recovery")
	}
	assertPortOpen(t, cfg.GetSbiBindingAddr())
	assertPortOpen(t, cfg.GetAnlfServerBindingAddr())
	assertPortOpen(t, cfg.GetMtlfServerBindingAddr())

	app.Terminate()
	waitForWaitGroup(t, &app.wg)
}

func TestStartRuntimeCleansUpMalformedRegistrationSuccess(t *testing.T) {
	cfg := newLifecycleTestConfig(t, takeFreePort(t), takeFreePort(t), takeFreePort(t))
	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}
	resourceURI := "http://nrf/nnrf-nfm/v1/nf-instances/" + app.nwdafCtx.NfId
	fake := &fakeNFManagement{
		registerFn: func(context.Context) (consumer.RegistrationResult, error) {
			return consumer.RegistrationResult{
				ResourceURI:      resourceURI,
				RemoteRegistered: true,
			}, errors.New("malformed NRF registration success")
		},
	}
	app.nrfManagement = fake

	startErr := app.startRuntime()
	if startErr == nil || !strings.Contains(startErr.Error(), "malformed NRF registration success") {
		t.Fatalf("startRuntime() error = %v, want malformed success", startErr)
	}
	if fake.deregisters != 1 {
		t.Fatalf("deregister calls = %d, want cleanup call", fake.deregisters)
	}
	if app.nwdafCtx.RegistrationState().Registered {
		t.Fatal("registration remained active after malformed-success cleanup")
	}
	if portIsOpen(cfg.GetSbiBindingAddr()) || portIsOpen(cfg.GetAnlfServerBindingAddr()) ||
		portIsOpen(cfg.GetMtlfServerBindingAddr()) {
		t.Fatal("owned listener opened after malformed registration success")
	}
}

func TestStartRuntimeContinuesAfterOAuth2RequiredRegistration(t *testing.T) {
	cfg := newLifecycleTestConfig(t, takeFreePort(t), takeFreePort(t), takeFreePort(t))
	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}
	resourceURI := "http://nrf/nnrf-nfm/v1/nf-instances/id"
	fake := &fakeNFManagement{
		registerFn: func(context.Context) (consumer.RegistrationResult, error) {
			return consumer.RegistrationResult{
				ResourceURI:      resourceURI,
				OAuth2Required:   true,
				RemoteRegistered: true,
			}, nil
		},
	}
	app.nrfManagement = fake

	if startErr := app.startRuntime(); startErr != nil {
		t.Fatalf("startRuntime() error = %v", startErr)
	}
	state := app.nwdafCtx.RegistrationState()
	if !state.Registered || !state.OAuth2Required || state.ResourceURI != resourceURI {
		t.Fatalf("registration state = %+v", state)
	}
	if !portIsOpen(cfg.GetSbiBindingAddr()) || !portIsOpen(cfg.GetAnlfServerBindingAddr()) ||
		!portIsOpen(cfg.GetMtlfServerBindingAddr()) {
		t.Fatal("owned listener did not open after OAuth-required registration response")
	}

	app.Terminate()
	waitForWaitGroup(t, &app.wg)
	if fake.deregisters != 1 {
		t.Fatalf("deregister calls = %d, want 1", fake.deregisters)
	}
}

func TestLogOAuthCertificateStateReportsMissingAndUnusableMaterial(t *testing.T) {
	invalidPEMPath := filepath.Join(t.TempDir(), "invalid-nrf.pem")
	if err := os.WriteFile(invalidPEMPath, []byte("not PEM content"), 0o600); err != nil {
		t.Fatalf("write invalid PEM: %v", err)
	}

	tests := []struct {
		name     string
		certPath string
		wantLog  string
	}{
		{name: "missing configuration", wantLog: "nrfCertPem is not configured"},
		{
			name:     "unusable certificate",
			certPath: filepath.Join(t.TempDir(), "missing-nrf.pem"),
			wantLog:  "nrfCertPem is unusable",
		},
		{
			name:     "invalid PEM content",
			certPath: invalidPEMPath,
			wantLog:  "nrfCertPem is unusable",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newLifecycleTestConfig(t, takeFreePort(t), takeFreePort(t), takeFreePort(t))
			cfg.Configuration.NrfCertPem = tt.certPath
			app, err := NewApp(context.Background(), cfg)
			if err != nil {
				t.Fatalf("NewApp() error = %v", err)
			}

			var output bytes.Buffer
			originalOutput := logger.Log.Out
			logger.Log.SetOutput(&output)
			t.Cleanup(func() { logger.Log.SetOutput(originalOutput) })
			app.logOAuthCertificateState()
			if !strings.Contains(output.String(), tt.wantLog) {
				t.Fatalf("log output = %q, want %q", output.String(), tt.wantLog)
			}
		})
	}
}

func TestStartRuntimeDeregistersAfterPostRegistrationListenerFailure(t *testing.T) {
	anlfBlocker, anlfPort := takeOccupiedPort(t)
	defer closeListener(t, anlfBlocker)
	cfg := newLifecycleTestConfig(t, takeFreePort(t), anlfPort, takeFreePort(t))
	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}
	deregisterObservedReachableSBI := false
	fake := &fakeNFManagement{
		deregisterFn: func(context.Context) error {
			deregisterObservedReachableSBI = portIsOpen(cfg.GetSbiBindingAddr())
			return nil
		},
	}
	app.nrfManagement = fake

	if startErr := app.startRuntime(); startErr == nil {
		t.Fatal("startRuntime() error = nil, want listener bind failure")
	}
	if fake.registers != 1 || fake.deregisters != 1 {
		t.Fatalf("register/deregister calls = %d/%d, want 1/1", fake.registers, fake.deregisters)
	}
	if !deregisterObservedReachableSBI {
		t.Fatal("SBI was not reachable during listener-failure rollback deregistration")
	}
	assertPortClosedEventually(t, cfg.GetSbiBindingAddr())
	assertPortClosedEventually(t, cfg.GetMtlfServerBindingAddr())
}

func TestStartRuntimeOAuthRollbackUsesProtectedDeregistration(t *testing.T) {
	var tokenRequests atomic.Int32
	var deregistrationRequests atomic.Int32
	var protectedDeregistration atomic.Bool
	server := httptest.NewServer(h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/nnrf-nfm/v1/nf-instances/"):
			var profile models.NrfNfManagementNfProfile
			if err := json.NewDecoder(r.Body).Decode(&profile); err != nil {
				t.Errorf("decode registration profile: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			profile.CustomInfo = map[string]interface{}{"oauth2": true}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Location", serverURL(r)+r.URL.Path)
			w.WriteHeader(http.StatusCreated)
			if err := json.NewEncoder(w).Encode(profile); err != nil {
				t.Errorf("encode registration profile: %v", err)
			}
		case r.Method == http.MethodPost && r.URL.Path == "/oauth2/token":
			tokenRequests.Add(1)
			if err := r.ParseForm(); err != nil {
				t.Errorf("parse access token form: %v", err)
			}
			if got := r.Form.Get("scope"); got != string(models.ServiceName_NNRF_NFM) {
				t.Errorf("access token scope = %q, want %q", got, models.ServiceName_NNRF_NFM)
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(models.NrfAccessTokenAccessTokenRsp{
				AccessToken: "rollback-token",
				TokenType:   "Bearer",
				ExpiresIn:   300,
				Scope:       string(models.ServiceName_NNRF_NFM),
			}); err != nil {
				t.Errorf("encode access token response: %v", err)
			}
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/nnrf-nfm/v1/nf-instances/"):
			deregistrationRequests.Add(1)
			if r.Header.Get("Authorization") != "Bearer rollback-token" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			protectedDeregistration.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}), &http2.Server{}))
	defer server.Close()

	anlfBlocker, anlfPort := takeOccupiedPort(t)
	defer closeListener(t, anlfBlocker)
	cfg := newLifecycleTestConfig(t, takeFreePort(t), anlfPort, takeFreePort(t))
	cfg.Configuration.NrfUri = server.URL
	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}

	startErr := app.startRuntime()
	if startErr == nil {
		app.stopOwnedServers()
		t.Fatal("startRuntime() error = nil, want listener bind failure")
	}
	if tokenRequests.Load() != 1 || deregistrationRequests.Load() != 1 {
		t.Fatalf(
			"token/deregistration requests = %d/%d, want 1/1",
			tokenRequests.Load(),
			deregistrationRequests.Load(),
		)
	}
	if !protectedDeregistration.Load() {
		t.Fatal("listener-failure rollback deregistration did not use the NRF access token")
	}
	if app.nwdafCtx.RegistrationState().Registered {
		t.Fatal("registration remained active after protected rollback deregistration")
	}
	assertPortClosedEventually(t, cfg.GetSbiBindingAddr())
	assertPortClosedEventually(t, cfg.GetMtlfServerBindingAddr())
}

func TestTerminateDeregistersWhileSbiIsReachable(t *testing.T) {
	cfg := newLifecycleTestConfig(t, takeFreePort(t), takeFreePort(t), takeFreePort(t))
	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}
	deregisterObservedReachableSBI := false
	fake := &fakeNFManagement{
		deregisterFn: func(context.Context) error {
			deregisterObservedReachableSBI = portIsOpen(cfg.GetSbiBindingAddr())
			return nil
		},
	}
	app.nrfManagement = fake

	if startErr := app.startRuntime(); startErr != nil {
		t.Fatalf("startRuntime() error = %v", startErr)
	}
	app.Terminate()
	waitForWaitGroup(t, &app.wg)
	if !deregisterObservedReachableSBI {
		t.Fatal("SBI was not reachable during NRF deregistration")
	}
	assertPortClosedEventually(t, cfg.GetSbiBindingAddr())
}

func TestTerminateBoundsDeregistrationFailureAndStillStopsListeners(t *testing.T) {
	cfg := newLifecycleTestConfig(t, takeFreePort(t), takeFreePort(t), takeFreePort(t))
	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}
	app.deregisterTimeout = 20 * time.Millisecond
	fake := &fakeNFManagement{
		deregisterFn: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	app.nrfManagement = fake

	if startErr := app.startRuntime(); startErr != nil {
		t.Fatalf("startRuntime() error = %v", startErr)
	}
	startedAt := time.Now()
	app.Terminate()
	waitForWaitGroup(t, &app.wg)
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("shutdown elapsed = %s, want bounded shutdown", elapsed)
	}
	if fake.deregisters != 1 {
		t.Fatalf("deregister calls = %d, want 1", fake.deregisters)
	}
	assertPortClosedEventually(t, cfg.GetSbiBindingAddr())
	assertPortClosedEventually(t, cfg.GetAnlfServerBindingAddr())
	assertPortClosedEventually(t, cfg.GetMtlfServerBindingAddr())
}

func newLifecycleTestConfig(t *testing.T, sbiPort, anlfPort, mtlfPort int) *factory.Config {
	t.Helper()

	return &factory.Config{
		Configuration: &factory.Configuration{
			NrfUri: "http://127.0.0.10:8000",
			Sbi: &factory.Sbi{
				Scheme:       "http",
				BindingIPv4:  "127.0.0.1",
				RegisterIPv4: "127.0.0.1",
				Port:         sbiPort,
			},
			Anlf: &factory.AnlfConfig{
				Server: &factory.AuxiliaryServerConfig{
					BindingIPv4:  "127.0.0.1",
					RegisterIPv4: "127.0.0.1",
					Port:         anlfPort,
				},
			},
			Mtlf: &factory.MtlfConfig{
				Server: &factory.AuxiliaryServerConfig{
					BindingIPv4:  "127.0.0.1",
					RegisterIPv4: "127.0.0.1",
					Port:         mtlfPort,
				},
			},
			SupportedAnalytics: []string{factory.NwdafSupportedEventUEComm},
		},
	}
}

func portIsOpen(addr string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	conn, err := (&net.Dialer{Timeout: 100 * time.Millisecond}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	return conn.Close() == nil
}

func serverURL(r *http.Request) string {
	return "http://" + r.Host
}

func takeFreePort(t *testing.T) int {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer closeListener(t, listener)

	return listener.Addr().(*net.TCPAddr).Port
}

func takeOccupiedPort(t *testing.T) (net.Listener, int) {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}

	return listener, listener.Addr().(*net.TCPAddr).Port
}

func assertPortOpen(t *testing.T, addr string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", addr)
	if err != nil {
		t.Fatalf("DialContext(%q) error = %v", addr, err)
	}
	closeConn(t, conn)
}

func assertTLSPortOpen(t *testing.T, addr string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: time.Second},
		Config: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		t.Fatalf("tls.Dial(%q) error = %v", addr, err)
	}
	closeConn(t, conn)
}

func assertPortClosedEventually(t *testing.T, addr string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		conn, err := (&net.Dialer{Timeout: 100 * time.Millisecond}).DialContext(ctx, "tcp", addr)
		cancel()
		if err != nil {
			return
		}
		closeConn(t, conn)
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("port %s remained open after startup failure cleanup", addr)
}

func waitForWaitGroup(t *testing.T, wg interface{ Wait() }) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waitgroup did not finish after stopping owned servers")
	}
}

func closeListener(t *testing.T, listener net.Listener) {
	t.Helper()
	if err := listener.Close(); err != nil {
		t.Fatalf("Listener.Close() error = %v", err)
	}
}

func closeConn(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.Close(); err != nil {
		t.Fatalf("Conn.Close() error = %v", err)
	}
}

func writeTempTLSCertPair(t *testing.T) (string, string) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey() error = %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "127.0.0.1",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("x509.CreateCertificate() error = %v", err)
	}

	dir := t.TempDir()
	certPath := filepath.Join(dir, "nwdaf.pem")
	keyPath := filepath.Join(dir, "nwdaf.key")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})

	if writeErr := os.WriteFile(certPath, certPEM, 0o600); writeErr != nil {
		t.Fatalf("WriteFile(%q) error = %v", certPath, writeErr)
	}
	if writeErr := os.WriteFile(keyPath, keyPEM, 0o600); writeErr != nil {
		t.Fatalf("WriteFile(%q) error = %v", keyPath, writeErr)
	}

	return certPath, keyPath
}
