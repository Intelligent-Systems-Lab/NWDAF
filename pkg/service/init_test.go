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

func TestBuildBackendSyncRequestEncodesEmptySmfResourceAssociationsAsArray(t *testing.T) {
	nwdaf_context.Init()
	nwdafContext := nwdaf_context.GetSelf()
	if !nwdafContext.AddSmfPeerResourceRoute(&nwdaf_context.SmfPeerResourceRoute{
		SubscriptionID:   "peer-without-associations",
		ResourceLocation: "http://smf.example/subscriptions/peer-without-associations",
		TargetAPIBaseURI: "http://smf.example",
		CorrelationID:    "corr-without-associations",
	}) {
		t.Fatal("could not add SMF peer route")
	}
	app := &NwdafApp{nwdafCtx: nwdafContext}

	snapshot := app.buildBackendSyncRequest(backend.KindAnLF)
	if len(snapshot.SmfResources) != 1 {
		t.Fatalf("SMF resources = %d", len(snapshot.SmfResources))
	}
	if snapshot.SmfResources[0].NwdafSubscriptionIDs == nil {
		t.Fatal("SMF resource associations are nil, want an empty JSON array")
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal sync snapshot: %v", err)
	}
	if !bytes.Contains(encoded, []byte(`"nwdafSubscriptionIds":[]`)) {
		t.Fatalf("sync snapshot does not contain an empty association array: %s", encoded)
	}
}

func TestBuildBackendSyncRequestProjectsMLModelResourcesToTheirOwners(t *testing.T) {
	nwdaf_context.Init()
	nwdafContext := nwdaf_context.GetSelf()
	for _, route := range []nwdaf_context.MLModelProvisionSubscriptionRoute{
		{
			SubscriptionID:         "provision-anlf",
			AcceptedRepresentation: json.RawMessage(`{"owner":"anlf"}`),
			BackendRepresentation:  json.RawMessage(`{"owner":"mtlf","callback":"go"}`),
			Initiator:              nwdaf_context.MLModelRoutePartyAnLFBackend,
			Destination:            nwdaf_context.MLModelRoutePartyAnLFBackend,
		},
		{
			SubscriptionID:         "provision-external",
			AcceptedRepresentation: json.RawMessage(`{"owner":"external"}`),
			BackendRepresentation:  json.RawMessage(`{"owner":"mtlf","callback":"go"}`),
			Initiator:              nwdaf_context.MLModelRoutePartyExternal,
			Destination:            nwdaf_context.MLModelRoutePartyExternal,
		},
		{
			SubscriptionID:         "provision-remote",
			AcceptedRepresentation: json.RawMessage(`{"owner":"anlf-remote"}`),
			BackendRepresentation:  json.RawMessage(`{"owner":"peer"}`),
			Initiator:              nwdaf_context.MLModelRoutePartyAnLFBackend,
			Destination:            nwdaf_context.MLModelRoutePartyAnLFBackend,
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				SelectedTarget: &backend.SelectedTarget{
					NFInstanceID: "33333333-3333-4333-8333-333333333333",
				},
			},
		},
	} {
		route.PeerRoute.LifecycleState = nwdaf_context.MLModelRouteActive
		if !nwdafContext.AddMLModelProvisionSubscriptionRoute(route) {
			t.Fatalf("could not add provision route %s", route.SubscriptionID)
		}
	}
	if !nwdafContext.AddMLModelProvisionSubscriptionRoute(
		nwdaf_context.MLModelProvisionSubscriptionRoute{
			SubscriptionID: "provision-pending-cleanup",
			Initiator:      nwdaf_context.MLModelRoutePartyAnLFBackend,
			Destination:    nwdaf_context.MLModelRoutePartyAnLFBackend,
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				Direction:      nwdaf_context.MLModelRouteDirectionOutbound,
				LifecycleState: nwdaf_context.MLModelRoutePendingCleanup,
				PeerLocation:   "http://peer.example/subscriptions/pending",
			},
		},
	) {
		t.Fatal("could not add pending cleanup route")
	}
	for _, route := range []nwdaf_context.MLModelMonitorRegistrationRoute{
		{
			RegistrationID:         "registration-anlf",
			AcceptedRepresentation: json.RawMessage(`{"owner":"anlf"}`),
			BackendRepresentation:  json.RawMessage(`{"owner":"mtlf"}`),
			Initiator:              nwdaf_context.MLModelRoutePartyAnLFBackend,
		},
		{
			RegistrationID:         "registration-external",
			AcceptedRepresentation: json.RawMessage(`{"owner":"external"}`),
			BackendRepresentation:  json.RawMessage(`{"owner":"mtlf"}`),
			Initiator:              nwdaf_context.MLModelRoutePartyExternal,
		},
		{
			RegistrationID:         "registration-remote",
			AcceptedRepresentation: json.RawMessage(`{"owner":"anlf-remote"}`),
			BackendRepresentation:  json.RawMessage(`{"owner":"peer"}`),
			Initiator:              nwdaf_context.MLModelRoutePartyAnLFBackend,
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				SelectedTarget: &backend.SelectedTarget{
					NFInstanceID: "33333333-3333-4333-8333-333333333333",
				},
			},
		},
	} {
		route.PeerRoute.LifecycleState = nwdaf_context.MLModelRouteActive
		if !nwdafContext.AddMLModelMonitorRegistrationRoute(route) {
			t.Fatalf("could not add registration route %s", route.RegistrationID)
		}
	}
	for _, route := range []nwdaf_context.MLModelMonitorSubscriptionRoute{
		{
			SubscriptionID:         "monitor-mtlf",
			AcceptedRepresentation: json.RawMessage(`{"owner":"mtlf"}`),
			BackendRepresentation:  json.RawMessage(`{"owner":"anlf","callback":"go"}`),
			Destination:            nwdaf_context.MLModelRoutePartyMTLFBackend,
		},
		{
			SubscriptionID:         "monitor-external",
			AcceptedRepresentation: json.RawMessage(`{"owner":"external"}`),
			BackendRepresentation:  json.RawMessage(`{"owner":"anlf","callback":"go"}`),
			Destination:            nwdaf_context.MLModelRoutePartyExternal,
		},
		{
			SubscriptionID:         "monitor-remote",
			AcceptedRepresentation: json.RawMessage(`{"owner":"mtlf-remote"}`),
			BackendRepresentation:  json.RawMessage(`{"owner":"peer"}`),
			Destination:            nwdaf_context.MLModelRoutePartyMTLFBackend,
			PeerRoute: nwdaf_context.MLModelPeerRoute{
				SelectedTarget: &backend.SelectedTarget{
					NFInstanceID: "11111111-1111-4111-8111-111111111111",
				},
			},
		},
	} {
		route.PeerRoute.LifecycleState = nwdaf_context.MLModelRouteActive
		if !nwdafContext.AddMLModelMonitorSubscriptionRoute(route) {
			t.Fatalf("could not add monitor route %s", route.SubscriptionID)
		}
	}
	app := &NwdafApp{nwdafCtx: nwdafContext}

	anlfSnapshot := app.buildBackendSyncRequest(backend.KindAnLF)
	anlfProvisionIDs := make(map[string]json.RawMessage)
	for _, item := range anlfSnapshot.MLModelProvisionSubscriptions {
		anlfProvisionIDs[item.SubscriptionID] = item.Representation
	}
	if len(anlfProvisionIDs) != 2 ||
		anlfProvisionIDs["provision-anlf"] == nil ||
		anlfProvisionIDs["provision-remote"] == nil {
		t.Fatalf("AnLF provision snapshots = %+v", anlfSnapshot.MLModelProvisionSubscriptions)
	}
	anlfRegistrationIDs := make(map[string]struct{})
	for _, item := range anlfSnapshot.MLModelMonitorRegistrations {
		anlfRegistrationIDs[item.RegistrationID] = struct{}{}
	}
	if len(anlfRegistrationIDs) != 2 {
		t.Fatalf("AnLF registration snapshots = %+v", anlfSnapshot.MLModelMonitorRegistrations)
	}
	if _, found := anlfRegistrationIDs["registration-anlf"]; !found {
		t.Fatalf("AnLF registration snapshots = %+v", anlfSnapshot.MLModelMonitorRegistrations)
	}
	if _, found := anlfRegistrationIDs["registration-remote"]; !found {
		t.Fatalf("AnLF registration snapshots = %+v", anlfSnapshot.MLModelMonitorRegistrations)
	}
	if len(anlfSnapshot.MLModelMonitorSubscriptions) != 2 {
		t.Fatalf("AnLF monitor snapshots = %+v", anlfSnapshot.MLModelMonitorSubscriptions)
	}
	for _, item := range anlfSnapshot.MLModelMonitorSubscriptions {
		if item.SubscriptionID == "monitor-remote" {
			t.Fatalf("AnLF received remote MTLF monitor route: %+v", item)
		}
		if !bytes.Contains(item.Representation, []byte(`"owner":"anlf"`)) {
			t.Fatalf("AnLF monitor representation = %s", item.Representation)
		}
	}

	mtlfSnapshot := app.buildBackendSyncRequest(backend.KindMTLF)
	if len(mtlfSnapshot.MLModelProvisionSubscriptions) != 2 {
		t.Fatalf("MTLF provision snapshots = %+v", mtlfSnapshot.MLModelProvisionSubscriptions)
	}
	for _, item := range mtlfSnapshot.MLModelProvisionSubscriptions {
		if item.SubscriptionID == "provision-remote" {
			t.Fatalf("MTLF received remote AnLF provision route: %+v", item)
		}
		if !bytes.Contains(item.Representation, []byte(`"owner":"mtlf"`)) {
			t.Fatalf("MTLF provision representation = %s", item.Representation)
		}
	}
	if len(mtlfSnapshot.MLModelMonitorRegistrations) != 2 {
		t.Fatalf("MTLF registration snapshots = %+v", mtlfSnapshot.MLModelMonitorRegistrations)
	}
	for _, item := range mtlfSnapshot.MLModelMonitorRegistrations {
		if item.RegistrationID == "registration-remote" {
			t.Fatalf("MTLF received remote AnLF registration route: %+v", item)
		}
	}
	mtlfMonitorIDs := make(map[string]json.RawMessage)
	for _, item := range mtlfSnapshot.MLModelMonitorSubscriptions {
		mtlfMonitorIDs[item.SubscriptionID] = item.Representation
	}
	if len(mtlfMonitorIDs) != 2 ||
		mtlfMonitorIDs["monitor-mtlf"] == nil ||
		mtlfMonitorIDs["monitor-remote"] == nil {
		t.Fatalf("MTLF monitor snapshots = %+v", mtlfSnapshot.MLModelMonitorSubscriptions)
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
	cfg := newFreeLifecycleTestConfig(t)
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

	app.stopOwnedServers()
	waitForWaitGroup(t, &app.wg)
}

func TestNewAppDoesNotRequireRunningBackends(t *testing.T) {
	cfg := newFreeLifecycleTestConfig(t)
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

func TestMtlfProbeUsesUnifiedTrainingDataSource(t *testing.T) {
	var sources []backend.DataSource
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
			sources = append(sources, payload.TrainingDataSource)
			if _, writeErr := writer.Write([]byte(
				`{"processInstanceId":"c11ed8a5-f093-459f-82dd-4a0fb36fb55d",` +
					`"snapshotAccepted":true}`,
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
		cfg:               &factory.Config{Configuration: &factory.Configuration{}},
		mtlfBackendClient: client,
	}
	app.trainingDataSource = backend.DataSourceMongoDB
	if _, err = app.probeMtlfBackend(context.Background()); err != nil {
		t.Fatalf("first probe error = %v", err)
	}
	app.trainingDataSource = backend.DataSourceUnavailable
	if _, err = app.probeMtlfBackend(context.Background()); err != nil {
		t.Fatalf("second probe error = %v", err)
	}
	if len(sources) != 2 || sources[0] != backend.DataSourceMongoDB ||
		sources[1] != backend.DataSourceUnavailable {
		t.Fatalf("sync sources = %v", sources)
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

	cfg := newFreeLifecycleTestConfig(t)
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
	waitForWaitGroup(t, &app.wg)
}

func TestStartOwnedServersCleansUpOnMissingHttpsTLSConfig(t *testing.T) {
	cfg := newFreeLifecycleTestConfig(t)
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
	waitForWaitGroup(t, &app.wg)
}

func TestStartOwnedServersCleansUpOnUnsupportedSbiScheme(t *testing.T) {
	cfg := newFreeLifecycleTestConfig(t)
	cfg.Configuration.Sbi.Scheme = "ftp"

	if _, err := NewApp(context.Background(), cfg); err == nil {
		t.Fatal("NewApp() error = nil, want unsupported scheme failure")
	}
}

func TestStartRuntimeRegistersBeforeStartingOwnedListeners(t *testing.T) {
	cfg := newFreeLifecycleTestConfig(t)
	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}

	registrationObservedClosedListeners := false
	fake := &fakeNFManagement{
		registerFn: func(context.Context) (consumer.RegistrationResult, error) {
			registrationObservedClosedListeners = !portIsOpen(cfg.GetSbiBindingAddr()) &&
				!portIsOpen(cfg.GetAnlfServerBindingAddr())
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

	app.Terminate()
	waitForWaitGroup(t, &app.wg)
	if fake.deregisters != 1 {
		t.Fatalf("deregister calls = %d, want 1", fake.deregisters)
	}
}

func TestStartRuntimeLeavesListenersClosedOnRegistrationFailure(t *testing.T) {
	cfg := newFreeLifecycleTestConfig(t)
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
	if portIsOpen(cfg.GetSbiBindingAddr()) || portIsOpen(cfg.GetAnlfServerBindingAddr()) {
		t.Fatal("owned listener opened after terminal registration failure")
	}
	if fake.deregisters != 0 {
		t.Fatalf("deregister calls = %d, want 0", fake.deregisters)
	}
}

func TestRunTreatsRegistrationCancellationAsGracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := newFreeLifecycleTestConfig(t)
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
	if portIsOpen(cfg.GetSbiBindingAddr()) || portIsOpen(cfg.GetAnlfServerBindingAddr()) {
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
	cfg := newFreeLifecycleTestConfig(t)
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
		if portIsOpen(cfg.GetSbiBindingAddr()) || portIsOpen(cfg.GetAnlfServerBindingAddr()) {
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

	app.Terminate()
	waitForWaitGroup(t, &app.wg)
}

func TestStartRuntimeCleansUpMalformedRegistrationSuccess(t *testing.T) {
	cfg := newFreeLifecycleTestConfig(t)
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
	if portIsOpen(cfg.GetSbiBindingAddr()) || portIsOpen(cfg.GetAnlfServerBindingAddr()) {
		t.Fatal("owned listener opened after malformed registration success")
	}
}

func TestStartRuntimeContinuesAfterOAuth2RequiredRegistration(t *testing.T) {
	cfg := newFreeLifecycleTestConfig(t)
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
	if !portIsOpen(cfg.GetSbiBindingAddr()) || !portIsOpen(cfg.GetAnlfServerBindingAddr()) {
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
			cfg := newFreeLifecycleTestConfig(t)
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
}

func TestTerminateDeregistersWhileSbiIsReachable(t *testing.T) {
	cfg := newFreeLifecycleTestConfig(t)
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
	cfg := newFreeLifecycleTestConfig(t)
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

func newFreeLifecycleTestConfig(t *testing.T) *factory.Config {
	t.Helper()

	listeners := make([]net.Listener, 0, 3)
	ports := make([]int, 0, 3)
	for range 3 {
		listener, err := (&net.ListenConfig{}).Listen(
			context.Background(),
			"tcp",
			"127.0.0.1:0",
		)
		if err != nil {
			t.Fatalf("Listen() error = %v", err)
		}
		listeners = append(listeners, listener)
		ports = append(ports, listener.Addr().(*net.TCPAddr).Port)
	}
	for _, listener := range listeners {
		closeListener(t, listener)
	}

	return newLifecycleTestConfig(t, ports[0], ports[1], ports[2])
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
