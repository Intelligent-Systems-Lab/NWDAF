package service

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
)

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

func TestStartRuntimeRecordsOAuth2RequirementWithoutStartingListeners(t *testing.T) {
	cfg := newLifecycleTestConfig(t, takeFreePort(t), takeFreePort(t), takeFreePort(t))
	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}
	resourceURI := "http://nrf/nnrf-nfm/v1/nf-instances/id"
	fake := &fakeNFManagement{
		registerFn: func(context.Context) (consumer.RegistrationResult, error) {
			return consumer.RegistrationResult{
				ResourceURI:    resourceURI,
				OAuth2Required: true,
			}, consumer.ErrOAuth2Required
		},
	}
	app.nrfManagement = fake

	if startErr := app.startRuntime(); !errors.Is(startErr, consumer.ErrOAuth2Required) {
		t.Fatalf("startRuntime() error = %v, want OAuth2 requirement", startErr)
	} else if !strings.Contains(startErr.Error(), app.nwdafCtx.NfId) ||
		!strings.Contains(startErr.Error(), resourceURI) ||
		!strings.Contains(startErr.Error(), "NRF-side cleanup") {
		t.Fatalf("startRuntime() error = %v, want NF identity, resource URI, and cleanup action", startErr)
	}
	state := app.nwdafCtx.RegistrationState()
	if state.Registered || !state.OAuth2Required || state.ResourceURI != resourceURI {
		t.Fatalf("registration state = %+v", state)
	}
	if portIsOpen(cfg.GetSbiBindingAddr()) || portIsOpen(cfg.GetAnlfServerBindingAddr()) ||
		portIsOpen(cfg.GetMtlfServerBindingAddr()) {
		t.Fatal("owned listener opened after OAuth-required registration response")
	}
	if fake.deregisters != 0 {
		t.Fatalf("deregister calls = %d, want 0", fake.deregisters)
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
