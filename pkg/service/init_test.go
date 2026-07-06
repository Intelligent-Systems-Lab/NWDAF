package service

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/free5gc/nwdaf/pkg/factory"
)

func TestStartOwnedServersStartsAndStopsAllListeners(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	cfg := newLifecycleTestConfig(t, takeFreePort(t), takeFreePort(t), takeFreePort(t))
	cfg.Configuration.Sbi.Scheme = "ftp"

	app, err := NewApp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}

	startErr := app.startOwnedServers()
	if startErr == nil {
		app.stopOwnedServers()
		t.Fatal("startOwnedServers() error = nil, want unsupported scheme failure")
	}

	assertPortClosedEventually(t, cfg.GetAnlfServerBindingAddr())
	assertPortClosedEventually(t, cfg.GetMtlfServerBindingAddr())
	waitForWaitGroup(t, &app.wg)
}

func newLifecycleTestConfig(t *testing.T, sbiPort, anlfPort, mtlfPort int) *factory.Config {
	t.Helper()

	return &factory.Config{
		Configuration: &factory.Configuration{
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
