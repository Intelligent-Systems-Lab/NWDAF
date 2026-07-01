package service

import (
	"context"
	"net"
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
