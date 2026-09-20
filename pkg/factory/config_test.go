package factory_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/free5gc/nwdaf/pkg/factory"
)

func TestReadConfigValidationMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		yaml        string
		wantErr     string
		checkConfig func(t *testing.T, cfg *factory.Config)
	}{
		{
			name: "valid minimal configuration defaults",
			yaml: `
configuration:
  nrfUri: http://127.0.0.10:8000/
  nrfCertPem: "  cert/nrf.pem  "
`,
			checkConfig: func(t *testing.T, cfg *factory.Config) {
				t.Helper()
				if got := cfg.GetSbiBindingAddr(); got != "127.0.0.1:8080" {
					t.Fatalf("GetSbiBindingAddr() = %q, want %q", got, "127.0.0.1:8080")
				}
				if got := cfg.GetAnlfServerURI(); got != "http://127.0.0.1:8090" {
					t.Fatalf("GetAnlfServerURI() = %q, want %q", got, "http://127.0.0.1:8090")
				}
				if got := cfg.GetMtlfServerURI(); got != "http://127.0.0.1:8091" {
					t.Fatalf("GetMtlfServerURI() = %q, want %q", got, "http://127.0.0.1:8091")
				}
				if got := cfg.Configuration.SupportedAnalytics; len(got) != 1 || got[0] != "UE_COMMUNICATION" {
					t.Fatalf("SupportedAnalytics = %v, want [UE_COMMUNICATION]", got)
				}
				if got := cfg.GetNrfUri(); got != "http://127.0.0.10:8000" {
					t.Fatalf("GetNrfUri() = %q, want %q", got, "http://127.0.0.10:8000")
				}
				if got := cfg.GetNrfCertPem(); got != "cert/nrf.pem" {
					t.Fatalf("GetNrfCertPem() = %q, want %q", got, "cert/nrf.pem")
				}
			},
		},
		{
			name: "disabled registration permits no nrf uri",
			yaml: `
configuration:
  nrfRegistrationEnabled: false
`,
			checkConfig: func(t *testing.T, cfg *factory.Config) {
				t.Helper()
				if cfg.NrfRegistrationEnabled() {
					t.Fatal("NRF registration should be disabled")
				}
				if cfg.GetNrfUri() != "" {
					t.Fatalf("GetNrfUri() = %q, want empty", cfg.GetNrfUri())
				}
			},
		},
		{
			name: "nrf uri rejects service path",
			yaml: `
configuration:
  nrfUri: http://127.0.0.10:8000/nnrf-nfm/v1
`,
			wantErr: "nrfUri must not include a path",
		},
		{
			name: "nrf uri rejects query",
			yaml: `
configuration:
  nrfUri: http://127.0.0.10:8000?target=nrf
`,
			wantErr: "nrfUri must not include a query",
		},
		{
			name: "nrf uri rejects unsupported scheme",
			yaml: `
configuration:
  nrfUri: ftp://127.0.0.10:8000
`,
			wantErr: "nrfUri must use http or https",
		},
		{
			name: "nrf uri rejects zero port",
			yaml: `
configuration:
  nrfUri: http://127.0.0.10:0
`,
			wantErr: "nrfUri port must be between 1 and 65535",
		},
		{
			name: "nrf uri rejects out of range port",
			yaml: `
configuration:
  nrfUri: http://127.0.0.10:65536
`,
			wantErr: "nrfUri port must be between 1 and 65535",
		},
		{
			name: "nrf uri rejects nonnumeric port",
			yaml: `
configuration:
  nrfUri: http://127.0.0.10:nrf
`,
			wantErr: "nrfUri must be a valid URL",
		},
		{
			name: "missing configuration section",
			yaml: `
info:
  version: 1.0.0
`,
			wantErr: "configuration section is required",
		},
		{
			name: "unsupported analytics value",
			yaml: `
configuration:
  supportedAnalytics:
    - ABNORMAL_BEHAVIOUR
`,
			wantErr: `supportedAnalytics[0]`,
		},
		{
			name: "experimental image training analytics profile",
			yaml: `
configuration:
  nrfUri: http://127.0.0.10:8000
  nwdafInfo:
    mlAnalyticsList:
      - mlAnalyticsIds: [X_IMAGE_CLASSIFICATION]
        mlModelInterInfo:
          vendorList: ["001122"]
`,
			checkConfig: func(t *testing.T, cfg *factory.Config) {
				t.Helper()
				entries := cfg.GetNwdafInfo().MLAnalyticsList
				if len(entries) != 1 || len(entries[0].MLAnalyticsIDs) != 1 ||
					string(entries[0].MLAnalyticsIDs[0]) != factory.NwdafExperimentalEventImageClass {
					t.Fatalf("ML analytics entries = %#v", entries)
				}
			},
		},
		{
			name: "https sbi requires tls paths",
			yaml: `
configuration:
  nrfUri: https://127.0.0.10:8000
  sbi:
    scheme: https
`,
			wantErr: "sbi.tls",
		},
		{
			name: "https sbi accepts free5gc style tls config",
			yaml: `
configuration:
  nrfUri: https://127.0.0.10:8000
  sbi:
    scheme: https
    tls:
      pem: cert/nwdaf.pem
      key: cert/nwdaf.key
`,
			checkConfig: func(t *testing.T, cfg *factory.Config) {
				t.Helper()
				if got := cfg.GetSbiUri(); got != "https://127.0.0.1:8080" {
					t.Fatalf("GetSbiUri() = %q, want %q", got, "https://127.0.0.1:8080")
				}
				if got := cfg.GetCertPemPath(); got != "cert/nwdaf.pem" {
					t.Fatalf("GetCertPemPath() = %q, want %q", got, "cert/nwdaf.pem")
				}
				if got := cfg.GetCertKeyPath(); got != "cert/nwdaf.key" {
					t.Fatalf("GetCertKeyPath() = %q, want %q", got, "cert/nwdaf.key")
				}
			},
		},
		{
			name: "unsupported sbi scheme",
			yaml: `
configuration:
  sbi:
    scheme: ftp
`,
			wantErr: "sbi.scheme",
		},
		{
			name: "invalid sbi port",
			yaml: `
configuration:
  sbi:
    port: -1
`,
			wantErr: "sbi.port",
		},
		{
			name: "anlf backend enabled requires endpoint",
			yaml: `
configuration:
  anlfBackend:
    enabled: true
`,
			wantErr: "anlfBackend.endpoint",
		},
		{
			name: "sbi wildcard binding requires register ip",
			yaml: `
configuration:
  nrfUri: http://127.0.0.10:8000
  sbi:
    bindingIPv4: 0.0.0.0
`,
			wantErr: "sbi.registerIPv4",
		},
		{
			name: "sbi register ip rejects wildcard",
			yaml: `
configuration:
  nrfUri: http://127.0.0.10:8000
  sbi:
    bindingIPv4: 127.0.0.1
    registerIPv4: 0.0.0.0
`,
			wantErr: "valid non-wildcard IPv4 address",
		},
		{
			name: "sbi register ip rejects hostname",
			yaml: `
configuration:
  nrfUri: http://127.0.0.10:8000
  sbi:
    bindingIPv4: 0.0.0.0
    registerIPv4: nwdaf.example.com
`,
			wantErr: "valid non-wildcard IPv4 address",
		},
		{
			name: "sbi register ip rejects ipv6",
			yaml: `
configuration:
  nrfUri: http://127.0.0.10:8000
  sbi:
    bindingIPv4: "::"
    registerIPv4: "2001:db8::10"
`,
			wantErr: "valid non-wildcard IPv4 address",
		},
		{
			name: "anlf wildcard binding requires register ip",
			yaml: `
configuration:
  anlf:
    server:
      bindingIPv4: 0.0.0.0
`,
			wantErr: "anlf.server.registerIPv4",
		},
		{
			name: "mtlf wildcard binding requires register ip",
			yaml: `
configuration:
  mtlf:
    server:
      bindingIPv4: 0.0.0.0
`,
			wantErr: "mtlf.server.registerIPv4",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfgPath := writeTempConfig(t, tt.yaml)
			cfg, err := factory.ReadConfig(cfgPath)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("ReadConfig() error = nil, want substring %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ReadConfig() error = %q, want substring %q", err.Error(), tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("ReadConfig() unexpected error: %v", err)
			}
			if tt.checkConfig != nil {
				tt.checkConfig(t, cfg)
			}
		})
	}
}

func TestConfigSbiGetters(t *testing.T) {
	t.Parallel()

	cfg := &factory.Config{
		Configuration: &factory.Configuration{
			Sbi: &factory.Sbi{
				Scheme:       "http",
				BindingIPv4:  "0.0.0.0",
				RegisterIPv4: "192.168.1.10",
				Port:         8080,
				Tls: &factory.Tls{
					Pem: "cert/nwdaf.pem",
					Key: "cert/nwdaf.key",
				},
			},
		},
	}

	if got := cfg.GetSbiBindingAddr(); got != "0.0.0.0:8080" {
		t.Fatalf("GetSbiBindingAddr() = %q, want %q", got, "0.0.0.0:8080")
	}
	if got := cfg.GetSbiRegisterAddr(); got != "192.168.1.10:8080" {
		t.Fatalf("GetSbiRegisterAddr() = %q, want %q", got, "192.168.1.10:8080")
	}
	if got := cfg.GetSbiUri(); got != "http://192.168.1.10:8080" {
		t.Fatalf("GetSbiUri() = %q, want %q", got, "http://192.168.1.10:8080")
	}
	if got := cfg.GetCertPemPath(); got != "cert/nwdaf.pem" {
		t.Fatalf("GetCertPemPath() = %q, want %q", got, "cert/nwdaf.pem")
	}
	if got := cfg.GetCertKeyPath(); got != "cert/nwdaf.key" {
		t.Fatalf("GetCertKeyPath() = %q, want %q", got, "cert/nwdaf.key")
	}
}

func TestConfigAuxiliaryServerGetters(t *testing.T) {
	t.Parallel()

	cfg := &factory.Config{
		Configuration: &factory.Configuration{
			Anlf: &factory.AnlfConfig{
				Server: &factory.AuxiliaryServerConfig{
					BindingIPv4:  "0.0.0.0",
					RegisterIPv4: "192.168.1.20",
					Port:         8090,
				},
			},
			Mtlf: &factory.MtlfConfig{
				Server: &factory.AuxiliaryServerConfig{
					BindingIPv4:  "127.0.0.9",
					RegisterIPv4: "192.168.1.21",
					Port:         8091,
				},
			},
		},
	}

	if got := cfg.GetAnlfServerBindingAddr(); got != "0.0.0.0:8090" {
		t.Fatalf("GetAnlfServerBindingAddr() = %q, want %q", got, "0.0.0.0:8090")
	}
	if got := cfg.GetAnlfServerURI(); got != "http://192.168.1.20:8090" {
		t.Fatalf("GetAnlfServerURI() = %q, want %q", got, "http://192.168.1.20:8090")
	}
	if got := cfg.GetMtlfServerBindingAddr(); got != "127.0.0.9:8091" {
		t.Fatalf("GetMtlfServerBindingAddr() = %q, want %q", got, "127.0.0.9:8091")
	}
	if got := cfg.GetMtlfServerURI(); got != "http://192.168.1.21:8091" {
		t.Fatalf("GetMtlfServerURI() = %q, want %q", got, "http://192.168.1.21:8091")
	}
}

func TestConfigSbiGettersFallbackRegisterIP(t *testing.T) {
	t.Parallel()

	cfg := &factory.Config{
		Configuration: &factory.Configuration{
			Sbi: &factory.Sbi{
				BindingIPv4: "127.0.0.9",
				Port:        9090,
			},
		},
	}

	if got := cfg.GetSbiRegisterIP(); got != "127.0.0.9" {
		t.Fatalf("GetSbiRegisterIP() = %q, want %q", got, "127.0.0.9")
	}
	if got := cfg.GetSbiUri(); got != "http://127.0.0.9:9090" {
		t.Fatalf("GetSbiUri() = %q, want %q", got, "http://127.0.0.9:9090")
	}
}

func TestMtlfBackendConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  string
		wantErr string
		check   func(*testing.T, *factory.MtlfBackendConfig)
	}{
		{
			name: "omitted preserves disabled behavior",
			config: `
configuration:
  nrfUri: http://127.0.0.10:8000
`,
			check: func(t *testing.T, backend *factory.MtlfBackendConfig) {
				t.Helper()
				if backend != nil {
					t.Fatalf("MtlfBackend = %#v, want nil", backend)
				}
			},
		},
		{
			name: "disabled does not require endpoint",
			config: `
configuration:
  nrfUri: http://127.0.0.10:8000
  mtlfBackend:
    enabled: false
`,
			check: func(t *testing.T, backend *factory.MtlfBackendConfig) {
				t.Helper()
				if backend == nil || backend.Enabled {
					t.Fatalf("MtlfBackend = %#v, want disabled config", backend)
				}
			},
		},
		{
			name: "enabled normalizes endpoint",
			config: `
configuration:
  nrfUri: http://127.0.0.10:8000
  mtlfBackend:
    enabled: true
    endpoint: " HTTP://127.0.0.1:9092/ "
    requestTimeout: 5
`,
			check: func(t *testing.T, backend *factory.MtlfBackendConfig) {
				t.Helper()
				if backend.Endpoint != "http://127.0.0.1:9092" {
					t.Fatalf("Endpoint = %q", backend.Endpoint)
				}
			},
		},
		{
			name: "enabled requires endpoint",
			config: `
configuration:
  nrfUri: http://127.0.0.10:8000
  mtlfBackend:
    enabled: true
    requestTimeout: 5
`,
			wantErr: "mtlfBackend.endpoint is required",
		},
		{
			name: "enabled rejects negative timeout",
			config: `
configuration:
  nrfUri: http://127.0.0.10:8000
  mtlfBackend:
    enabled: true
    endpoint: http://127.0.0.1:9092
    requestTimeout: -1
`,
			wantErr: "mtlfBackend.requestTimeout must be positive",
		},
		{
			name: "enabled rejects endpoint path",
			config: `
configuration:
  nrfUri: http://127.0.0.10:8000
  mtlfBackend:
    enabled: true
    endpoint: http://127.0.0.1:9092/internal
    requestTimeout: 5
`,
			wantErr: "must not include a path",
		},
		{
			name: "enabled rejects endpoint query",
			config: `
configuration:
  nrfUri: http://127.0.0.10:8000
  mtlfBackend:
    enabled: true
    endpoint: http://127.0.0.1:9092?mode=test
    requestTimeout: 5
`,
			wantErr: "must not include a query or fragment",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeTempConfig(t, test.config)
			cfg, err := factory.ReadConfig(path)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("ReadConfig() error = %v, want containing %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ReadConfig() error = %v", err)
			}
			test.check(t, cfg.Configuration.MtlfBackend)
		})
	}
}

func TestAnlfBackendConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  string
		wantErr string
		check   func(*testing.T, *factory.AnlfBackendConfig)
	}{
		{
			name: "omitted preserves disabled behavior",
			config: `
configuration:
  nrfUri: http://127.0.0.10:8000
`,
			check: func(t *testing.T, backend *factory.AnlfBackendConfig) {
				t.Helper()
				if backend != nil {
					t.Fatalf("AnlfBackend = %#v, want nil", backend)
				}
			},
		},
		{
			name: "disabled does not require endpoint",
			config: `
configuration:
  nrfUri: http://127.0.0.10:8000
  anlfBackend:
    enabled: false
`,
			check: func(t *testing.T, backend *factory.AnlfBackendConfig) {
				t.Helper()
				if backend == nil || backend.Enabled {
					t.Fatalf("AnlfBackend = %#v, want disabled config", backend)
				}
			},
		},
		{
			name: "enabled normalizes endpoint and defaults timeout",
			config: `
configuration:
  nrfUri: http://127.0.0.10:8000
  anlfBackend:
    enabled: true
    endpoint: " HTTP://127.0.0.1:9093/ "
`,
			check: func(t *testing.T, backend *factory.AnlfBackendConfig) {
				t.Helper()
				if backend.Endpoint != "http://127.0.0.1:9093" || backend.RequestTimeout != 5 {
					t.Fatalf("AnlfBackend = %#v", backend)
				}
			},
		},
		{
			name: "enabled requires endpoint",
			config: `
configuration:
  nrfUri: http://127.0.0.10:8000
  anlfBackend:
    enabled: true
`,
			wantErr: "anlfBackend.endpoint is required",
		},
		{
			name: "enabled rejects negative timeout",
			config: `
configuration:
  nrfUri: http://127.0.0.10:8000
  anlfBackend:
    enabled: true
    endpoint: http://127.0.0.1:9093
    requestTimeout: -1
`,
			wantErr: "anlfBackend.requestTimeout must be positive",
		},
		{
			name: "enabled rejects endpoint path",
			config: `
configuration:
  nrfUri: http://127.0.0.10:8000
  anlfBackend:
    enabled: true
    endpoint: http://127.0.0.1:9093/internal
`,
			wantErr: "must not include a path",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeTempConfig(t, test.config)
			cfg, err := factory.ReadConfig(path)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("ReadConfig() error = %v, want containing %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ReadConfig() error = %v", err)
			}
			test.check(t, cfg.Configuration.AnlfBackend)
		})
	}
}

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "nwdafcfg.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(content)), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return path
}
