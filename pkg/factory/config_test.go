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
			name: "missing nrf uri",
			yaml: `
configuration: {}
`,
			wantErr: "nrfUri is required",
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
			name: "smf enabled requires endpoint source",
			yaml: `
configuration:
  smf:
    enabled: true
`,
			wantErr: "smf.endpointSource is required",
		},
		{
			name: "smf configured source requires endpoints",
			yaml: `
configuration:
  smf:
    enabled: true
    endpointSource: configured
    notifUris:
      smf: http://127.0.0.1:8080/collector/notify
      upf: http://127.0.0.1:8080/collector/upf-notify
`,
			wantErr: "smf.endpoints",
		},
		{
			name: "smf configured source validates endpoints",
			yaml: `
configuration:
  smf:
    enabled: true
    endpointSource: configured
    endpoints:
      - not-a-url
    notifUris:
      smf: http://127.0.0.1:8080/collector/notify
      upf: http://127.0.0.1:8080/collector/upf-notify
`,
			wantErr: "smf.endpoints[0]",
		},
		{
			name: "smf nrf source does not require endpoints",
			yaml: `
configuration:
  nrfUri: http://127.0.0.10:8000
  smf:
    enabled: true
    endpointSource: nrf
    notifUris:
      smf: http://127.0.0.1:8080/collector/notify
      upf: http://127.0.0.1:8080/collector/upf-notify
`,
		},
		{
			name: "smf nrf source ignores legacy endpoints",
			yaml: `
configuration:
  nrfUri: http://127.0.0.10:8000
  smf:
    enabled: true
    endpointSource: nrf
    endpoints:
      - not-a-url
    notifUris:
      smf: http://127.0.0.1:8080/collector/notify
      upf: http://127.0.0.1:8080/collector/upf-notify
`,
		},
		{
			name: "smf rejects unknown endpoint source",
			yaml: `
configuration:
  smf:
    enabled: true
    endpointSource: merged
    notifUris:
      smf: http://127.0.0.1:8080/collector/notify
      upf: http://127.0.0.1:8080/collector/upf-notify
`,
			wantErr: "smf.endpointSource must be",
		},
		{
			name: "owned collector callback scheme must match sbi scheme",
			yaml: `
configuration:
  sbi:
    scheme: https
    tls:
      pem: cert/nwdaf.pem
      key: cert/nwdaf.key
  smf:
    enabled: true
    endpointSource: configured
    endpoints:
      - http://127.0.0.1:8081
    notifUris:
      smf: http://127.0.0.1:8080/collector/notify
      upf: https://127.0.0.1:8080/collector/upf-notify
`,
			wantErr: "smf.notifUris.smf scheme must match sbi.scheme (https)",
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
			name: "external mtlf enabled accepts endpoint-only config",
			yaml: `
configuration:
  nrfUri: http://127.0.0.10:8000
  externalMtlf:
    enabled: true
    endpoints:
      - http://127.0.0.1:8082
`,
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
		{
			name: "adrf fetch batch larger than supported",
			yaml: `
configuration:
  adrf:
    url: http://127.0.0.1:9888
    fetchBatchSize: 2
`,
			wantErr: "adrf.fetchBatchSize",
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

func TestSampleConfigUsesNrfSmfEndpointSource(t *testing.T) {
	cfg, err := factory.ReadConfig(filepath.Join("..", "..", "config", "nwdafcfg.yaml"))
	if err != nil {
		t.Fatalf("ReadConfig(sample) error = %v", err)
	}
	if cfg.Configuration.Smf == nil {
		t.Fatal("sample SMF config is nil")
	}
	if got := cfg.Configuration.Smf.EndpointSource; got != factory.SmfEndpointSourceNRF {
		t.Fatalf("sample smf.endpointSource = %q, want %q", got, factory.SmfEndpointSourceNRF)
	}
	if len(cfg.Configuration.Smf.Endpoints) != 0 {
		t.Fatalf("sample smf.endpoints = %v, want none in NRF mode", cfg.Configuration.Smf.Endpoints)
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
