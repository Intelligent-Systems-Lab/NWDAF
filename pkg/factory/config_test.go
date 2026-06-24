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
configuration: {}
`,
			checkConfig: func(t *testing.T, cfg *factory.Config) {
				t.Helper()
				if got := cfg.GetSbiBindingAddr(); got != "127.0.0.1:8080" {
					t.Fatalf("GetSbiBindingAddr() = %q, want %q", got, "127.0.0.1:8080")
				}
				if got := cfg.Configuration.SupportedAnalytics; len(got) != 1 || got[0] != "UE_COMMUNICATION" {
					t.Fatalf("SupportedAnalytics = %v, want [UE_COMMUNICATION]", got)
				}
			},
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
			name: "unsupported sbi scheme",
			yaml: `
configuration:
  sbi:
    scheme: https
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
			name: "input window larger than ring buffer",
			yaml: `
configuration:
  analytics:
    ueCommunication:
      inputWindow: 64
      ringBufferSize: 32
`,
			wantErr: "analytics.ueCommunication.inputWindow",
		},
		{
			name: "smf enabled requires endpoints and callback uris",
			yaml: `
configuration:
  smf:
    enabled: true
`,
			wantErr: "smf.endpoints",
		},
		{
			name: "ml service enabled requires endpoint",
			yaml: `
configuration:
  mlService:
    enabled: true
`,
			wantErr: "mlService.endpoint",
		},
		{
			name: "external mtlf enabled requires notif uri",
			yaml: `
configuration:
  externalMtlf:
    enabled: true
    endpoints:
      - http://127.0.0.1:8082
`,
			wantErr: "externalMtlf.notifUri",
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

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "nwdafcfg.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(content)), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return path
}
