package factory

import (
	"os"

	"gopkg.in/yaml.v3"

	"github.com/free5gc/nwdaf/internal/logger"
)

const (
	NwdafDefaultConfigPath     = "./config/nwdafcfg.yaml"
	NwdafSbiDefaultScheme      = "http"
	NwdafSbiDefaultIPv4        = "127.0.0.1"
	NwdafSbiDefaultPort        = 8080
	NwdafDefaultNwdafName      = "NWDAF"
	NwdafEventsSubResUriPrefix = "/nnwdaf-eventssubscription/v1"
)

var NwdafConfig *Config

type Config struct {
	Info          *Info          `yaml:"info"`
	Configuration *Configuration `yaml:"configuration"`
	Logger        *Logger        `yaml:"logger,omitempty"`
}

type Info struct {
	Version     string `yaml:"version,omitempty"`
	Description string `yaml:"description,omitempty"`
}

type Configuration struct {
	Mongodb            *Mongodb               `yaml:"mongodb,omitempty"`
	NwdafName          string                 `yaml:"nwdafName,omitempty"`
	Sbi                *Sbi                   `yaml:"sbi,omitempty"`
	NrfUri             string                 `yaml:"nrfUri,omitempty"`
	SupportedAnalytics []string               `yaml:"supportedAnalytics,omitempty"`
	Smf                *SmfConfig             `yaml:"smf,omitempty"`
	ExternalMtlf       *ExternalMtlfConfig    `yaml:"externalMtlf,omitempty"`
	MlService          *MlServiceConfig       `yaml:"mlService,omitempty"`
	GroupMembership    *GroupMembershipConfig `yaml:"groupMembership,omitempty"`
	Mtlf               *MtlfConfig            `yaml:"mtlf,omitempty"`
	Analytics          *AnalyticsConfig       `yaml:"analytics,omitempty"`
}

// GroupMembershipConfig maps Group IDs to SUPI lists (substitute for UDM)
// Per TS 23.502 §4.15.4.5.2: NWDAF should query UDM for group membership
// This config provides a static mapping when UDM is not available
type GroupMembershipConfig struct {
	Groups []GroupDefinition `yaml:"groups"`
}

// GroupDefinition defines a group and its member SUPIs
type GroupDefinition struct {
	GroupId string   `yaml:"groupId"`
	Supis   []string `yaml:"supis"`
}

// ExternalMtlfConfig configuration for External MTLF (ML Model Training Logical Function) integration
type ExternalMtlfConfig struct {
	Enabled   bool     `yaml:"enabled"`
	Endpoints []string `yaml:"endpoints,omitempty"`
	NotifUri  string   `yaml:"notifUri,omitempty"` // Callback URI for ML model notifications
}

// AnalyticsConfig holds per-analytics-type model parameters
type AnalyticsConfig struct {
	UeCommunication *ModelParams `yaml:"ueCommunication,omitempty"`
	// Future: AbnormalBehaviour *ModelParams `yaml:"abnormalBehaviour,omitempty"`
}

// ModelParams defines the ML model input/output window and data collection parameters
// for a specific analytics type.
type ModelParams struct {
	// SamplingInterval is the UPF report period in seconds.
	// Must match smf.subscriptionDuration / report-period (default: 10).
	SamplingInterval int `yaml:"samplingInterval,omitempty"`

	// InputWindow is the number of data points fed to the ML model as history.
	InputWindow int `yaml:"inputWindow,omitempty"`

	// OutputWindow is the number of future steps the ML model predicts.
	OutputWindow int `yaml:"outputWindow,omitempty"`

	// LookbackBuffer is extra seconds added to the MongoDB query window beyond
	// inputWindow×samplingInterval to absorb delivery jitter (default: samplingInterval).
	LookbackBuffer int `yaml:"lookbackBuffer,omitempty"`
}

// QueryLookback returns the computed time window to query from MongoDB:
// SamplingInterval × InputWindow (seconds).
func (m *ModelParams) QueryLookback() int {
	si := m.SamplingInterval
	if si <= 0 {
		si = 10 // default: 10s
	}
	iw := m.InputWindow
	if iw <= 0 {
		iw = 30 // default: 30 points
	}
	return si * iw
}

// SamplingIntervalOrDefault returns SamplingInterval with a fallback to 10s.
func (m *ModelParams) SamplingIntervalOrDefault() int {
	if m.SamplingInterval > 0 {
		return m.SamplingInterval
	}
	return 10
}

// InputWindowOrDefault returns InputWindow with a fallback to 30.
func (m *ModelParams) InputWindowOrDefault() int {
	if m.InputWindow > 0 {
		return m.InputWindow
	}
	return 30
}

// LookbackBufferOrDefault returns LookbackBuffer with a fallback to SamplingIntervalOrDefault.
func (m *ModelParams) LookbackBufferOrDefault() int {
	if m.LookbackBuffer > 0 {
		return m.LookbackBuffer
	}
	return m.SamplingIntervalOrDefault()
}

// OutputWindowOrDefault returns OutputWindow with a fallback to 5.
func (m *ModelParams) OutputWindowOrDefault() int {
	if m.OutputWindow > 0 {
		return m.OutputWindow
	}
	return 5
}

// MlServiceConfig configuration for external ML inference service
type MlServiceConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Endpoint string `yaml:"endpoint,omitempty"`
}

// MtlfConfig configuration for 1st-party MTLF / Daisy FL framework integration
type MtlfConfig struct {
	Enabled          bool                   `yaml:"enabled"`                    // Master switch for all Daisy FL features
	Endpoint         string                 `yaml:"endpoint,omitempty"`         // Master REST API
	TriggerOnStartup bool                   `yaml:"triggerOnStartup,omitempty"` // Trigger training on NWDAF startup
	TriggerDelay     int                    `yaml:"triggerDelay,omitempty"`     // Startup trigger delay (default: 30)
	StaticModelUrl   string                 `yaml:"staticModelUrl,omitempty"`   // Static URL for ML model
	Task             map[string]any         `yaml:"task,omitempty"`             // Task payload (mirrors task.json)
	AccuracyMonitor  *AccuracyMonitorConfig `yaml:"accuracyMonitor,omitempty"`  // Accuracy monitoring settings
}

// AccuracyMonitorConfig controls accuracy monitoring behavior
// Per TS 23.288 §5C: accuracy determined by comparing predictions against ground truth
type AccuracyMonitorConfig struct {
	Enabled            bool    `yaml:"enabled"`
	CheckInterval      int     `yaml:"checkInterval,omitempty"`      // Seconds between checks (default: 60)
	DeviationThreshold float64 `yaml:"deviationThreshold,omitempty"` // sMAPE retrain threshold in [0,2] (default: 0.3)
	MinSamples         int     `yaml:"minSamples,omitempty"`         // Min samples before evaluation (default: 5)
	WarmupDuration     int     `yaml:"warmupDuration,omitempty"`     // Seconds to skip checks after start (default: 120)

	// Trigger strategy: "consecutive" or "ema" (default: "consecutive")
	TriggerStrategy     string  `yaml:"triggerStrategy,omitempty"`
	ConsecutiveBreaches int     `yaml:"consecutiveBreaches,omitempty"` // Consecutive checks above threshold (default: 3)
	EmaAlpha            float64 `yaml:"emaAlpha,omitempty"`            // EMA smoothing factor 0-1 (default: 0.3)
}

// SmfConfig configuration for SMF data collection
type SmfConfig struct {
	Enabled              bool       `yaml:"enabled"`
	Endpoints            []string   `yaml:"endpoints,omitempty"`
	SubscriptionDuration int        `yaml:"subscriptionDuration,omitempty"` // seconds
	NotifUris            *NotifUris `yaml:"notifUris,omitempty"`
}

// NotifUris contains notification URIs for data collection callbacks
type NotifUris struct {
	Smf string `yaml:"smf,omitempty"` // Callback URI for SMF notifications
	Upf string `yaml:"upf,omitempty"` // Callback URI for UPF notifications
}

type Mongodb struct {
	Name string `yaml:"name"`
	Url  string `yaml:"url"`
}

type Sbi struct {
	Scheme       string `yaml:"scheme"`
	RegisterIPv4 string `yaml:"registerIPv4,omitempty"`
	BindingIPv4  string `yaml:"bindingIPv4,omitempty"`
	Port         int    `yaml:"port,omitempty"`
}

type Logger struct {
	Enable       bool   `yaml:"enable"`
	Level        string `yaml:"level"`
	ReportCaller bool   `yaml:"reportCaller"`
}

func ReadConfig(cfgPath string) (*Config, error) {
	if cfgPath == "" {
		cfgPath = NwdafDefaultConfigPath
	}

	cfg := &Config{}
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		logger.CfgLog.Errorf("Failed to read config file: %v", err)
		return nil, err
	}

	if err = yaml.Unmarshal(data, cfg); err != nil {
		logger.CfgLog.Errorf("Failed to parse config file: %v", err)
		return nil, err
	}

	// Set defaults
	if cfg.Configuration == nil {
		cfg.Configuration = &Configuration{}
	}
	if cfg.Configuration.NwdafName == "" {
		cfg.Configuration.NwdafName = NwdafDefaultNwdafName
	}
	if cfg.Configuration.Sbi == nil {
		cfg.Configuration.Sbi = &Sbi{
			Scheme:      NwdafSbiDefaultScheme,
			BindingIPv4: NwdafSbiDefaultIPv4,
			Port:        NwdafSbiDefaultPort,
		}
	}
	if len(cfg.Configuration.SupportedAnalytics) == 0 {
		cfg.Configuration.SupportedAnalytics = []string{"ABNORMAL_BEHAVIOUR"}
	}

	logger.CfgLog.Infof("Config loaded: %s", cfgPath)
	return cfg, nil
}

func (c *Config) GetSbiBindingAddr() string {
	if c.Configuration.Sbi == nil {
		return "127.0.0.1:8080"
	}
	return c.Configuration.Sbi.BindingIPv4 + ":" +
		string(rune(c.Configuration.Sbi.Port+'0'))
}

func (c *Config) GetSbiScheme() string {
	if c.Configuration.Sbi == nil {
		return "http"
	}
	return c.Configuration.Sbi.Scheme
}

// GetSamplingInterval returns the configured UE communication sampling interval,
// or 0 if not configured (caller should skip snapping).
func (c *Config) GetSamplingInterval() int {
	if c == nil || c.Configuration == nil ||
		c.Configuration.Analytics == nil ||
		c.Configuration.Analytics.UeCommunication == nil {
		return 0
	}
	return c.Configuration.Analytics.UeCommunication.SamplingIntervalOrDefault()
}

func (c *Config) GetNwdafName() string {
	if c.Configuration == nil {
		return NwdafDefaultNwdafName
	}
	return c.Configuration.NwdafName
}
