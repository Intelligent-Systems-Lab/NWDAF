package factory

import (
	"os"
	"strings"

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
	Adrf               *AdrfConfig            `yaml:"adrf,omitempty"`
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

	// RingBufferSize is the maximum number of UPF data points kept in memory per session.
	// Inference reads exclusively from this buffer; must be >= InputWindow (default: 50).
	RingBufferSize int `yaml:"ringBufferSize,omitempty"`
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

// LookbackBufferOrDefault returns LookbackBuffer with a fallback to 2×SamplingIntervalOrDefault.
func (m *ModelParams) LookbackBufferOrDefault() int {
	if m.LookbackBuffer > 0 {
		return m.LookbackBuffer
	}
	return 2 * m.SamplingIntervalOrDefault()
}

// OutputWindowOrDefault returns OutputWindow with a fallback to 5.
func (m *ModelParams) OutputWindowOrDefault() int {
	if m.OutputWindow > 0 {
		return m.OutputWindow
	}
	return 5
}

// RingBufferSizeOrDefault returns RingBufferSize with a fallback to 50.
func (m *ModelParams) RingBufferSizeOrDefault() int {
	if m.RingBufferSize > 0 {
		return m.RingBufferSize
	}
	return 50
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
	Enabled       bool `yaml:"enabled"`
	CheckInterval int  `yaml:"checkInterval,omitempty"` // Seconds between checks (default: 60)
	MinSamples    int  `yaml:"minSamples,omitempty"`    // Min samples before evaluation (default: 5)
	// Seconds to skip checks after start (default: 120).
	WarmupDuration int `yaml:"warmupDuration,omitempty"`
	// Candidate metrics retained for observability.
	MetricsToRecord []string `yaml:"metricsToRecord,omitempty"`
	// Metric used for retrain decision.
	PrimaryMetric string `yaml:"primaryMetric,omitempty"`
	// Per-scope history length.
	RecentBufferSize int `yaml:"recentBufferSize,omitempty"`
	// Buffer samples required before z-score gate.
	MinBufferSamples int `yaml:"minBufferSamples,omitempty"`
	// Standard-deviation floor for z-score.
	MinStd float64 `yaml:"minStd,omitempty"`
	// Degradation eligibility floor for the primary metric.
	FixedFloor float64 `yaml:"fixedFloor,omitempty"`
	// Relative anomaly threshold.
	ZScoreThreshold float64 `yaml:"zScoreThreshold,omitempty"`
	// Decision window length M.
	DecisionWindowSize int `yaml:"decisionWindowSize,omitempty"`
	// Required hits N in the latest M rounds.
	RequiredHitsInWindow int `yaml:"requiredHitsInWindow,omitempty"`
	// Scope state GC threshold in seconds.
	ScopeStateTTL int `yaml:"scopeStateTTL,omitempty"`
	// Backward-compatible fallback for strict consecutive behavior.
	ConsecutiveBreaches int                      `yaml:"consecutiveBreaches,omitempty"`
	DegradationPolicy   *DegradationPolicyConfig `yaml:"degradationPolicy,omitempty"`
	ChronicPolicy       *ChronicPolicyConfig     `yaml:"chronicPolicy,omitempty"`
	LowTrafficPolicy    *LowTrafficPolicyConfig  `yaml:"lowTrafficOverpredictionPolicy,omitempty"`
}

type DegradationPolicyConfig struct {
	MinDecisionTrafficScale float64 `yaml:"minDecisionTrafficScale,omitempty"`
}

type ChronicPolicyConfig struct {
	Enabled                 *bool   `yaml:"enabled,omitempty"`
	Metric                  string  `yaml:"metric,omitempty"`
	Aggregator              string  `yaml:"aggregator,omitempty"`
	Percentile              int     `yaml:"percentile,omitempty"`
	Threshold               float64 `yaml:"threshold,omitempty"`
	MinDecisionTrafficScale float64 `yaml:"minDecisionTrafficScale,omitempty"`
}

type LowTrafficPolicyConfig struct {
	Enabled                  *bool   `yaml:"enabled,omitempty"`
	MaxActualTrafficScale    float64 `yaml:"maxActualTrafficScale,omitempty"`
	MinPredictedTrafficScale float64 `yaml:"minPredictedTrafficScale,omitempty"`
	PredictionOvershootRatio float64 `yaml:"predictionOvershootRatio,omitempty"`
}

const chronicAggregatorPercentile = "percentile"

func (a *AccuracyMonitorConfig) MetricsToRecordOrDefault() []string {
	if a == nil || len(a.MetricsToRecord) == 0 {
		return []string{"sMAPE", "MAE", "MSE", "WAPE", "NRMSE"}
	}
	return append([]string(nil), a.MetricsToRecord...)
}

func (a *AccuracyMonitorConfig) PrimaryMetricOrDefault() string {
	if a == nil || a.PrimaryMetric == "" {
		return "MAE"
	}
	return a.PrimaryMetric
}

func (a *AccuracyMonitorConfig) RecentBufferSizeOrDefault() int {
	if a == nil || a.RecentBufferSize <= 0 {
		return 20
	}
	return a.RecentBufferSize
}

func (a *AccuracyMonitorConfig) MinBufferSamplesOrDefault() int {
	if a == nil || a.MinBufferSamples <= 0 {
		return 8
	}
	return a.MinBufferSamples
}

func (a *AccuracyMonitorConfig) MinStdOrDefault() float64 {
	if a == nil || a.MinStd <= 0 {
		return 0.01
	}
	return a.MinStd
}

func (a *AccuracyMonitorConfig) FixedFloorOrDefault() float64 {
	if a == nil || a.FixedFloor <= 0 {
		return 1024
	}
	return a.FixedFloor
}

func (d *DegradationPolicyConfig) MinDecisionTrafficScaleOrDefault() float64 {
	if d == nil || d.MinDecisionTrafficScale < 0 {
		return 0
	}
	return d.MinDecisionTrafficScale
}

func (a *AccuracyMonitorConfig) ZScoreThresholdOrDefault() float64 {
	if a == nil || a.ZScoreThreshold <= 0 {
		return 3.0
	}
	return a.ZScoreThreshold
}

func (a *AccuracyMonitorConfig) ScopeStateTTLOrDefault() int {
	if a == nil || a.ScopeStateTTL <= 0 {
		return 600
	}
	return a.ScopeStateTTL
}

func (a *AccuracyMonitorConfig) DecisionWindowSizeOrDefault() int {
	if a == nil {
		return 3
	}
	if a.DecisionWindowSize > 0 {
		return a.DecisionWindowSize
	}
	if a.ConsecutiveBreaches > 0 {
		return a.ConsecutiveBreaches
	}
	return 3
}

func (a *AccuracyMonitorConfig) RequiredHitsInWindowOrDefault() int {
	windowSize := a.DecisionWindowSizeOrDefault()
	required := 0
	switch {
	case a == nil:
		required = 3
	case a.RequiredHitsInWindow > 0:
		required = a.RequiredHitsInWindow
	case a.ConsecutiveBreaches > 0:
		required = a.ConsecutiveBreaches
	default:
		required = 3
	}
	if required > windowSize {
		return windowSize
	}
	return required
}

func (a *AccuracyMonitorConfig) ConsecutiveBreachesOrDefault() int {
	if a == nil || a.ConsecutiveBreaches <= 0 {
		return 3
	}
	return a.ConsecutiveBreaches
}

func (c *ChronicPolicyConfig) EnabledOrDefault() bool {
	if c == nil || c.Enabled == nil {
		return false
	}
	return *c.Enabled
}

func (c *ChronicPolicyConfig) MetricOrDefault() string {
	if c == nil {
		return "WAPE"
	}
	switch strings.ToUpper(strings.TrimSpace(c.Metric)) {
	case "WAPE", "NRMSE", "MAE":
		return strings.ToUpper(strings.TrimSpace(c.Metric))
	default:
		return "WAPE"
	}
}

func (c *ChronicPolicyConfig) AggregatorOrDefault() string {
	if c == nil {
		return chronicAggregatorPercentile
	}
	switch strings.ToLower(strings.TrimSpace(c.Aggregator)) {
	case "mean", chronicAggregatorPercentile:
		return strings.ToLower(strings.TrimSpace(c.Aggregator))
	default:
		return chronicAggregatorPercentile
	}
}

func (c *ChronicPolicyConfig) PercentileOrDefault() int {
	if c == nil || c.Percentile <= 0 {
		return 75
	}
	if c.Percentile > 99 {
		return 99
	}
	return c.Percentile
}

func (c *ChronicPolicyConfig) ThresholdOrDefault() float64 {
	if c == nil || c.Threshold <= 0 {
		return 1.0
	}
	return c.Threshold
}

func (c *ChronicPolicyConfig) MinDecisionTrafficScaleOrDefault() float64 {
	if c == nil {
		return 1024
	}
	if c.MinDecisionTrafficScale > 0 {
		return c.MinDecisionTrafficScale
	}
	return 1024
}

func (c *LowTrafficPolicyConfig) EnabledOrDefault() bool {
	if c == nil || c.Enabled == nil {
		return false
	}
	return *c.Enabled
}

func (c *LowTrafficPolicyConfig) MaxActualTrafficScaleOrDefault() float64 {
	if c == nil || c.MaxActualTrafficScale <= 0 {
		return 1024
	}
	return c.MaxActualTrafficScale
}

func (c *LowTrafficPolicyConfig) MinPredictedTrafficScaleOrDefault() float64 {
	if c == nil || c.MinPredictedTrafficScale <= 0 {
		return 4096
	}
	return c.MinPredictedTrafficScale
}

func (c *LowTrafficPolicyConfig) PredictionOvershootRatioOrDefault() float64 {
	if c == nil || c.PredictionOvershootRatio <= 1.0 {
		return 4.0
	}
	return c.PredictionOvershootRatio
}

// AdrfConfig holds connection settings for the ADRF (Analytics Data Repository Function).
// Per TS 29.575: ADRF stores and retrieves analytics/data records.
// StorageThreshold is used by the processor ADRF buffer.
// FetchBatchSize, RetrainWindow, WatchdogTimeout are used by MTLF retrieval (Phase E3).
type AdrfConfig struct {
	Url              string `yaml:"url,omitempty"`
	StorageThreshold int    `yaml:"storageThreshold,omitempty"` // default: 1
	// default: 1; ADRF V0 accepts exactly 1 fetch-correlation-id per GET — do not set above 1
	FetchBatchSize  int `yaml:"fetchBatchSize,omitempty"`
	RetrainWindow   int `yaml:"retrainWindow,omitempty"`   // default: 1800 (seconds of history to fetch)
	WatchdogTimeout int `yaml:"watchdogTimeout,omitempty"` // default: 120 (seconds after last callback)
}

func (a *AdrfConfig) AdrfEnabled() bool {
	return a != nil && a.Url != ""
}

func (a *AdrfConfig) StorageThresholdOrDefault() int {
	if a == nil || a.StorageThreshold <= 0 {
		return 1
	}
	return a.StorageThreshold
}

func (a *AdrfConfig) FetchBatchSizeOrDefault() int {
	if a == nil || a.FetchBatchSize <= 0 {
		return 1
	}
	return a.FetchBatchSize
}

func (a *AdrfConfig) RetrainWindowOrDefault() int {
	if a == nil || a.RetrainWindow <= 0 {
		return 1800
	}
	return a.RetrainWindow
}

func (a *AdrfConfig) WatchdogTimeoutOrDefault() int {
	if a == nil || a.WatchdogTimeout <= 0 {
		return 120
	}
	return a.WatchdogTimeout
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

	// Validate ring buffer vs input window
	if cfg.Configuration.Analytics != nil && cfg.Configuration.Analytics.UeCommunication != nil {
		p := cfg.Configuration.Analytics.UeCommunication
		iw := p.InputWindowOrDefault()
		rb := p.RingBufferSizeOrDefault()
		if iw > rb {
			logger.CfgLog.Warnf(
				"analytics.ueCommunication.inputWindow (%d) > ringBufferSize (%d): "+
					"inference will always have fewer points than the model expects; "+
					"increase ringBufferSize to at least %d",
				iw, rb, iw,
			)
		}
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

// GetRingBufferSize returns the configured in-memory ring buffer size for UE communication,
// or the default (50) if not configured.
func (c *Config) GetRingBufferSize() int {
	if c == nil || c.Configuration == nil ||
		c.Configuration.Analytics == nil ||
		c.Configuration.Analytics.UeCommunication == nil {
		return 50
	}
	return c.Configuration.Analytics.UeCommunication.RingBufferSizeOrDefault()
}

func (c *Config) GetNwdafName() string {
	if c.Configuration == nil {
		return NwdafDefaultNwdafName
	}
	return c.Configuration.NwdafName
}
