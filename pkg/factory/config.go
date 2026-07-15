package factory

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/free5gc/nwdaf/internal/logger"
)

const (
	NwdafDefaultConfigPath      = "./config/nwdafcfg.yaml"
	NwdafSbiDefaultScheme       = "http"
	NwdafSbiTLSScheme           = "https"
	NwdafSbiDefaultIPv4         = "127.0.0.1"
	NwdafSbiDefaultPort         = 8080
	NwdafAnlfDefaultPort        = 8090
	NwdafMtlfDefaultPort        = 8091
	NwdafDefaultNwdafName       = "NWDAF"
	NwdafEventsSubResUriPrefix  = "/nnwdaf-eventssubscription/v1"
	NwdafSupportedEventUEComm   = "UE_COMMUNICATION"
	SmfEndpointSourceNRF        = "nrf"
	SmfEndpointSourceConfigured = "configured"
)

var NwdafConfig *Config

var supportedAnalyticsAllowlist = map[string]struct{}{
	NwdafSupportedEventUEComm: {},
}

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
	NrfCertPem         string                 `yaml:"nrfCertPem,omitempty"`
	SupportedAnalytics []string               `yaml:"supportedAnalytics,omitempty"`
	Smf                *SmfConfig             `yaml:"smf,omitempty"`
	Anlf               *AnlfConfig            `yaml:"anlf,omitempty"`
	ExternalMtlf       *ExternalMtlfConfig    `yaml:"externalMtlf,omitempty"`
	AnlfBackend        *AnlfBackendConfig     `yaml:"anlfBackend,omitempty"`
	GroupMembership    *GroupMembershipConfig `yaml:"groupMembership,omitempty"`
	Mtlf               *MtlfConfig            `yaml:"mtlf,omitempty"`
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
}

type AnlfConfig struct {
	Server *AuxiliaryServerConfig `yaml:"server,omitempty"`
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

// AnlfBackendConfig configures the downstream AnLF backend used by NWDAF.
type AnlfBackendConfig struct {
	Enabled             bool                       `yaml:"enabled"`
	Endpoint            string                     `yaml:"endpoint,omitempty"`
	ObservationDelivery *ObservationDeliveryConfig `yaml:"observationDelivery,omitempty"`
}

type ObservationDeliveryConfig struct {
	QueueCapacity  int `yaml:"queueCapacity,omitempty"`
	RequestTimeout int `yaml:"requestTimeout,omitempty"`
	MaxRetries     int `yaml:"maxRetries,omitempty"`
	RetryInterval  int `yaml:"retryInterval,omitempty"`
}

func (c *ObservationDeliveryConfig) QueueCapacityOrDefault() int {
	if c != nil && c.QueueCapacity > 0 {
		return c.QueueCapacity
	}
	return 1024
}

func (c *ObservationDeliveryConfig) RequestTimeoutOrDefault() int {
	if c != nil && c.RequestTimeout > 0 {
		return c.RequestTimeout
	}
	return 5
}

func (c *ObservationDeliveryConfig) MaxRetriesOrDefault() int {
	if c != nil && c.MaxRetries > 0 {
		return c.MaxRetries
	}
	return 3
}

func (c *ObservationDeliveryConfig) RetryIntervalOrDefault() int {
	if c != nil && c.RetryInterval > 0 {
		return c.RetryInterval
	}
	return 1
}

// MtlfConfig configuration for 1st-party MTLF / Daisy FL framework integration
type MtlfConfig struct {
	Enabled          bool                   `yaml:"enabled"`                    // Master switch for all Daisy FL features
	Server           *AuxiliaryServerConfig `yaml:"server,omitempty"`           // Auxiliary inbound callback server
	Endpoint         string                 `yaml:"endpoint,omitempty"`         // Master REST API
	TriggerOnStartup bool                   `yaml:"triggerOnStartup,omitempty"` // Trigger training on NWDAF startup
	TriggerDelay     int                    `yaml:"triggerDelay,omitempty"`     // Startup trigger delay (default: 30)
	Task             map[string]any         `yaml:"task,omitempty"`             // Task payload (mirrors task.json)
	AccuracyPolicy   *AccuracyMonitorConfig `yaml:"accuracyPolicy,omitempty"`   // MTLF accuracy decision policy
	ModelProvider    *ModelProviderConfig   `yaml:"modelProvider,omitempty"`    // Stable local model identity
}

type ModelProviderConfig struct {
	ProviderID             string `yaml:"providerId"`
	BootstrapModelUniqueID int64  `yaml:"bootstrapModelUniqueId"`
}

// AccuracyMonitorConfig controls how MTLF evaluates accuracy reports from AnLF.
type AccuracyMonitorConfig struct {
	Enabled bool `yaml:"enabled"`
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
	EndpointSource       string     `yaml:"endpointSource,omitempty"`
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
	Tls          *Tls   `yaml:"tls,omitempty"`
}

type Tls struct {
	Pem string `yaml:"pem,omitempty"`
	Key string `yaml:"key,omitempty"`
}

type AuxiliaryServerConfig struct {
	RegisterIPv4 string `yaml:"registerIPv4,omitempty"`
	BindingIPv4  string `yaml:"bindingIPv4,omitempty"`
	Port         int    `yaml:"port,omitempty"`
}

type Logger struct {
	Enable       bool   `yaml:"enable"`
	Level        string `yaml:"level"`
	ReportCaller bool   `yaml:"reportCaller"`
}

func (c *Config) applyDefaults() {
	if c.Configuration == nil {
		c.Configuration = &Configuration{}
	}

	if c.Configuration.NwdafName == "" {
		c.Configuration.NwdafName = NwdafDefaultNwdafName
	}

	if c.Configuration.Sbi == nil {
		c.Configuration.Sbi = &Sbi{}
	}
	if c.Configuration.Sbi.Scheme == "" {
		c.Configuration.Sbi.Scheme = NwdafSbiDefaultScheme
	}
	if c.Configuration.Sbi.BindingIPv4 == "" {
		c.Configuration.Sbi.BindingIPv4 = NwdafSbiDefaultIPv4
	}
	if c.Configuration.Sbi.Port == 0 {
		c.Configuration.Sbi.Port = NwdafSbiDefaultPort
	}
	if c.Configuration.Anlf == nil {
		c.Configuration.Anlf = &AnlfConfig{}
	}
	if c.Configuration.Anlf.Server == nil {
		c.Configuration.Anlf.Server = &AuxiliaryServerConfig{}
	}
	if c.Configuration.Anlf.Server.BindingIPv4 == "" {
		c.Configuration.Anlf.Server.BindingIPv4 = NwdafSbiDefaultIPv4
	}
	if c.Configuration.Anlf.Server.Port == 0 {
		c.Configuration.Anlf.Server.Port = NwdafAnlfDefaultPort
	}
	if c.Configuration.Mtlf == nil {
		c.Configuration.Mtlf = &MtlfConfig{}
	}
	if c.Configuration.Mtlf.Server == nil {
		c.Configuration.Mtlf.Server = &AuxiliaryServerConfig{}
	}
	if c.Configuration.Mtlf.Server.BindingIPv4 == "" {
		c.Configuration.Mtlf.Server.BindingIPv4 = NwdafSbiDefaultIPv4
	}
	if c.Configuration.Mtlf.Server.Port == 0 {
		c.Configuration.Mtlf.Server.Port = NwdafMtlfDefaultPort
	}

	if len(c.Configuration.SupportedAnalytics) == 0 {
		c.Configuration.SupportedAnalytics = []string{NwdafSupportedEventUEComm}
	}
}

func (c *Config) Validate() (bool, error) {
	if c == nil {
		return false, errors.New("config is nil")
	}

	var errs []error
	if c.Configuration == nil {
		errs = append(errs, errors.New("configuration section is required"))
	} else if err := c.Configuration.validate(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return false, errors.Join(errs...)
	}
	return true, nil
}

func (c *Configuration) validate() error {
	var errs []error
	c.NrfCertPem = strings.TrimSpace(c.NrfCertPem)

	normalizedNrfURI, nrfErr := normalizeNrfURI(c.NrfUri)
	if nrfErr != nil {
		errs = append(errs, nrfErr)
	} else {
		c.NrfUri = normalizedNrfURI
	}

	if c.Sbi == nil {
		errs = append(errs, errors.New("sbi section is required"))
	} else if err := c.Sbi.validate(); err != nil {
		errs = append(errs, err)
	}
	if c.Anlf == nil {
		errs = append(errs, errors.New("anlf section is required"))
	} else if err := c.Anlf.validate(); err != nil {
		errs = append(errs, err)
	}
	if c.Mtlf == nil {
		errs = append(errs, errors.New("mtlf section is required"))
	} else if err := c.Mtlf.validate(); err != nil {
		errs = append(errs, err)
	}

	normalizedAnalytics, analyticsErr := normalizeSupportedAnalytics(c.SupportedAnalytics)
	if analyticsErr != nil {
		errs = append(errs, analyticsErr)
	} else {
		c.SupportedAnalytics = normalizedAnalytics
	}
	if c.Smf != nil && c.Smf.Enabled {
		if validateErr := c.Smf.validate(); validateErr != nil {
			errs = append(errs, validateErr)
		} else if c.Sbi != nil && c.Smf.NotifUris != nil {
			if schemeErr := validateOwnedCallbackScheme(
				"smf.notifUris.smf",
				c.Smf.NotifUris.Smf,
				c.Sbi.Scheme,
			); schemeErr != nil {
				errs = append(errs, schemeErr)
			}
			if schemeErr := validateOwnedCallbackScheme(
				"smf.notifUris.upf",
				c.Smf.NotifUris.Upf,
				c.Sbi.Scheme,
			); schemeErr != nil {
				errs = append(errs, schemeErr)
			}
		}
	}
	if c.AnlfBackend != nil && c.AnlfBackend.Enabled {
		if validateErr := c.AnlfBackend.validate(); validateErr != nil {
			errs = append(errs, validateErr)
		}
	}
	if c.ExternalMtlf != nil && c.ExternalMtlf.Enabled {
		if validateErr := c.ExternalMtlf.validate(); validateErr != nil {
			errs = append(errs, validateErr)
		}
	}
	if c.Adrf != nil && c.Adrf.AdrfEnabled() {
		if validateErr := c.Adrf.validate(); validateErr != nil {
			errs = append(errs, validateErr)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Sbi) validate() error {
	var errs []error

	s.Scheme = strings.ToLower(strings.TrimSpace(s.Scheme))
	s.BindingIPv4 = strings.TrimSpace(s.BindingIPv4)
	s.RegisterIPv4 = strings.TrimSpace(s.RegisterIPv4)

	switch s.Scheme {
	case NwdafSbiDefaultScheme, NwdafSbiTLSScheme:
	default:
		errs = append(errs, fmt.Errorf("sbi.scheme must be %q or %q", NwdafSbiDefaultScheme, NwdafSbiTLSScheme))
	}
	if !isValidHostValue(s.BindingIPv4) {
		errs = append(errs, fmt.Errorf("sbi.bindingIPv4 must be a valid host or IP"))
	}
	if s.RegisterIPv4 != "" && !isAdvertisableIPv4(s.RegisterIPv4) {
		errs = append(errs, fmt.Errorf("sbi.registerIPv4 must be a valid non-wildcard IPv4 address"))
	}
	if s.RegisterIPv4 == "" && !isAdvertisableIPv4(s.BindingIPv4) {
		errs = append(errs, fmt.Errorf(
			"sbi.registerIPv4 is required when sbi.bindingIPv4 is not an advertisable IPv4 address",
		))
	}
	if s.Port <= 0 || s.Port > 65535 {
		errs = append(errs, fmt.Errorf("sbi.port must be between 1 and 65535"))
	}
	if s.Scheme == NwdafSbiTLSScheme {
		if s.Tls == nil {
			errs = append(errs, errors.New("sbi.tls is required when sbi.scheme is https"))
		} else if err := s.Tls.validate("sbi.tls"); err != nil {
			errs = append(errs, err)
		}
	} else if s.Tls != nil {
		if err := s.Tls.validate("sbi.tls"); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (t *Tls) validate(fieldPrefix string) error {
	var errs []error

	t.Pem = strings.TrimSpace(t.Pem)
	t.Key = strings.TrimSpace(t.Key)

	if t.Pem == "" {
		errs = append(errs, fmt.Errorf("%s.pem is required", fieldPrefix))
	}
	if t.Key == "" {
		errs = append(errs, fmt.Errorf("%s.key is required", fieldPrefix))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (a *AnlfConfig) validate() error {
	if a.Server == nil {
		return errors.New("anlf.server section is required")
	}
	return a.Server.validate("anlf.server")
}

func (m *MtlfConfig) validate() error {
	if m.Server == nil {
		return errors.New("mtlf.server section is required")
	}
	return m.Server.validate("mtlf.server")
}

func (s *AuxiliaryServerConfig) validate(fieldPrefix string) error {
	var errs []error

	s.BindingIPv4 = strings.TrimSpace(s.BindingIPv4)
	s.RegisterIPv4 = strings.TrimSpace(s.RegisterIPv4)

	if !isValidHostValue(s.BindingIPv4) {
		errs = append(errs, fmt.Errorf("%s.bindingIPv4 must be a valid host or IP", fieldPrefix))
	}
	if s.RegisterIPv4 != "" && !isValidHostValue(s.RegisterIPv4) {
		errs = append(errs, fmt.Errorf("%s.registerIPv4 must be a valid host or IP", fieldPrefix))
	}
	if s.Port <= 0 || s.Port > 65535 {
		errs = append(errs, fmt.Errorf("%s.port must be between 1 and 65535", fieldPrefix))
	}
	if s.RegisterIPv4 == "" && isWildcardHostValue(s.BindingIPv4) {
		errs = append(errs, fmt.Errorf(
			"%s.registerIPv4 is required when bindingIPv4 is a wildcard address",
			fieldPrefix,
		))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *SmfConfig) validate() error {
	var errs []error

	s.EndpointSource = strings.ToLower(strings.TrimSpace(s.EndpointSource))
	switch s.EndpointSource {
	case SmfEndpointSourceConfigured:
		if len(s.Endpoints) == 0 {
			errs = append(errs, errors.New(
				"smf.endpoints must contain at least one endpoint when smf.endpointSource is configured",
			))
		}
		for i, endpoint := range s.Endpoints {
			if err := validateHTTPURL(fmt.Sprintf("smf.endpoints[%d]", i), endpoint); err != nil {
				errs = append(errs, err)
			}
		}
	case SmfEndpointSourceNRF:
	case "":
		errs = append(errs, errors.New("smf.endpointSource is required when smf.enabled is true"))
	default:
		errs = append(errs, fmt.Errorf(
			"smf.endpointSource must be %q or %q",
			SmfEndpointSourceNRF,
			SmfEndpointSourceConfigured,
		))
	}
	if s.NotifUris == nil {
		errs = append(errs, errors.New("smf.notifUris is required when smf.enabled is true"))
	} else {
		if err := validateHTTPURL("smf.notifUris.smf", s.NotifUris.Smf); err != nil {
			errs = append(errs, err)
		}
		if err := validateHTTPURL("smf.notifUris.upf", s.NotifUris.Upf); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (m *AnlfBackendConfig) validate() error {
	var errs []error
	if err := validateHTTPURL("anlfBackend.endpoint", m.Endpoint); err != nil {
		errs = append(errs, err)
	}
	if delivery := m.ObservationDelivery; delivery != nil {
		if delivery.QueueCapacity < 0 {
			errs = append(errs, errors.New("anlfBackend.observationDelivery.queueCapacity must be positive"))
		}
		if delivery.RequestTimeout < 0 {
			errs = append(errs, errors.New("anlfBackend.observationDelivery.requestTimeout must be positive"))
		}
		if delivery.MaxRetries < 0 {
			errs = append(errs, errors.New("anlfBackend.observationDelivery.maxRetries must be zero or positive"))
		}
		if delivery.RetryInterval < 0 {
			errs = append(errs, errors.New("anlfBackend.observationDelivery.retryInterval must be positive"))
		}
	}
	return errors.Join(errs...)
}

func (m *ExternalMtlfConfig) validate() error {
	var errs []error

	if len(m.Endpoints) == 0 {
		errs = append(
			errs,
			errors.New(
				"externalMtlf.endpoints must contain at least one endpoint when externalMtlf.enabled is true",
			),
		)
	}
	for i, endpoint := range m.Endpoints {
		if err := validateHTTPURL(fmt.Sprintf("externalMtlf.endpoints[%d]", i), endpoint); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (a *AdrfConfig) validate() error {
	var errs []error

	if err := validateHTTPURL("adrf.url", a.Url); err != nil {
		errs = append(errs, err)
	}
	if a.StorageThreshold < 0 {
		errs = append(errs, errors.New("adrf.storageThreshold must be zero or positive"))
	}
	if a.FetchBatchSize < 0 {
		errs = append(errs, errors.New("adrf.fetchBatchSize must be zero or positive"))
	}
	if a.FetchBatchSize > 1 {
		errs = append(errs, errors.New("adrf.fetchBatchSize must be 1 for the current ADRF retrieval flow"))
	}
	if a.RetrainWindow < 0 {
		errs = append(errs, errors.New("adrf.retrainWindow must be zero or positive"))
	}
	if a.WatchdogTimeout < 0 {
		errs = append(errs, errors.New("adrf.watchdogTimeout must be zero or positive"))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func normalizeSupportedAnalytics(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, errors.New("supportedAnalytics must contain at least one entry")
	}

	normalized := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	var errs []error

	for i, value := range values {
		normalizedValue := strings.ToUpper(strings.TrimSpace(value))
		if normalizedValue == "" {
			errs = append(errs, fmt.Errorf("supportedAnalytics[%d] must not be empty", i))
			continue
		}
		if _, ok := supportedAnalyticsAllowlist[normalizedValue]; !ok {
			errs = append(errs, fmt.Errorf("supportedAnalytics[%d] %q is not supported by the current runtime", i, value))
			continue
		}
		if _, ok := seen[normalizedValue]; ok {
			continue
		}
		seen[normalizedValue] = struct{}{}
		normalized = append(normalized, normalizedValue)
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return normalized, nil
}

func validateHTTPURL(fieldName string, raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return fmt.Errorf("%s is required", fieldName)
	}

	parsed, err := url.ParseRequestURI(trimmed)
	if err != nil {
		return fmt.Errorf("%s must be a valid URL: %w", fieldName, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("%s must use http or https", fieldName)
	}
	if parsed.Hostname() == "" {
		return fmt.Errorf("%s must include a host", fieldName)
	}

	return nil
}

func normalizeNrfURI(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", errors.New("nrfUri is required")
	}

	parsed, err := url.ParseRequestURI(trimmed)
	if err != nil {
		return "", fmt.Errorf("nrfUri must be a valid URL: %w", err)
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Scheme != NwdafSbiDefaultScheme && parsed.Scheme != NwdafSbiTLSScheme {
		return "", errors.New("nrfUri must use http or https")
	}
	if parsed.Hostname() == "" {
		return "", errors.New("nrfUri must include a host")
	}
	if port := parsed.Port(); port != "" {
		portNumber, portErr := strconv.Atoi(port)
		if portErr != nil || portNumber < 1 || portNumber > 65535 {
			return "", errors.New("nrfUri port must be between 1 and 65535")
		}
	}
	if parsed.User != nil {
		return "", errors.New("nrfUri must not include user information")
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return "", errors.New("nrfUri must not include a query")
	}
	if parsed.Fragment != "" {
		return "", errors.New("nrfUri must not include a fragment")
	}
	if parsed.RawPath != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", errors.New("nrfUri must not include a path")
	}

	parsed.Path = ""
	parsed.RawPath = ""
	return parsed.String(), nil
}

func validateOwnedCallbackScheme(fieldName string, raw string, expectedScheme string) error {
	trimmedScheme := strings.ToLower(strings.TrimSpace(expectedScheme))
	if trimmedScheme == "" {
		trimmedScheme = NwdafSbiDefaultScheme
	}

	trimmedURL := strings.TrimSpace(raw)
	if trimmedURL == "" {
		return nil
	}

	parsed, err := url.ParseRequestURI(trimmedURL)
	if err != nil {
		return fmt.Errorf("%s must be a valid URL: %w", fieldName, err)
	}
	if parsed.Scheme != trimmedScheme {
		return fmt.Errorf("%s scheme must match sbi.scheme (%s)", fieldName, trimmedScheme)
	}

	return nil
}

func isValidHostValue(value string) bool {
	if value == "" {
		return false
	}
	if net.ParseIP(value) != nil {
		return true
	}
	if strings.EqualFold(value, "localhost") {
		return true
	}

	labels := strings.Split(value, ".")
	for _, label := range labels {
		if label == "" || strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
			return false
		}
		for _, r := range label {
			if (r < 'a' || r > 'z') &&
				(r < 'A' || r > 'Z') &&
				(r < '0' || r > '9') &&
				r != '-' {
				return false
			}
		}
	}

	return true
}

func isWildcardHostValue(value string) bool {
	switch strings.TrimSpace(value) {
	case "0.0.0.0", "::":
		return true
	default:
		return false
	}
}

func isAdvertisableIPv4(value string) bool {
	trimmed := strings.TrimSpace(value)
	if strings.Contains(trimmed, ":") {
		return false
	}
	ip := net.ParseIP(trimmed)
	return ip != nil && ip.To4() != nil && !ip.IsUnspecified()
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

	hadConfiguration := cfg.Configuration != nil
	cfg.applyDefaults()
	if !hadConfiguration {
		err = errors.New("configuration section is required")
		logger.CfgLog.Errorf("Config validate error: %v", err)
		return nil, err
	}
	if _, err = cfg.Validate(); err != nil {
		logger.CfgLog.Errorf("Config validate error: %v", err)
		logger.CfgLog.Errorf("[-- PLEASE REFER TO SAMPLE CONFIG FILE COMMENTS --]")
		return nil, err
	}

	logger.CfgLog.Infof("Config loaded: %s", cfgPath)
	return cfg, nil
}

func (c *Config) GetSbiBindingAddr() string {
	return c.GetSbiBindingIP() + ":" + strconv.Itoa(c.GetSbiPort())
}

func (c *Config) GetSbiBindingIP() string {
	if c == nil || c.Configuration == nil || c.Configuration.Sbi == nil ||
		strings.TrimSpace(c.Configuration.Sbi.BindingIPv4) == "" {
		return NwdafSbiDefaultIPv4
	}
	return strings.TrimSpace(c.Configuration.Sbi.BindingIPv4)
}

func (c *Config) GetSbiRegisterIP() string {
	if c == nil || c.Configuration == nil || c.Configuration.Sbi == nil {
		return NwdafSbiDefaultIPv4
	}
	if registerIP := strings.TrimSpace(c.Configuration.Sbi.RegisterIPv4); registerIP != "" {
		return registerIP
	}
	return c.GetSbiBindingIP()
}

func (c *Config) GetSbiRegisterAddr() string {
	return c.GetSbiRegisterIP() + ":" + strconv.Itoa(c.GetSbiPort())
}

func (c *Config) GetSbiUri() string {
	return c.GetSbiScheme() + "://" + c.GetSbiRegisterAddr()
}

func (c *Config) GetSbiPort() int {
	if c == nil || c.Configuration == nil || c.Configuration.Sbi == nil || c.Configuration.Sbi.Port == 0 {
		return NwdafSbiDefaultPort
	}
	return c.Configuration.Sbi.Port
}

func (c *Config) GetSbiScheme() string {
	if c == nil || c.Configuration == nil || c.Configuration.Sbi == nil ||
		strings.TrimSpace(c.Configuration.Sbi.Scheme) == "" {
		return NwdafSbiDefaultScheme
	}
	return strings.ToLower(strings.TrimSpace(c.Configuration.Sbi.Scheme))
}

func (c *Config) GetCertPemPath() string {
	if c == nil || c.Configuration == nil || c.Configuration.Sbi == nil || c.Configuration.Sbi.Tls == nil {
		return ""
	}
	return strings.TrimSpace(c.Configuration.Sbi.Tls.Pem)
}

func (c *Config) GetCertKeyPath() string {
	if c == nil || c.Configuration == nil || c.Configuration.Sbi == nil || c.Configuration.Sbi.Tls == nil {
		return ""
	}
	return strings.TrimSpace(c.Configuration.Sbi.Tls.Key)
}

func (c *Config) GetAnlfServerBindingAddr() string {
	return c.GetAnlfServerBindingIP() + ":" + strconv.Itoa(c.GetAnlfServerPort())
}

func (c *Config) GetAnlfServerBindingIP() string {
	if c == nil || c.Configuration == nil || c.Configuration.Anlf == nil || c.Configuration.Anlf.Server == nil ||
		strings.TrimSpace(c.Configuration.Anlf.Server.BindingIPv4) == "" {
		return NwdafSbiDefaultIPv4
	}
	return strings.TrimSpace(c.Configuration.Anlf.Server.BindingIPv4)
}

func (c *Config) GetAnlfServerRegisterIP() string {
	if c == nil || c.Configuration == nil || c.Configuration.Anlf == nil || c.Configuration.Anlf.Server == nil {
		return NwdafSbiDefaultIPv4
	}
	if registerIP := strings.TrimSpace(c.Configuration.Anlf.Server.RegisterIPv4); registerIP != "" {
		return registerIP
	}
	return c.GetAnlfServerBindingIP()
}

func (c *Config) GetAnlfServerRegisterAddr() string {
	return c.GetAnlfServerRegisterIP() + ":" + strconv.Itoa(c.GetAnlfServerPort())
}

func (c *Config) GetAnlfServerURI() string {
	return NwdafSbiDefaultScheme + "://" + c.GetAnlfServerRegisterAddr()
}

func (c *Config) GetAnlfServerPort() int {
	if c == nil || c.Configuration == nil || c.Configuration.Anlf == nil || c.Configuration.Anlf.Server == nil ||
		c.Configuration.Anlf.Server.Port == 0 {
		return NwdafAnlfDefaultPort
	}
	return c.Configuration.Anlf.Server.Port
}

func (c *Config) GetMtlfServerBindingAddr() string {
	return c.GetMtlfServerBindingIP() + ":" + strconv.Itoa(c.GetMtlfServerPort())
}

func (c *Config) GetMtlfServerBindingIP() string {
	if c == nil || c.Configuration == nil || c.Configuration.Mtlf == nil || c.Configuration.Mtlf.Server == nil ||
		strings.TrimSpace(c.Configuration.Mtlf.Server.BindingIPv4) == "" {
		return NwdafSbiDefaultIPv4
	}
	return strings.TrimSpace(c.Configuration.Mtlf.Server.BindingIPv4)
}

func (c *Config) GetMtlfServerRegisterIP() string {
	if c == nil || c.Configuration == nil || c.Configuration.Mtlf == nil || c.Configuration.Mtlf.Server == nil {
		return NwdafSbiDefaultIPv4
	}
	if registerIP := strings.TrimSpace(c.Configuration.Mtlf.Server.RegisterIPv4); registerIP != "" {
		return registerIP
	}
	return c.GetMtlfServerBindingIP()
}

func (c *Config) GetMtlfServerRegisterAddr() string {
	return c.GetMtlfServerRegisterIP() + ":" + strconv.Itoa(c.GetMtlfServerPort())
}

func (c *Config) GetMtlfServerURI() string {
	return NwdafSbiDefaultScheme + "://" + c.GetMtlfServerRegisterAddr()
}

func (c *Config) GetMtlfServerPort() int {
	if c == nil || c.Configuration == nil || c.Configuration.Mtlf == nil || c.Configuration.Mtlf.Server == nil ||
		c.Configuration.Mtlf.Server.Port == 0 {
		return NwdafMtlfDefaultPort
	}
	return c.Configuration.Mtlf.Server.Port
}

func (c *Config) GetNwdafName() string {
	if c == nil || c.Configuration == nil {
		return NwdafDefaultNwdafName
	}
	return c.Configuration.NwdafName
}

func (c *Config) GetNrfUri() string {
	if c == nil || c.Configuration == nil {
		return ""
	}
	return c.Configuration.NrfUri
}

func (c *Config) GetNrfCertPem() string {
	if c == nil || c.Configuration == nil {
		return ""
	}
	return strings.TrimSpace(c.Configuration.NrfCertPem)
}
