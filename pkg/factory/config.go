package factory

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"

	compatnrf "github.com/free5gc/nwdaf/internal/compat/nrf"
	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/openapi/models"
)

const (
	NwdafDefaultConfigPath            = "./config/nwdafcfg.yaml"
	NwdafSbiDefaultScheme             = "http"
	NwdafSbiTLSScheme                 = "https"
	NwdafSbiDefaultIPv4               = "127.0.0.1"
	NwdafSbiDefaultPort               = 8080
	NwdafAnlfDefaultPort              = 8090
	NwdafMtlfDefaultPort              = 8091
	NwdafDefaultNwdafName             = "NWDAF"
	NwdafEventsSubResUriPrefix        = "/nnwdaf-eventssubscription/v1"
	NwdafMLModelProvisionResURIPrefix = "/nnwdaf-mlmodelprovision/v1"
	NwdafMLModelMonitorResURIPrefix   = "/nnwdaf-mlmodelmonitor/v1"
	NwdafMLModelTrainingResURIPrefix  = "/nnwdaf-mlmodeltraining/v1"
	NwdafSupportedEventUEComm         = "UE_COMMUNICATION"
	NwdafMLModelMonitorServiceName    = "nnwdaf-mlmodelmonitor"
	NwdafMLModelTrainingServiceName   = "nnwdaf-mlmodeltraining"
)

var NwdafConfig *Config

var supportedAnalyticsAllowlist = map[string]struct{}{
	NwdafSupportedEventUEComm: {},
}

var supportedServiceAllowlist = map[models.ServiceName]struct{}{
	models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION:        {},
	models.ServiceName_NNWDAF_MLMODELPROVISION:          {},
	models.ServiceName(NwdafMLModelMonitorServiceName):  {},
	models.ServiceName(NwdafMLModelTrainingServiceName): {},
}

var (
	mccPattern = regexp.MustCompile(`^[0-9]{3}$`)
	mncPattern = regexp.MustCompile(`^[0-9]{2,3}$`)
	tacPattern = regexp.MustCompile(`^(?:[0-9A-Fa-f]{4}|[0-9A-Fa-f]{6})$`)
	sdPattern  = regexp.MustCompile(`^[0-9A-Fa-f]{6}$`)
)

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
	NwdafName              string               `yaml:"nwdafName,omitempty"`
	NfInstanceID           string               `yaml:"nfInstanceId,omitempty"`
	Sbi                    *Sbi                 `yaml:"sbi,omitempty"`
	NrfRegistrationEnabled *bool                `yaml:"nrfRegistrationEnabled,omitempty"`
	NrfUri                 string               `yaml:"nrfUri,omitempty"`
	NrfCertPem             string               `yaml:"nrfCertPem,omitempty"`
	ServiceNameList        []models.ServiceName `yaml:"serviceNameList,omitempty"`
	NwdafInfo              *NwdafInfoConfig     `yaml:"nwdafInfo,omitempty"`
	SupportedAnalytics     []string             `yaml:"supportedAnalytics,omitempty"`
	Anlf                   *AnlfConfig          `yaml:"anlf,omitempty"`
	AnlfBackend            *AnlfBackendConfig   `yaml:"anlfBackend,omitempty"`
	MtlfBackend            *MtlfBackendConfig   `yaml:"mtlfBackend,omitempty"`
	Mtlf                   *MtlfConfig          `yaml:"mtlf,omitempty"`
}

// NwdafInfoConfig mirrors the Release 18 NwdafInfo wire property names used
// by NF registration. It is configuration input, not a second capability
// model.
type NwdafInfoConfig struct {
	NwdafEvents     []models.NwdafEvent     `yaml:"nwdafEvents,omitempty"`
	MLAnalyticsList []MLAnalyticsInfoConfig `yaml:"mlAnalyticsList,omitempty"`
}

type MLAnalyticsInfoConfig struct {
	MLAnalyticsIDs              []models.NwdafEvent            `yaml:"mlAnalyticsIds,omitempty"`
	SNSSAIList                  []models.Snssai                `yaml:"snssaiList,omitempty"`
	TrackingAreaList            []models.Tai                   `yaml:"trackingAreaList,omitempty"`
	MLModelInteroperabilityInfo *MLModelInteroperabilityConfig `yaml:"mlModelInterInfo,omitempty"`
	FLCapabilityType            compatnrf.FLCapabilityType     `yaml:"flCapabilityType,omitempty"`
	FLTimeInterval              *models.TimeWindow             `yaml:"flTimeInterval,omitempty"`
	NFTypeList                  []models.NrfNfManagementNfType `yaml:"nfTypeList,omitempty"`
	NFSetIDList                 []string                       `yaml:"nfSetIdList,omitempty"`
}

type MLModelInteroperabilityConfig struct {
	VendorList []string `yaml:"vendorList,omitempty"`
}

type AnlfConfig struct {
	Server *AuxiliaryServerConfig `yaml:"server,omitempty"`
}

// AnlfBackendConfig configures the downstream AnLF backend used by NWDAF.
type AnlfBackendConfig struct {
	Enabled        bool   `yaml:"enabled"`
	Endpoint       string `yaml:"endpoint,omitempty"`
	RequestTimeout int    `yaml:"requestTimeout,omitempty"`
}

func (c *AnlfBackendConfig) RequestTimeoutOrDefault() int {
	if c != nil && c.RequestTimeout > 0 {
		return c.RequestTimeout
	}
	return 5
}

// MtlfBackendConfig configures the private MTLF backend boundary. The backend
// is not a standalone standard NF and does not own standard SBI communication.
type MtlfBackendConfig struct {
	Enabled        bool   `yaml:"enabled"`
	Endpoint       string `yaml:"endpoint,omitempty"`
	RequestTimeout int    `yaml:"requestTimeout,omitempty"`
}

func (c *MtlfBackendConfig) RequestTimeoutOrDefault() int {
	if c != nil && c.RequestTimeout > 0 {
		return c.RequestTimeout
	}
	return 5
}

// MtlfConfig configures the private MTLF backend-facing auxiliary listener.
type MtlfConfig struct {
	Server *AuxiliaryServerConfig `yaml:"server,omitempty"`
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
	if c.Configuration.MtlfBackend != nil && c.Configuration.MtlfBackend.RequestTimeout == 0 {
		c.Configuration.MtlfBackend.RequestTimeout = 5
	}
	if c.Configuration.AnlfBackend != nil && c.Configuration.AnlfBackend.RequestTimeout == 0 {
		c.Configuration.AnlfBackend.RequestTimeout = 5
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
	c.NfInstanceID = strings.TrimSpace(c.NfInstanceID)
	c.NrfCertPem = strings.TrimSpace(c.NrfCertPem)

	if c.NfInstanceID != "" {
		parsedID, parseErr := uuid.Parse(c.NfInstanceID)
		if parseErr != nil || parsedID.Version() != 4 {
			errs = append(errs, errors.New("nfInstanceId must be a UUIDv4 when configured"))
		} else {
			c.NfInstanceID = parsedID.String()
		}
	}

	if strings.TrimSpace(c.NrfUri) != "" || c.NrfRegistrationEnabledOrDefault() {
		normalizedNrfURI, nrfErr := normalizeNrfURI(c.NrfUri)
		if nrfErr != nil {
			errs = append(errs, nrfErr)
		} else {
			c.NrfUri = normalizedNrfURI
		}
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

	explicitProfile := c.ServiceNameList != nil || c.NwdafInfo != nil
	if explicitProfile {
		normalizedServices, serviceErr := normalizeServiceNameList(c.ServiceNameList)
		if serviceErr != nil {
			errs = append(errs, serviceErr)
		} else {
			c.ServiceNameList = normalizedServices
		}
		if c.NwdafInfo != nil {
			if infoErr := c.NwdafInfo.normalizeAndValidate(); infoErr != nil {
				errs = append(errs, infoErr)
			}
		}
		if dependencyErr := c.validateProfileDependencies(); dependencyErr != nil {
			errs = append(errs, dependencyErr)
		}
	}
	if c.AnlfBackend != nil && c.AnlfBackend.Enabled {
		if validateErr := c.AnlfBackend.validate(); validateErr != nil {
			errs = append(errs, validateErr)
		}
	}
	if c.MtlfBackend != nil && c.MtlfBackend.Enabled {
		if validateErr := c.MtlfBackend.validate(); validateErr != nil {
			errs = append(errs, validateErr)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (c *Configuration) validateProfileDependencies() error {
	var errs []error
	services := make(map[models.ServiceName]struct{}, len(c.ServiceNameList))
	for _, serviceName := range c.ServiceNameList {
		services[serviceName] = struct{}{}
	}

	_, advertisesEvents := services[models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION]
	_, advertisesProvision := services[models.ServiceName_NNWDAF_MLMODELPROVISION]
	_, advertisesMonitor := services[models.ServiceName(NwdafMLModelMonitorServiceName)]
	_, advertisesTraining := services[models.ServiceName(NwdafMLModelTrainingServiceName)]

	hasAnlf := c.AnlfBackend != nil && c.AnlfBackend.Enabled
	hasMtlf := c.MtlfBackend != nil && c.MtlfBackend.Enabled
	hasEvents := c.NwdafInfo != nil && len(c.NwdafInfo.NwdafEvents) > 0
	hasMLAnalytics := c.NwdafInfo != nil && len(c.NwdafInfo.MLAnalyticsList) > 0

	if advertisesEvents && !hasAnlf {
		errs = append(errs, errors.New(
			"serviceNameList nnwdaf-eventssubscription requires an enabled anlfBackend",
		))
	}
	if advertisesEvents && !hasEvents {
		errs = append(errs, errors.New(
			"serviceNameList nnwdaf-eventssubscription requires non-empty nwdafInfo.nwdafEvents",
		))
	}
	if hasEvents && !advertisesEvents {
		errs = append(errs, errors.New(
			"nwdafInfo.nwdafEvents requires nnwdaf-eventssubscription in serviceNameList",
		))
	}
	if advertisesProvision && !hasMtlf {
		errs = append(errs, errors.New(
			"serviceNameList nnwdaf-mlmodelprovision requires an enabled mtlfBackend",
		))
	}
	if advertisesProvision && !hasMLAnalytics {
		errs = append(errs, errors.New(
			"serviceNameList nnwdaf-mlmodelprovision requires non-empty nwdafInfo.mlAnalyticsList",
		))
	}
	if advertisesMonitor && !hasAnlf && !hasMtlf {
		errs = append(errs, errors.New(
			"serviceNameList nnwdaf-mlmodelmonitor requires an enabled anlfBackend or mtlfBackend",
		))
	}
	if advertisesTraining && !hasMtlf {
		errs = append(errs, errors.New(
			"serviceNameList nnwdaf-mlmodeltraining requires an enabled mtlfBackend",
		))
	}
	if advertisesTraining && !hasMLAnalytics {
		errs = append(errs, errors.New(
			"serviceNameList nnwdaf-mlmodeltraining requires non-empty nwdafInfo.mlAnalyticsList",
		))
	}
	if advertisesTraining {
		hasClientCapability := false
		for _, entry := range c.NwdafInfo.MLAnalyticsList {
			if entry.FLCapabilityType == compatnrf.FLCapabilityTypeClient ||
				entry.FLCapabilityType == compatnrf.FLCapabilityTypeServerAndClient {
				hasClientCapability = true
				break
			}
		}
		if !hasClientCapability {
			errs = append(errs, errors.New(
				"serviceNameList nnwdaf-mlmodeltraining requires FL_CLIENT capability",
			))
		}
	}
	if hasMLAnalytics {
		for index, entry := range c.NwdafInfo.MLAnalyticsList {
			if entry.FLCapabilityType != "" && !hasMtlf {
				errs = append(errs, fmt.Errorf(
					"nwdafInfo.mlAnalyticsList[%d].flCapabilityType requires an enabled mtlfBackend",
					index,
				))
			}
		}
	}
	return errors.Join(errs...)
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

func (m *AnlfBackendConfig) validate() error {
	var errs []error
	endpoint, err := normalizeHTTPOrigin("anlfBackend.endpoint", m.Endpoint)
	if err != nil {
		errs = append(errs, err)
	} else {
		m.Endpoint = endpoint
	}
	if m.RequestTimeout <= 0 {
		errs = append(errs, errors.New("anlfBackend.requestTimeout must be positive"))
	}
	return errors.Join(errs...)
}

func (m *MtlfBackendConfig) validate() error {
	var errs []error
	endpoint, err := normalizeHTTPOrigin("mtlfBackend.endpoint", m.Endpoint)
	if err != nil {
		errs = append(errs, err)
	} else {
		m.Endpoint = endpoint
	}
	if m.RequestTimeout <= 0 {
		errs = append(errs, errors.New("mtlfBackend.requestTimeout must be positive"))
	}
	return errors.Join(errs...)
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

func normalizeServiceNameList(values []models.ServiceName) ([]models.ServiceName, error) {
	if values == nil {
		return nil, nil
	}
	if len(values) == 0 {
		return nil, errors.New("serviceNameList must contain at least one entry when present")
	}

	normalized := make([]models.ServiceName, 0, len(values))
	seen := make(map[models.ServiceName]struct{}, len(values))
	var errs []error
	for index, value := range values {
		serviceName := models.ServiceName(strings.TrimSpace(string(value)))
		if _, supported := supportedServiceAllowlist[serviceName]; !supported {
			errs = append(errs, fmt.Errorf(
				"serviceNameList[%d] %q is not implemented by the current runtime",
				index,
				value,
			))
			continue
		}
		if _, duplicate := seen[serviceName]; duplicate {
			errs = append(errs, fmt.Errorf(
				"serviceNameList[%d] duplicates %q",
				index,
				serviceName,
			))
			continue
		}
		seen[serviceName] = struct{}{}
		normalized = append(normalized, serviceName)
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	slices.SortFunc(normalized, func(left, right models.ServiceName) int {
		return strings.Compare(string(left), string(right))
	})
	return normalized, nil
}

func (c *NwdafInfoConfig) normalizeAndValidate() error {
	if c == nil {
		return nil
	}
	var errs []error

	var normalizedEvents []models.NwdafEvent
	if c.NwdafEvents != nil {
		normalizedEvents = make([]models.NwdafEvent, 0, len(c.NwdafEvents))
	}
	seenEvents := make(map[models.NwdafEvent]struct{}, len(c.NwdafEvents))
	for index, event := range c.NwdafEvents {
		normalized := models.NwdafEvent(strings.TrimSpace(string(event)))
		if normalized != models.NwdafEvent_UE_COMMUNICATION {
			errs = append(errs, fmt.Errorf(
				"nwdafInfo.nwdafEvents[%d] %q is not supported by the current runtime",
				index,
				event,
			))
			continue
		}
		if _, duplicate := seenEvents[normalized]; duplicate {
			continue
		}
		seenEvents[normalized] = struct{}{}
		normalizedEvents = append(normalizedEvents, normalized)
	}
	c.NwdafEvents = normalizedEvents

	seenEntries := make(map[string]struct{}, len(c.MLAnalyticsList))
	for index := range c.MLAnalyticsList {
		entry := &c.MLAnalyticsList[index]
		if err := entry.normalizeAndValidate(); err != nil {
			errs = append(errs, fmt.Errorf("nwdafInfo.mlAnalyticsList[%d]: %w", index, err))
			continue
		}
		canonical, err := json.Marshal(entry.CompatibilityValue())
		if err != nil {
			errs = append(errs, fmt.Errorf(
				"nwdafInfo.mlAnalyticsList[%d]: encode normalized entry: %w",
				index,
				err,
			))
			continue
		}
		key := string(canonical)
		if _, duplicate := seenEntries[key]; duplicate {
			errs = append(errs, fmt.Errorf(
				"nwdafInfo.mlAnalyticsList[%d] duplicates an equivalent entry",
				index,
			))
			continue
		}
		seenEntries[key] = struct{}{}
	}

	if c.NwdafEvents != nil && len(c.NwdafEvents) == 0 {
		errs = append(errs, errors.New(
			"nwdafInfo.nwdafEvents must contain at least one entry when present",
		))
	}
	if c.MLAnalyticsList != nil && len(c.MLAnalyticsList) == 0 {
		errs = append(errs, errors.New(
			"nwdafInfo.mlAnalyticsList must contain at least one entry when present",
		))
	}
	return errors.Join(errs...)
}

func (c *MLAnalyticsInfoConfig) normalizeAndValidate() error {
	if c == nil {
		return errors.New("entry is required")
	}
	var errs []error

	c.MLAnalyticsIDs = normalizeNwdafEvents(c.MLAnalyticsIDs)
	if len(c.MLAnalyticsIDs) == 0 {
		errs = append(errs, errors.New("mlAnalyticsIds must contain at least one entry"))
	}
	for index, event := range c.MLAnalyticsIDs {
		if event != models.NwdafEvent_UE_COMMUNICATION {
			errs = append(errs, fmt.Errorf(
				"mlAnalyticsIds[%d] %q is not supported by the current runtime",
				index,
				event,
			))
		}
	}

	slices.SortFunc(c.SNSSAIList, func(left, right models.Snssai) int {
		if left.Sst != right.Sst {
			return int(left.Sst - right.Sst)
		}
		return strings.Compare(strings.ToUpper(left.Sd), strings.ToUpper(right.Sd))
	})
	c.SNSSAIList = slices.CompactFunc(c.SNSSAIList, func(left, right models.Snssai) bool {
		return left.Sst == right.Sst && strings.EqualFold(left.Sd, right.Sd)
	})
	for index := range c.SNSSAIList {
		c.SNSSAIList[index].Sd = strings.ToUpper(strings.TrimSpace(c.SNSSAIList[index].Sd))
		if c.SNSSAIList[index].Sst < 0 || c.SNSSAIList[index].Sst > 255 {
			errs = append(errs, fmt.Errorf("snssaiList[%d].sst must be between 0 and 255", index))
		}
		if c.SNSSAIList[index].Sd != "" && !sdPattern.MatchString(c.SNSSAIList[index].Sd) {
			errs = append(errs, fmt.Errorf("snssaiList[%d].sd must contain six hexadecimal digits", index))
		}
	}

	for index := range c.TrackingAreaList {
		tai := &c.TrackingAreaList[index]
		tai.Tac = strings.ToUpper(strings.TrimSpace(tai.Tac))
		if tai.PlmnId == nil {
			errs = append(errs, fmt.Errorf("trackingAreaList[%d].plmnId is required", index))
			continue
		}
		tai.PlmnId.Mcc = strings.TrimSpace(tai.PlmnId.Mcc)
		tai.PlmnId.Mnc = strings.TrimSpace(tai.PlmnId.Mnc)
		if !mccPattern.MatchString(tai.PlmnId.Mcc) {
			errs = append(errs, fmt.Errorf("trackingAreaList[%d].plmnId.mcc must contain three digits", index))
		}
		if !mncPattern.MatchString(tai.PlmnId.Mnc) {
			errs = append(errs, fmt.Errorf("trackingAreaList[%d].plmnId.mnc must contain two or three digits", index))
		}
		if !tacPattern.MatchString(tai.Tac) {
			errs = append(errs, fmt.Errorf(
				"trackingAreaList[%d].tac must contain four or six hexadecimal digits",
				index,
			))
		}
	}
	slices.SortFunc(c.TrackingAreaList, func(left, right models.Tai) int {
		return strings.Compare(taiSortKey(left), taiSortKey(right))
	})
	c.TrackingAreaList = slices.CompactFunc(c.TrackingAreaList, func(left, right models.Tai) bool {
		return taiSortKey(left) == taiSortKey(right)
	})

	if c.MLModelInteroperabilityInfo != nil {
		vendors := make([]string, 0, len(c.MLModelInteroperabilityInfo.VendorList))
		for _, vendor := range c.MLModelInteroperabilityInfo.VendorList {
			vendors = append(vendors, strings.TrimSpace(vendor))
		}
		sort.Strings(vendors)
		c.MLModelInteroperabilityInfo.VendorList = slices.Compact(vendors)
	}

	c.NFTypeList = normalizeNFTypes(c.NFTypeList)
	for index, nfType := range c.NFTypeList {
		if nfType != models.NrfNfManagementNfType_UPF {
			errs = append(errs, fmt.Errorf(
				"nfTypeList[%d] %q is not supported as a local training data source",
				index,
				nfType,
			))
		}
	}
	c.NFSetIDList = normalizeStrings(c.NFSetIDList)
	if c.FLCapabilityType != "" && !compatnrf.IsKnownFLCapability(c.FLCapabilityType) {
		errs = append(errs, fmt.Errorf(
			"flCapabilityType %q is not a supported Release 18 value",
			c.FLCapabilityType,
		))
	}
	if c.FLCapabilityType == "" &&
		(c.FLTimeInterval != nil || len(c.NFTypeList) > 0 || len(c.NFSetIDList) > 0) {
		errs = append(errs, errors.New(
			"flTimeInterval, nfTypeList and nfSetIdList require flCapabilityType",
		))
	}
	if compatErr := compatnrf.ValidateMLAnalyticsInfo(c.CompatibilityValue()); compatErr != nil {
		errs = append(errs, compatErr)
	}
	return errors.Join(errs...)
}

func normalizeNwdafEvents(values []models.NwdafEvent) []models.NwdafEvent {
	normalized := make([]models.NwdafEvent, 0, len(values))
	for _, value := range values {
		normalized = append(normalized, models.NwdafEvent(strings.TrimSpace(string(value))))
	}
	slices.SortFunc(normalized, func(left, right models.NwdafEvent) int {
		return strings.Compare(string(left), string(right))
	})
	return slices.Compact(normalized)
}

func normalizeNFTypes(values []models.NrfNfManagementNfType) []models.NrfNfManagementNfType {
	normalized := make([]models.NrfNfManagementNfType, 0, len(values))
	for _, value := range values {
		normalized = append(normalized, models.NrfNfManagementNfType(strings.TrimSpace(string(value))))
	}
	slices.SortFunc(normalized, func(left, right models.NrfNfManagementNfType) int {
		return strings.Compare(string(left), string(right))
	})
	return slices.Compact(normalized)
}

func normalizeStrings(values []string) []string {
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		normalized = append(normalized, strings.TrimSpace(value))
	}
	sort.Strings(normalized)
	return slices.Compact(normalized)
}

func taiSortKey(value models.Tai) string {
	if value.PlmnId == nil {
		return "\x00" + value.Tac
	}
	return value.PlmnId.Mcc + "\x00" + value.PlmnId.Mnc + "\x00" + value.Tac + "\x00" + value.Nid
}

func (c MLAnalyticsInfoConfig) CompatibilityValue() compatnrf.MLAnalyticsInfo {
	var interoperability *compatnrf.MLModelInteroperabilityInfo
	if c.MLModelInteroperabilityInfo != nil {
		interoperability = &compatnrf.MLModelInteroperabilityInfo{
			VendorList: append([]string(nil), c.MLModelInteroperabilityInfo.VendorList...),
		}
	}
	return compatnrf.MLAnalyticsInfo{
		MLAnalyticsIDs:              append([]models.NwdafEvent(nil), c.MLAnalyticsIDs...),
		SNSSAIList:                  append([]models.Snssai(nil), c.SNSSAIList...),
		TrackingAreaList:            append([]models.Tai(nil), c.TrackingAreaList...),
		MLModelInteroperabilityInfo: interoperability,
		FLCapabilityType:            c.FLCapabilityType,
		FLTimeInterval:              c.FLTimeInterval,
		NFTypeList:                  append([]models.NrfNfManagementNfType(nil), c.NFTypeList...),
		NFSetIDList:                 append([]string(nil), c.NFSetIDList...),
	}
}

func (c *NwdafInfoConfig) CompatibilityValue() *compatnrf.NwdafInfo {
	if c == nil {
		return nil
	}
	value := &compatnrf.NwdafInfo{
		NwdafInfo: models.NwdafInfo{
			NwdafEvents: append([]models.NwdafEvent(nil), c.NwdafEvents...),
		},
	}
	if c.MLAnalyticsList != nil {
		value.MLAnalyticsList = make([]compatnrf.MLAnalyticsInfo, len(c.MLAnalyticsList))
		for index, entry := range c.MLAnalyticsList {
			value.MLAnalyticsList[index] = entry.CompatibilityValue()
		}
	}
	return value
}

func normalizeHTTPOrigin(fieldName string, raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("%s is required", fieldName)
	}
	parsed, err := url.ParseRequestURI(trimmed)
	if err != nil {
		return "", fmt.Errorf("%s must be a valid URL: %w", fieldName, err)
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Scheme != NwdafSbiDefaultScheme && parsed.Scheme != NwdafSbiTLSScheme {
		return "", fmt.Errorf("%s must use http or https", fieldName)
	}
	if parsed.Hostname() == "" {
		return "", fmt.Errorf("%s must include a host", fieldName)
	}
	if parsed.User != nil {
		return "", fmt.Errorf("%s must not include userinfo", fieldName)
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return "", fmt.Errorf("%s must not include a path", fieldName)
	}
	if parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return "", fmt.Errorf("%s must not include a query or fragment", fieldName)
	}
	if port := parsed.Port(); port != "" {
		portNumber, portErr := strconv.Atoi(port)
		if portErr != nil || portNumber < 1 || portNumber > 65535 {
			return "", fmt.Errorf("%s port must be between 1 and 65535", fieldName)
		}
	}
	parsed.Path = ""
	parsed.RawPath = ""
	return parsed.Scheme + "://" + parsed.Host, nil
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

func (c *Config) GetNFInstanceID() string {
	if c == nil || c.Configuration == nil {
		return ""
	}
	return strings.TrimSpace(c.Configuration.NfInstanceID)
}

func (c *Config) GetServiceNameList() []models.ServiceName {
	if c == nil || c.Configuration == nil {
		return nil
	}
	if c.Configuration.ServiceNameList != nil || c.Configuration.NwdafInfo != nil {
		return append([]models.ServiceName(nil), c.Configuration.ServiceNameList...)
	}

	services := make([]models.ServiceName, 0, 4)
	hasAnlf := c.Configuration.AnlfBackend != nil && c.Configuration.AnlfBackend.Enabled
	hasMtlf := c.Configuration.MtlfBackend != nil && c.Configuration.MtlfBackend.Enabled
	if hasAnlf {
		services = append(services, models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION)
	}
	if hasMtlf {
		services = append(services, models.ServiceName_NNWDAF_MLMODELPROVISION)
	}
	if hasAnlf && hasMtlf {
		services = append(services, models.ServiceName(NwdafMLModelMonitorServiceName))
	}
	return services
}

func (c *Config) GetNwdafInfo() *compatnrf.NwdafInfo {
	if c == nil || c.Configuration == nil {
		return nil
	}
	if c.Configuration.ServiceNameList != nil || c.Configuration.NwdafInfo != nil {
		return c.Configuration.NwdafInfo.CompatibilityValue()
	}

	services := c.GetServiceNameList()
	info := &compatnrf.NwdafInfo{}
	for _, serviceName := range services {
		switch serviceName {
		case models.ServiceName_NNWDAF_EVENTSSUBSCRIPTION:
			info.NwdafEvents = []models.NwdafEvent{models.NwdafEvent_UE_COMMUNICATION}
		case models.ServiceName_NNWDAF_MLMODELPROVISION:
			info.MLAnalyticsList = []compatnrf.MLAnalyticsInfo{{
				MLAnalyticsIDs: []models.NwdafEvent{models.NwdafEvent_UE_COMMUNICATION},
			}}
		}
	}
	if len(info.NwdafEvents) == 0 && len(info.MLAnalyticsList) == 0 {
		return nil
	}
	return info
}

func (c *Config) HasExplicitNFProfile() bool {
	return c != nil && c.Configuration != nil &&
		(c.Configuration.ServiceNameList != nil || c.Configuration.NwdafInfo != nil)
}

func (c *Config) GetNrfUri() string {
	if c == nil || c.Configuration == nil {
		return ""
	}
	return c.Configuration.NrfUri
}

func (c *Config) NrfRegistrationEnabled() bool {
	return c != nil && c.Configuration != nil &&
		c.Configuration.NrfRegistrationEnabledOrDefault()
}

func (c *Configuration) NrfRegistrationEnabledOrDefault() bool {
	return c == nil || c.NrfRegistrationEnabled == nil || *c.NrfRegistrationEnabled
}

func (c *Config) GetNrfCertPem() string {
	if c == nil || c.Configuration == nil {
		return ""
	}
	return strings.TrimSpace(c.Configuration.NrfCertPem)
}
