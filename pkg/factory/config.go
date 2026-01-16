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
	NwdafName          string          `yaml:"nwdafName,omitempty"`
	Sbi                *Sbi            `yaml:"sbi,omitempty"`
	NrfUri             string          `yaml:"nrfUri,omitempty"`
	SupportedAnalytics []string        `yaml:"supportedAnalytics,omitempty"`
	DataCollection     *DataCollection `yaml:"dataCollection,omitempty"`
}

// DataCollection configuration for data collection from other NFs
type DataCollection struct {
	Smf *SmfDataCollection `yaml:"smf,omitempty"`
}

// SmfDataCollection configuration for SMF data collection
type SmfDataCollection struct {
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

	if err := yaml.Unmarshal(data, cfg); err != nil {
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

func (c *Config) GetNwdafName() string {
	if c.Configuration == nil {
		return NwdafDefaultNwdafName
	}
	return c.Configuration.NwdafName
}
