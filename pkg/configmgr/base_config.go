package configmgr

// Config - config interface.
type Config interface {
	GetServiceName() string
	GetVersion() string
	GetEnvironment() string
	GetServerConfig() *ServerConfig
	GetGcpConfig() *GcpConfig
	GetLoggingConfig() *LoggingConfig
	IsLocalEnvironment() bool
}

// BaseConfig - app config struct.
// This struct represents the base configuration for the application and is expected to be in the following YAML format:
/*
name: "TestApp"
environment: "development"
version: "1.0"
logging:
  level: "debug"
gcp:
  projectNumber: 620222630834
  project: test-project
  location: europe-west4
image:
  repository: europe-west4-docker.pkg.dev/ingka-opoa-nexus-dev/nexus-docker-repo
  name: test-app
  pullPolicy: Always
  tag: latest
server:
  port: "8080"
  concurrency: 10
  disableStartupMsg: false
*/
type BaseConfig struct {
	Name        string         `mapstructure:"name"`
	Environment string         `mapstructure:"environment"`
	Version     string         `mapstructure:"version"`
	Logging     *LoggingConfig `mapstructure:"logging"`
	Server      *ServerConfig  `mapstructure:"server"`
	Gcp         *GcpConfig     `mapstructure:"gcp"`
	Image       *Image         `mapstructure:"image"`
}

type ServerConfig struct {
	Port                  string `mapstructure:"port"`
	Concurrency           int    `mapstructure:"concurrency"`
	DisableStartupMessage bool   `mapstructure:"disableStartupMsg"`
}

type LoggingConfig struct {
	Level string `mapstructure:"level"`
}

type GcpConfig struct {
	ProjectId     string `mapstructure:"project"`
	ProjectNumber string `mapstructure:"projectNumber"`
	Location      string `mapstructure:"location"`
}

// Image - Image properties
type Image struct {
	Repository string `mapstructure:"repository"`
	Name       string `mapstructure:"name"`
	PullPolicy string `mapstructure:"pullPolicy"`
	Tag        string `mapstructure:"tag"`
}

func (cfg BaseConfig) GetServiceName() string {
	return cfg.Name
}

func (cfg BaseConfig) GetVersion() string {
	return cfg.Version
}

func (cfg BaseConfig) GetEnvironment() string {
	return cfg.Environment
}

func (cfg BaseConfig) IsLocalEnvironment() bool {
	return checkIfLocalEnv(cfg.Environment)
}

func (cfg BaseConfig) GetServerConfig() *ServerConfig {
	return cfg.Server
}

func (cfg BaseConfig) GetGcpConfig() *GcpConfig {
	return cfg.Gcp
}

func (cfg BaseConfig) GetLoggingConfig() *LoggingConfig {
	return cfg.Logging
}
