package logx

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/marcodd23/go-micro-core/pkg/configx"
	"github.com/rs/zerolog"
)

type ZeroLogWrapper struct {
	zeroLog            *zerolog.Logger
	projectName        string
	isLocalEnvironment bool
}

// SetupLogger sets up a new Logger with a service name override.
func SetupLogger(config configx.Config) Logger {

	// Set log level
	logLevel := zerolog.InfoLevel
	switch strings.ToLower(config.GetLoggingConfig().Level) {
	case "debug":
		logLevel = zerolog.DebugLevel
	case "info":
		logLevel = zerolog.InfoLevel
	case "warn":
		logLevel = zerolog.WarnLevel
	case "error":
		logLevel = zerolog.ErrorLevel
	}

	// Set zerolog global log level
	zerolog.SetGlobalLevel(logLevel)

	// Create zLog instance
	var zLog zerolog.Logger

	isLocalEnvironment := true

	switch strings.ToUpper(config.GetEnvironment()) {
	case "DEV", "STAGE", "PROD":
		isLocalEnvironment = false
		zLog = zerolog.New(os.Stdout).With().Timestamp().Logger()
	default:
		zLog = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	}

	// Add common fields
	zLog = zLog.With().
		Str("service", config.GetServiceName()).
		Str("project", config.GetGcpConfig().ProjectId).
		Interface("serviceContext", ServiceContext{Environment: config.GetEnvironment(), Version: config.GetVersion()}).
		Logger()

	logger = &ZeroLogWrapper{
		&zLog,
		config.GetGcpConfig().ProjectId,
		isLocalEnvironment,
	}
	return logger
}

func (lm *ZeroLogWrapper) logWithContext(ctx context.Context, level zerolog.Level, errs []error, msg string) {
	logEvent := lm.zeroLog.WithLevel(level)

	switch level {
	case zerolog.DebugLevel:
		logEvent = logEvent.Str("severity", "DEBUG")
	case zerolog.InfoLevel:
		logEvent = logEvent.Str("severity", "INFO")
	case zerolog.WarnLevel:
		logEvent = logEvent.Str("severity", "WARNING")
	case zerolog.ErrorLevel:
		logEvent = logEvent.Str("severity", "ERROR")
	case zerolog.FatalLevel:
		logEvent = logEvent.Str("severity", "CRITICAL")
	case zerolog.PanicLevel:
		logEvent = logEvent.Str("severity", "CRITICAL")
	}

	for _, err := range errs {
		logEvent = logEvent.Err(err)
	}

	logEvent.Msg(msg)
}

func (lm *ZeroLogWrapper) LogInfo(ctx context.Context, msg string) {
	lm.logWithContext(ctx, zerolog.InfoLevel, nil, msg)
}

func (lm *ZeroLogWrapper) LogDebug(ctx context.Context, msg string) {
	lm.logWithContext(ctx, zerolog.DebugLevel, nil, msg)
}

func (lm *ZeroLogWrapper) LogWarning(ctx context.Context, msg string, errs ...error) {
	lm.logWithContext(ctx, zerolog.WarnLevel, errs, msg)
}

func (lm *ZeroLogWrapper) LogError(ctx context.Context, msg string, errs ...error) {
	lm.logWithContext(ctx, zerolog.ErrorLevel, errs, msg)
}

func (lm *ZeroLogWrapper) LogPanic(ctx context.Context, msg string, errs ...error) {
	lm.logWithContext(ctx, zerolog.PanicLevel, errs, msg)
}

func (lm *ZeroLogWrapper) LogFatal(ctx context.Context, msg string, errs ...error) {
	lm.logWithContext(ctx, zerolog.FatalLevel, errs, msg)
}

// GetLogger - returns the underlying logger.
func (lm *ZeroLogWrapper) GetLogger() interface{} {
	return lm.zeroLog
}
