//nolint:gochecknoglobals
package logx

import (
	"context"
	"log"
)

//// LogConfig represents a configuration structure
//type LogConfig struct {
//	ServiceName string
//	Environment string
//	LogLevel    string
//	Version     string
//	GCPProject  string
//}

type ServiceContext struct {
	Environment string `json:"environment"`
	Version     string `json:"version"`
}

// Logger - logger interface.
type Logger interface {
	// LogInfo logs a message at Info level.
	LogInfo(ctx context.Context, msg string)
	// LogDebug logs a message at Debug level.
	LogDebug(ctx context.Context, msg string)
	// LogWarning logs a message at Warning level.
	LogWarning(ctx context.Context, msg string, errs ...error)
	// LogError logs a message at Error level.
	LogError(ctx context.Context, msg string, errs ...error)
	// LogPanic logs a message at Panic level then panics.
	LogPanic(ctx context.Context, msg string, errs ...error)
	// LogFatal logs a message at Fatal Level.
	// The logger then calls os.Exit(1), even if logging at FatalLevel is
	// disabled.
	LogFatal(ctx context.Context, msg string, errs ...error)

	GetLogger() interface{}
}

// var lock sync.Mutex
var logger Logger

// DefaultLogger - Logger implementation that does nothing.
type DefaultLogger struct{}

// GetLogger - returns an instance of the Logger.
// If called before SetupLogger a no-op logger will be returned.
func GetLogger() Logger {
	if logger == nil {
		return &DefaultLogger{}
	}

	return logger
}

// LogInfo noop.
func (nl *DefaultLogger) LogInfo(ctx context.Context, msg string) {
	log.Println("INFO" + msg)
}

// LogDebug noop.
func (nl *DefaultLogger) LogDebug(ctx context.Context, msg string) {
	log.Println("DEBUG" + msg)
}

// LogWarning noop.
func (nl *DefaultLogger) LogWarning(ctx context.Context, msg string, errs ...error) {
	log.Println("WARN" + msg)
}

// LogError noop.
func (nl *DefaultLogger) LogError(ctx context.Context, msg string, errs ...error) {
	log.Println("ERROR" + msg)
}

// LogPanic noop.
func (nl *DefaultLogger) LogPanic(ctx context.Context, msg string, errs ...error) {
	log.Panicln("PANIC" + msg)
}

// LogFatal noop.
func (nl *DefaultLogger) LogFatal(ctx context.Context, msg string, errs ...error) {
	log.Fatalln("FATAL" + msg)
}

// GetLogger noop.
func (nl *DefaultLogger) GetLogger() interface{} { return nil }
