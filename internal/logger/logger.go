// Package logger provides a centralized logging interface for pgshadow.
// It wraps logrus and provides structured logging with configurable levels.
package logger

import (
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

// Level represents the logging verbosity level.
type Level string

const (
	LevelDebug   Level = "debug"
	LevelInfo    Level = "info"
	LevelWarn    Level = "warn"
	LevelError   Level = "error"
	LevelFatal   Level = "fatal"
	LevelUnknown Level = "unknown"
)

// ParseLevel converts a string level to logrus.Level.
func ParseLevel(level string) Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return LevelDebug
	case "info":
		return LevelInfo
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	case "fatal":
		return LevelFatal
	default:
		return LevelInfo // default to info
	}
}

// toLogrusLevel converts our Level type to logrus.Level.
func (l Level) toLogrusLevel() logrus.Level {
	switch l {
	case LevelDebug:
		return logrus.DebugLevel
	case LevelInfo:
		return logrus.InfoLevel
	case LevelWarn:
		return logrus.WarnLevel
	case LevelError:
		return logrus.ErrorLevel
	case LevelFatal:
		return logrus.FatalLevel
	default:
		return logrus.InfoLevel
	}
}

var (
	// log is the package-level logger instance.
	log = logrus.New()
	// initialized tracks whether the logger has been configured.
	initialized = false
)

// Init configures the logger with the specified level and format.
// It must be called before using the logger.
func Init(level Level, jsonFormat bool) {
	log.SetLevel(level.toLogrusLevel())
	
	if jsonFormat {
		log.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
		})
	} else {
		log.SetFormatter(&logrus.TextFormatter{
			TimestampFormat: "2006-01-02 15:04:05",
			FullTimestamp:   true,
		})
	}
	
	// Set output to stderr (default for server applications)
	log.SetOutput(os.Stderr)
	
	initialized = true
}

// InitFromEnv initializes the logger from environment variables.
// PGSHADOW_LOG_LEVEL: debug, info, warn, error, fatal (default: info)
// PGSHADOW_LOG_FORMAT: json, text (default: text)
func InitFromEnv() {
	levelStr := os.Getenv("PGSHADOW_LOG_LEVEL")
	if levelStr == "" {
		levelStr = "info"
	}
	
	formatStr := os.Getenv("PGSHADOW_LOG_FORMAT")
	jsonFormat := strings.ToLower(formatStr) == "json"
	
	Init(ParseLevel(levelStr), jsonFormat)
}

// EnsureInitialized ensures the logger is initialized.
// If not initialized, it initializes with defaults.
func EnsureInitialized() {
	if !initialized {
		Init(LevelInfo, false)
	}
}

// Debug logs a message at debug level.
func Debug(args ...interface{}) {
	EnsureInitialized()
	log.Debug(args...)
}

// Debugf logs a formatted message at debug level.
func Debugf(format string, args ...interface{}) {
	EnsureInitialized()
	log.Debugf(format, args...)
}

// Info logs a message at info level.
func Info(args ...interface{}) {
	EnsureInitialized()
	log.Info(args...)
}

// Infof logs a formatted message at info level.
func Infof(format string, args ...interface{}) {
	EnsureInitialized()
	log.Infof(format, args...)
}

// Warn logs a message at warn level.
func Warn(args ...interface{}) {
	EnsureInitialized()
	log.Warn(args...)
}

// Warnf logs a formatted message at warn level.
func Warnf(format string, args ...interface{}) {
	EnsureInitialized()
	log.Warnf(format, args...)
}

// Error logs a message at error level.
func Error(args ...interface{}) {
	EnsureInitialized()
	log.Error(args...)
}

// Errorf logs a formatted message at error level.
func Errorf(format string, args ...interface{}) {
	EnsureInitialized()
	log.Errorf(format, args...)
}

// Fatal logs a message at fatal level and exits.
func Fatal(args ...interface{}) {
	EnsureInitialized()
	log.Fatal(args...)
}

// Fatalf logs a formatted message at fatal level and exits.
func Fatalf(format string, args ...interface{}) {
	EnsureInitialized()
	log.Fatalf(format, args...)
}

// WithFields creates a logger with additional fields.
func WithFields(fields map[string]interface{}) *logrus.Entry {
	EnsureInitialized()
	return log.WithFields(fields)
}

// WithField creates a logger with a single additional field.
func WithField(key string, value interface{}) *logrus.Entry {
	EnsureInitialized()
	log.WithField(key, value)
	return log.WithField(key, value)
}

// GetLogger returns the underlying logrus instance for advanced usage.
func GetLogger() *logrus.Logger {
	EnsureInitialized()
	return log
}

// SetLevel sets the logging level dynamically.
func SetLevel(level Level) {
	EnsureInitialized()
	log.SetLevel(level.toLogrusLevel())
}

// GetLevel returns the current logging level.
func GetLevel() Level {
	EnsureInitialized()
	switch log.GetLevel() {
	case logrus.DebugLevel:
		return LevelDebug
	case logrus.InfoLevel:
		return LevelInfo
	case logrus.WarnLevel:
		return LevelWarn
	case logrus.ErrorLevel:
		return LevelError
	case logrus.FatalLevel:
		return LevelFatal
	default:
		return LevelInfo
	}
}

// LogStartup logs a startup message with component information.
func LogStartup(component, mode, dialect string) {
	Infof("pgshadow: startup complete (capture=%s queue=%s dialect=%s)",
		component, mode, dialect)
}

// LogShutdown logs a shutdown message.
func LogShutdown(reason string) {
	Info(fmt.Sprintf("pgshadow: shutdown initiated: %s", reason))
}

// LogError logs an error with context information.
func LogError(err error, context string) {
	EnsureInitialized()
	log.WithError(err).Error(context)
}

// LogPanic recovers from a panic and logs it.
func LogPanic() {
	if r := recover(); r != nil {
		log.WithField("panic", r).Error("recovered from panic")
	}
}
