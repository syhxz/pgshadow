package logger

import (
	"testing"

	"github.com/sirupsen/logrus"
)

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected Level
	}{
		{"debug", LevelDebug},
		{"DEBUG", LevelDebug},
		{"info", LevelInfo},
		{"INFO", LevelInfo},
		{"warn", LevelWarn},
		{"WARN", LevelWarn},
		{"warning", LevelWarn},
		{"error", LevelError},
		{"ERROR", LevelError},
		{"fatal", LevelFatal},
		{"FATAL", LevelFatal},
		{"", LevelInfo},           // empty defaults to info
		{"invalid", LevelInfo},    // unknown defaults to info
		{"  info  ", LevelInfo},   // trimmed
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result := ParseLevel(tc.input)
			if result != tc.expected {
				t.Errorf("ParseLevel(%q) = %v; want %v", tc.input, result, tc.expected)
			}
		})
	}
}

func TestLevelToLogrusLevel(t *testing.T) {
	tests := []struct {
		input    Level
		expected logrus.Level
	}{
		{LevelDebug, logrus.DebugLevel},
		{LevelInfo, logrus.InfoLevel},
		{LevelWarn, logrus.WarnLevel},
		{LevelError, logrus.ErrorLevel},
		{LevelFatal, logrus.FatalLevel},
		{LevelUnknown, logrus.InfoLevel}, // unknown defaults to info
	}

	for _, tc := range tests {
		t.Run(string(tc.input), func(t *testing.T) {
			result := tc.input.toLogrusLevel()
			if result != tc.expected {
				t.Errorf("%v.toLogrusLevel() = %v; want %v", tc.input, result, tc.expected)
			}
		})
	}
}

func TestInit(t *testing.T) {
	// Test basic initialization
	Init(LevelDebug, false)
	if log.GetLevel() != logrus.DebugLevel {
		t.Errorf("Init(LevelDebug) - got level %v, want DebugLevel", log.GetLevel())
	}

	// Test JSON format
	Init(LevelInfo, true)
	if log.GetLevel() != logrus.InfoLevel {
		t.Errorf("Init(LevelInfo, true) - got level %v, want InfoLevel", log.GetLevel())
	}
}

func TestSetLevel(t *testing.T) {
	Init(LevelInfo, false)

	SetLevel(LevelDebug)
	if log.GetLevel() != logrus.DebugLevel {
		t.Errorf("SetLevel(LevelDebug) - got level %v, want DebugLevel", log.GetLevel())
	}

	SetLevel(LevelError)
	if log.GetLevel() != logrus.ErrorLevel {
		t.Errorf("SetLevel(LevelError) - got level %v, want ErrorLevel", log.GetLevel())
	}
}

func TestGetLevel(t *testing.T) {
	Init(LevelWarn, false)
	level := GetLevel()
	if level != LevelWarn {
		t.Errorf("GetLevel() = %v; want %v", level, LevelWarn)
	}
}

func TestEnsureInitialized(t *testing.T) {
	// Reset the initialized flag to test EnsureInitialized
	initialized = false

	// Should initialize with defaults
	EnsureInitialized()
	if !initialized {
		t.Error("EnsureInitialized() - logger should be initialized")
	}
}

func TestWithFields(t *testing.T) {
	Init(LevelInfo, false)

	fields := map[string]interface{}{
		"component": "capture",
		"mode":      "pcap",
	}
	entry := WithFields(fields)
	if entry == nil {
		t.Error("WithFields() returned nil")
	}
}

func TestLogStartup(t *testing.T) {
	Init(LevelInfo, false)
	// Should not panic
	LogStartup("ebpf", "kafka", "postgresql")
}

func TestLogShutdown(t *testing.T) {
	Init(LevelInfo, false)
	// Should not panic
	LogShutdown("SIGTERM received")
}

func TestLogError(t *testing.T) {
	Init(LevelInfo, false)
	// Should not panic
	LogError(nil, "test context")
}
