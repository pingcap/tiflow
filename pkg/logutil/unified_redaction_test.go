// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build integration

package logutil

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

// TestUnifiedRedactInfoLogConfiguration tests that both DM and TiCDC can use the same
// redact-info-log configuration approach following PD's pattern
func TestUnifiedRedactInfoLogConfiguration(t *testing.T) {
	testCases := []struct {
		name              string
		configValue       interface{}
		expectedMode      RedactInfoLogType
		expectedLogOutput func(sensitive string) string
	}{
		{
			name:              "Boolean false disables redaction",
			configValue:       false,
			expectedMode:      RedactInfoLogOFF,
			expectedLogOutput: func(s string) string { return s },
		},
		{
			name:              "Boolean true enables redaction",
			configValue:       true,
			expectedMode:      RedactInfoLogON,
			expectedLogOutput: func(s string) string { return "?" },
		},
		{
			name:              "String MARKER enables marker redaction",
			configValue:       "MARKER",
			expectedMode:      RedactInfoLogMarker,
			expectedLogOutput: func(s string) string { return "‹" + s + "›" },
		},
		{
			name:              "String marker (lowercase) enables marker redaction",
			configValue:       "marker",
			expectedMode:      RedactInfoLogMarker,
			expectedLogOutput: func(s string) string { return "‹" + s + "›" },
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test TOML unmarshaling (like config files)
			var redactType RedactInfoLogType
			err := redactType.UnmarshalTOML(tc.configValue)
			require.NoError(t, err)
			require.Equal(t, tc.expectedMode, redactType)

			// Test redaction functionality
			err = InitLogRedactionFromType(redactType)
			require.NoError(t, err)

			sensitiveData := "SELECT * FROM users WHERE password = 'secret'"
			redactedString := RedactString(sensitiveData)
			expectedOutput := tc.expectedLogOutput(sensitiveData)

			if tc.expectedMode == RedactInfoLogMarker {
				// For marker mode, verify it contains the markers
				require.Contains(t, redactedString, "‹")
				require.Contains(t, redactedString, "›")
				require.Contains(t, redactedString, sensitiveData)
			} else {
				require.Equal(t, expectedOutput, redactedString)
			}
		})
	}
}

// TestDMWorkerConfigCompatibility tests DM worker config integration
func TestDMWorkerConfigCompatibility(t *testing.T) {
	type DMWorkerConfig struct {
		RedactInfoLog RedactInfoLogType `toml:"redact-info-log" json:"redact-info-log"`
	}

	testConfigs := []struct {
		name       string
		tomlConfig string
		expected   RedactInfoLogType
	}{
		{
			name:       "DM worker with false",
			tomlConfig: `redact-info-log = false`,
			expected:   RedactInfoLogOFF,
		},
		{
			name:       "DM worker with true",
			tomlConfig: `redact-info-log = true`,
			expected:   RedactInfoLogON,
		},
		{
			name:       "DM worker with MARKER",
			tomlConfig: `redact-info-log = "MARKER"`,
			expected:   RedactInfoLogMarker,
		},
	}

	for _, tc := range testConfigs {
		t.Run(tc.name, func(t *testing.T) {
			var config DMWorkerConfig
			err := toml.Unmarshal([]byte(tc.tomlConfig), &config)
			require.NoError(t, err)
			require.Equal(t, tc.expected, config.RedactInfoLog)
		})
	}
}

// TestTiCDCServerConfigCompatibility tests TiCDC server config integration
func TestTiCDCServerConfigCompatibility(t *testing.T) {
	type LogConfig struct {
		RedactInfoLog RedactInfoLogType `toml:"redact-info-log" json:"redact-info-log"`
	}

	type TiCDCServerConfig struct {
		Log LogConfig `toml:"log" json:"log"`
	}

	testConfigs := []struct {
		name       string
		tomlConfig string
		expected   RedactInfoLogType
	}{
		{
			name: "TiCDC server with false",
			tomlConfig: `
[log]
redact-info-log = false`,
			expected: RedactInfoLogOFF,
		},
		{
			name: "TiCDC server with true",
			tomlConfig: `
[log]
redact-info-log = true`,
			expected: RedactInfoLogON,
		},
		{
			name: "TiCDC server with MARKER",
			tomlConfig: `
[log]
redact-info-log = "MARKER"`,
			expected: RedactInfoLogMarker,
		},
	}

	for _, tc := range testConfigs {
		t.Run(tc.name, func(t *testing.T) {
			var config TiCDCServerConfig
			err := toml.Unmarshal([]byte(tc.tomlConfig), &config)
			require.NoError(t, err)
			require.Equal(t, tc.expected, config.Log.RedactInfoLog)
		})
	}
}

// TestJSONCompatibility tests JSON configuration compatibility
func TestJSONCompatibility(t *testing.T) {
	type Config struct {
		RedactInfoLog RedactInfoLogType `json:"redact-info-log"`
	}

	testCases := []struct {
		name       string
		jsonConfig string
		expected   RedactInfoLogType
	}{
		{
			name:       "JSON with false",
			jsonConfig: `{"redact-info-log": false}`,
			expected:   RedactInfoLogOFF,
		},
		{
			name:       "JSON with true",
			jsonConfig: `{"redact-info-log": true}`,
			expected:   RedactInfoLogON,
		},
		{
			name:       "JSON with MARKER",
			jsonConfig: `{"redact-info-log": "MARKER"}`,
			expected:   RedactInfoLogMarker,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var config Config
			err := json.Unmarshal([]byte(tc.jsonConfig), &config)
			require.NoError(t, err)
			require.Equal(t, tc.expected, config.RedactInfoLog)
		})
	}
}

// TestRedactInfoLogWithLogging tests actual logging behavior with different redaction modes
func TestRedactInfoLogWithLogging(t *testing.T) {
	testCases := []RedactInfoLogType{
		RedactInfoLogOFF,
		RedactInfoLogON,
		RedactInfoLogMarker,
	}

	for _, mode := range testCases {
		t.Run(mode.String(), func(t *testing.T) {
			// Create a test logger that captures output
			buf := &zaptest.Buffer{}
			logger := zap.New(zapcore.NewCore(
				zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig()),
				buf,
				zapcore.DebugLevel,
			))

			// Replace global logger for test
			originalLogger := log.L()
			log.ReplaceGlobals(logger, &log.ZapProperties{Level: zap.NewAtomicLevelAt(zapcore.DebugLevel)})
			defer func() {
				log.ReplaceGlobals(originalLogger, nil)
			}()

			// Initialize redaction with the test mode
			err := InitLogRedactionFromType(mode)
			require.NoError(t, err)

			sensitiveSQL := "UPDATE users SET password = 'newpass' WHERE id = 123"
			sensitiveKey := []byte("user_secret_key")

			buf.Reset()
			log.Info("Database operation",
				ZapRedactString("sql", sensitiveSQL),
				ZapRedactKey("key", sensitiveKey))

			output := buf.String()

			switch mode {
			case RedactInfoLogOFF:
				require.Contains(t, output, sensitiveSQL)
				require.Contains(t, output, "USER_SECRET_KEY") // hex encoded key
			case RedactInfoLogON:
				require.Contains(t, output, `"sql":"?"`)
				require.Contains(t, output, `"key":"?"`)
				require.NotContains(t, output, sensitiveSQL)
				require.NotContains(t, output, "USER_SECRET_KEY")
			case RedactInfoLogMarker:
				require.Contains(t, output, "‹"+sensitiveSQL+"›")
				require.Contains(t, output, `"key":"?"`) // Keys are always ? for security
				require.NotContains(t, output, "USER_SECRET_KEY")
			}
		})
	}
}

// TestBackwardCompatibilityHelper tests the helper for converting legacy configs
func TestBackwardCompatibilityHelper(t *testing.T) {
	testCases := []struct {
		enabled  bool
		mode     string
		expected RedactInfoLogType
	}{
		{false, "ON", RedactInfoLogOFF},
		{false, "MARKER", RedactInfoLogOFF},
		{true, "ON", RedactInfoLogON},
		{true, "MARKER", RedactInfoLogMarker},
		{true, "invalid", RedactInfoLogON}, // defaults to ON
		{true, "", RedactInfoLogON},        // defaults to ON
	}

	for _, tc := range testCases {
		result := RedactInfoLogFromBoolAndMode(tc.enabled, tc.mode)
		require.Equal(t, tc.expected, result,
			"RedactInfoLogFromBoolAndMode(%v, %q) should return %v", tc.enabled, tc.mode, tc.expected)
	}
}

// TestCommandLineFlagCompatibility tests flag.Var implementation
func TestCommandLineFlagCompatibility(t *testing.T) {
	var redactType RedactInfoLogType

	// Test setting from command line arguments
	testCases := []struct {
		value    string
		expected RedactInfoLogType
		hasError bool
	}{
		{"false", RedactInfoLogOFF, false},
		{"true", RedactInfoLogON, false},
		{"MARKER", RedactInfoLogMarker, false},
		{"marker", RedactInfoLogMarker, false},
		{"invalid", RedactInfoLogOFF, true},
	}

	for _, tc := range testCases {
		err := redactType.Set(tc.value)
		if tc.hasError {
			require.Error(t, err, "Set(%q) should fail", tc.value)
		} else {
			require.NoError(t, err, "Set(%q) should succeed", tc.value)
			require.Equal(t, tc.expected, redactType, "Set(%q) should set to %v", tc.value, tc.expected)
		}
	}

	// Test string representation for flag help
	redactType = RedactInfoLogMarker
	require.Equal(t, "MARKER", redactType.String())
}

// Add the Set method to RedactInfoLogType to satisfy flag.Value interface
func (t *RedactInfoLogType) Set(s string) error {
	return t.UnmarshalText([]byte(s))
}
