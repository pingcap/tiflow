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

package logutil

import (
	"encoding/json"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tiflow/pkg/logutil/redact"
	"github.com/stretchr/testify/require"
)

func TestRedactInfoLogType_String(t *testing.T) {
	testCases := []struct {
		input    RedactInfoLogType
		expected string
	}{
		{RedactInfoLogOFF, "OFF"},
		{RedactInfoLogON, "ON"},
		{RedactInfoLogMarker, "MARKER"},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.expected, tc.input.String())
	}
}

func TestRedactInfoLogType_UnmarshalJSON(t *testing.T) {
	testCases := []struct {
		input    string
		expected RedactInfoLogType
		hasError bool
	}{
		{`false`, RedactInfoLogOFF, false},
		{`true`, RedactInfoLogON, false},
		{`"MARKER"`, RedactInfoLogMarker, false},
		{`"marker"`, RedactInfoLogMarker, false},
		{`"Marker"`, RedactInfoLogMarker, false},
		{`"invalid"`, RedactInfoLogOFF, true},
		{`123`, RedactInfoLogOFF, true},
		{`null`, RedactInfoLogOFF, true},
	}

	for _, tc := range testCases {
		var result RedactInfoLogType
		err := json.Unmarshal([]byte(tc.input), &result)
		if tc.hasError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		}
	}
}

func TestRedactInfoLogType_MarshalJSON(t *testing.T) {
	testCases := []struct {
		input    RedactInfoLogType
		expected string
	}{
		{RedactInfoLogOFF, `false`},
		{RedactInfoLogON, `true`},
		{RedactInfoLogMarker, `"MARKER"`},
	}

	for _, tc := range testCases {
		result, err := json.Marshal(tc.input)
		require.NoError(t, err)
		require.Equal(t, tc.expected, string(result))
	}
}

func TestRedactInfoLogType_UnmarshalTOML(t *testing.T) {
	testCases := []struct {
		input    interface{}
		expected RedactInfoLogType
		hasError bool
	}{
		{false, RedactInfoLogOFF, false},
		{true, RedactInfoLogON, false},
		{"MARKER", RedactInfoLogMarker, false},
		{"marker", RedactInfoLogMarker, false},
		{"invalid", RedactInfoLogOFF, true},
		{123, RedactInfoLogOFF, true},
	}

	for _, tc := range testCases {
		var result RedactInfoLogType
		err := result.UnmarshalTOML(tc.input)
		if tc.hasError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		}
	}
}

func TestRedactInfoLogType_UnmarshalText(t *testing.T) {
	testCases := []struct {
		input    string
		expected RedactInfoLogType
		hasError bool
	}{
		{"false", RedactInfoLogOFF, false},
		{"FALSE", RedactInfoLogOFF, false},
		{"off", RedactInfoLogOFF, false},
		{"OFF", RedactInfoLogOFF, false},
		{"0", RedactInfoLogOFF, false},
		{"true", RedactInfoLogON, false},
		{"TRUE", RedactInfoLogON, false},
		{"on", RedactInfoLogON, false},
		{"ON", RedactInfoLogON, false},
		{"1", RedactInfoLogON, false},
		{"MARKER", RedactInfoLogMarker, false},
		{"marker", RedactInfoLogMarker, false},
		{"Marker", RedactInfoLogMarker, false},
		{"invalid", RedactInfoLogOFF, true},
	}

	for _, tc := range testCases {
		var result RedactInfoLogType
		err := result.UnmarshalText([]byte(tc.input))
		if tc.hasError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		}
	}
}

func TestRedactInfoLogType_ToRedactionMode(t *testing.T) {
	testCases := []struct {
		input    RedactInfoLogType
		expected redact.RedactionMode
	}{
		{RedactInfoLogOFF, redact.RedactionModeOff},
		{RedactInfoLogON, redact.RedactionModeOn},
		{RedactInfoLogMarker, redact.RedactionModeMarker},
	}

	for _, tc := range testCases {
		result := tc.input.ToRedactionMode()
		require.Equal(t, tc.expected, result)
	}
}

func TestFromRedactionMode(t *testing.T) {
	testCases := []struct {
		input    redact.RedactionMode
		expected RedactInfoLogType
	}{
		{redact.RedactionModeOff, RedactInfoLogOFF},
		{redact.RedactionModeOn, RedactInfoLogON},
		{redact.RedactionModeMarker, RedactInfoLogMarker},
	}

	for _, tc := range testCases {
		result := FromRedactionMode(tc.input)
		require.Equal(t, tc.expected, result)
	}
}

func TestParseRedactInfoLog(t *testing.T) {
	testCases := []struct {
		input    string
		expected RedactInfoLogType
		hasError bool
	}{
		{"false", RedactInfoLogOFF, false},
		{"true", RedactInfoLogON, false},
		{"MARKER", RedactInfoLogMarker, false},
		{"invalid", RedactInfoLogOFF, true},
	}

	for _, tc := range testCases {
		result, err := ParseRedactInfoLog(tc.input)
		if tc.hasError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		}
	}
}

func TestRedactInfoLogFromBoolAndMode(t *testing.T) {
	testCases := []struct {
		enabled  bool
		mode     string
		expected RedactInfoLogType
	}{
		{false, "ON", RedactInfoLogOFF},
		{false, "MARKER", RedactInfoLogOFF},
		{true, "ON", RedactInfoLogON},
		{true, "MARKER", RedactInfoLogMarker},
		{true, "invalid", RedactInfoLogON},
		{true, "", RedactInfoLogON},
	}

	for _, tc := range testCases {
		result := RedactInfoLogFromBoolAndMode(tc.enabled, tc.mode)
		require.Equal(t, tc.expected, result)
	}
}

func TestTOMLIntegration(t *testing.T) {
	type TestConfig struct {
		RedactInfoLog RedactInfoLogType `toml:"redact-info-log"`
	}

	testCases := []struct {
		tomlContent string
		expected    RedactInfoLogType
		hasError    bool
	}{
		{`redact-info-log = false`, RedactInfoLogOFF, false},
		{`redact-info-log = true`, RedactInfoLogON, false},
		{`redact-info-log = "MARKER"`, RedactInfoLogMarker, false},
		{`redact-info-log = "marker"`, RedactInfoLogMarker, false},
	}

	for _, tc := range testCases {
		var config TestConfig
		err := toml.Unmarshal([]byte(tc.tomlContent), &config)
		if tc.hasError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.expected, config.RedactInfoLog)
		}
	}
}

func TestJSONIntegration(t *testing.T) {
	type TestConfig struct {
		RedactInfoLog RedactInfoLogType `json:"redact-info-log"`
	}

	testCases := []struct {
		jsonContent string
		expected    RedactInfoLogType
		hasError    bool
	}{
		{`{"redact-info-log": false}`, RedactInfoLogOFF, false},
		{`{"redact-info-log": true}`, RedactInfoLogON, false},
		{`{"redact-info-log": "MARKER"}`, RedactInfoLogMarker, false},
		{`{"redact-info-log": "marker"}`, RedactInfoLogMarker, false},
	}

	for _, tc := range testCases {
		var config TestConfig
		err := json.Unmarshal([]byte(tc.jsonContent), &config)
		if tc.hasError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.expected, config.RedactInfoLog)
		}
	}
}

func TestInitLogRedactionFromType(t *testing.T) {
	testCases := []RedactInfoLogType{
		RedactInfoLogOFF,
		RedactInfoLogON,
		RedactInfoLogMarker,
	}

	for _, tc := range testCases {
		err := InitLogRedactionFromType(tc)
		require.NoError(t, err)

		// Verify the global redaction mode was set correctly
		expectedMode := tc.ToRedactionMode()
		actualMode := redact.GetRedactionMode()
		require.Equal(t, expectedMode, actualMode)
	}
}

func TestValidateRedactInfoLog(t *testing.T) {
	validValues := []string{"false", "true", "MARKER", "marker", "TRUE", "FALSE", "OFF", "ON", "0", "1"}
	for _, val := range validValues {
		err := ValidateRedactInfoLog(val)
		require.NoError(t, err, "should be valid: %s", val)
	}

	invalidValues := []string{"invalid", "2", "yes", "no", "maybe"}
	for _, val := range invalidValues {
		err := ValidateRedactInfoLog(val)
		require.Error(t, err, "should be invalid: %s", val)
	}
}
