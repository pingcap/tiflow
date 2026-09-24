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
	"testing"

	"github.com/pingcap/tiflow/pkg/logutil/redact"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type testStringer struct {
	str string
}

func (s *testStringer) String() string {
	return s.str
}

func TestInitLogRedaction(t *testing.T) {
	// Test with redaction disabled
	err := InitLogRedaction(false, "ON")
	require.NoError(t, err)
	require.False(t, IsRedactionEnabled())
	require.Equal(t, redact.RedactionModeOff, redact.GetRedactionMode())

	// Test with redaction enabled - ON mode
	err = InitLogRedaction(true, "ON")
	require.NoError(t, err)
	require.True(t, IsRedactionEnabled())
	require.Equal(t, redact.RedactionModeOn, redact.GetRedactionMode())

	// Test with redaction enabled - MARKER mode
	err = InitLogRedaction(true, "MARKER")
	require.NoError(t, err)
	require.True(t, IsRedactionEnabled())
	require.Equal(t, redact.RedactionModeMarker, redact.GetRedactionMode())

	// Test with invalid mode
	err = InitLogRedaction(true, "INVALID")
	require.Error(t, err)

	// Reset to OFF
	err = InitLogRedaction(false, "OFF")
	require.NoError(t, err)
}

func TestRedactStringIntegration(t *testing.T) {
	sensitiveData := "sensitive_data_123"

	// Test OFF mode
	err := InitLogRedaction(false, "OFF")
	require.NoError(t, err)
	result := RedactString(sensitiveData)
	require.Equal(t, sensitiveData, result)

	// Test ON mode
	err = InitLogRedaction(true, "ON")
	require.NoError(t, err)
	result = RedactString(sensitiveData)
	require.Equal(t, "?", result)

	// Test MARKER mode
	err = InitLogRedaction(true, "MARKER")
	require.NoError(t, err)
	result = RedactString(sensitiveData)
	require.Equal(t, "‹"+sensitiveData+"›", result)

	// Reset to OFF
	err = InitLogRedaction(false, "OFF")
	require.NoError(t, err)
}

func TestRedactKeyIntegration(t *testing.T) {
	sensitiveKey := []byte("sensitive_key_123")
	expectedHex := "53454E5349544956455F4B45595F313233"

	// Test OFF mode
	err := InitLogRedaction(false, "OFF")
	require.NoError(t, err)
	result := RedactKey(sensitiveKey)
	require.Equal(t, expectedHex, result)

	// Test ON mode
	err = InitLogRedaction(true, "ON")
	require.NoError(t, err)
	result = RedactKey(sensitiveKey)
	require.Equal(t, "?", result)

	// Test MARKER mode
	err = InitLogRedaction(true, "MARKER")
	require.NoError(t, err)
	result = RedactKey(sensitiveKey)
	require.Equal(t, "?", result)

	// Reset to OFF
	err = InitLogRedaction(false, "OFF")
	require.NoError(t, err)
}

func TestZapRedactionFields(t *testing.T) {
	sensitiveValue := "secret_value"

	// Test ZapRedactString
	err := InitLogRedaction(true, "ON")
	require.NoError(t, err)

	field := ZapRedactString("key", sensitiveValue)
	require.Equal(t, "key", field.Key)
	require.Equal(t, "?", field.String)

	// Test ZapRedactStringer
	stringer := &testStringer{sensitiveValue}
	field = ZapRedactStringer("key", stringer)
	require.Equal(t, "key", field.Key)

	// Test ZapRedactKey
	key := []byte(sensitiveValue)
	field = ZapRedactKey("key", key)
	require.Equal(t, "key", field.Key)
	require.Equal(t, "?", field.String)

	// Test with redaction OFF
	err = InitLogRedaction(false, "OFF")
	require.NoError(t, err)

	field = ZapRedactString("key", sensitiveValue)
	require.Equal(t, "key", field.Key)
	require.Equal(t, sensitiveValue, field.String)

	// Reset to OFF
	err = InitLogRedaction(false, "OFF")
	require.NoError(t, err)
}

func TestRedactionModeConsistency(t *testing.T) {
	// Test that all modes work consistently
	testValue := "test_value"

	modes := []struct {
		enabled bool
		mode    string
		expect  string
	}{
		{false, "OFF", "test_value"},
		{true, "ON", "?"},
		{true, "MARKER", "‹test_value›"},
	}

	for _, tc := range modes {
		err := InitLogRedaction(tc.enabled, tc.mode)
		require.NoError(t, err)

		// Test direct string redaction
		result := RedactString(testValue)
		require.Equal(t, tc.expect, result)

		// Test value redaction
		result = RedactValue(testValue)
		require.Equal(t, tc.expect, result)

		// Test enabled state
		require.Equal(t, tc.enabled, IsRedactionEnabled())
	}

	// Reset to OFF
	err := InitLogRedaction(false, "OFF")
	require.NoError(t, err)
}
