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

package redact

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type testStringer struct {
	str string
}

func (s *testStringer) String() string {
	return s.str
}

func TestRedactionMode(t *testing.T) {
	// Test RedactionMode string representation
	require.Equal(t, "OFF", RedactionModeOff.String())
	require.Equal(t, "ON", RedactionModeOn.String())
	require.Equal(t, "MARKER", RedactionModeMarker.String())

	// Test parsing
	mode, err := ParseRedactionMode("OFF")
	require.NoError(t, err)
	require.Equal(t, RedactionModeOff, mode)

	mode, err = ParseRedactionMode("ON")
	require.NoError(t, err)
	require.Equal(t, RedactionModeOn, mode)

	mode, err = ParseRedactionMode("MARKER")
	require.NoError(t, err)
	require.Equal(t, RedactionModeMarker, mode)

	mode, err = ParseRedactionMode("TRUE")
	require.NoError(t, err)
	require.Equal(t, RedactionModeOn, mode)

	mode, err = ParseRedactionMode("FALSE")
	require.NoError(t, err)
	require.Equal(t, RedactionModeOff, mode)

	// Test invalid mode
	_, err = ParseRedactionMode("INVALID")
	require.Error(t, err)
}

func TestRedactString(t *testing.T) {
	testCases := []struct {
		mode   RedactionMode
		input  string
		output string
	}{
		{RedactionModeOff, "sensitive data", "sensitive data"},
		{RedactionModeOff, "f‹xcv", "f‹xcv"},
		{RedactionModeOn, "sensitive data", "?"},
		{RedactionModeOn, "f‹xcv", "?"},
		{RedactionModeMarker, "sensitive data", "‹sensitive data›"},
		{RedactionModeMarker, "f‹xcv", "‹f‹‹xcv›"},
		{RedactionModeMarker, "f›xcv", "‹f››xcv›"},
	}

	for _, tc := range testCases {
		// Test StringWithMode
		result := StringWithMode(tc.mode, tc.input)
		require.Equal(t, tc.output, result, "StringWithMode failed for mode %v, input %s", tc.mode, tc.input)

		// Test global mode
		SetRedactionMode(tc.mode)
		result = String(tc.input)
		require.Equal(t, tc.output, result, "String failed for mode %v, input %s", tc.mode, tc.input)

		// Test stringer
		stringer := &testStringer{tc.input}
		result = StringerWithMode(tc.mode, stringer).String()
		require.Equal(t, tc.output, result, "StringerWithMode failed for mode %v, input %s", tc.mode, tc.input)

		SetRedactionMode(tc.mode)
		result = Stringer(stringer).String()
		require.Equal(t, tc.output, result, "Stringer failed for mode %v, input %s", tc.mode, tc.input)
	}
}

func TestRedactionModeSettings(t *testing.T) {
	// Test initial state
	originalMode := GetRedactionMode()

	// Test setting and getting modes
	SetRedactionMode(RedactionModeOn)
	require.Equal(t, RedactionModeOn, GetRedactionMode())
	require.True(t, IsRedactionEnabled())

	SetRedactionMode(RedactionModeOff)
	require.Equal(t, RedactionModeOff, GetRedactionMode())
	require.False(t, IsRedactionEnabled())

	SetRedactionMode(RedactionModeMarker)
	require.Equal(t, RedactionModeMarker, GetRedactionMode())
	require.True(t, IsRedactionEnabled())

	// Restore original mode
	SetRedactionMode(originalMode)
}

func TestRedactKey(t *testing.T) {
	key := []byte("sensitive_key")
	expectedHex := strings.ToUpper(hex.EncodeToString(key))

	SetRedactionMode(RedactionModeOff)
	result := Key(key)
	require.Equal(t, expectedHex, result)

	SetRedactionMode(RedactionModeOn)
	result = Key(key)
	require.Equal(t, "?", result)

	SetRedactionMode(RedactionModeMarker)
	result = Key(key)
	require.Equal(t, "?", result)

	// Reset to off
	SetRedactionMode(RedactionModeOff)
}

func TestRedactValue(t *testing.T) {
	value := "sensitive_value"

	SetRedactionMode(RedactionModeOff)
	result := Value(value)
	require.Equal(t, value, result)

	SetRedactionMode(RedactionModeOn)
	result = Value(value)
	require.Equal(t, "?", result)

	SetRedactionMode(RedactionModeMarker)
	result = Value(value)
	require.Equal(t, "‹"+value+"›", result)

	// Reset to off
	SetRedactionMode(RedactionModeOff)
}

func TestZapFields(t *testing.T) {
	SetRedactionMode(RedactionModeOn)

	// Test ZapString
	field := ZapString("key", "value")
	require.Equal(t, "key", field.Key)
	require.Equal(t, "?", field.String)

	// Test ZapKey
	keyBytes := []byte("test_key")
	field = ZapKey("key", keyBytes)
	require.Equal(t, "key", field.Key)
	require.Equal(t, "?", field.String)

	// Test ZapStringer
	stringer := &testStringer{"test_value"}
	field = ZapStringer("key", stringer)
	require.Equal(t, "key", field.Key)

	// Reset to off
	SetRedactionMode(RedactionModeOff)
}

func TestDeRedact(t *testing.T) {
	testCases := []struct {
		remove bool
		input  string
		output string
	}{
		{true, "‹sensitive›data", "?data"},
		{false, "‹sensitive›data", "sensitivedata"},
		{true, "normal data", "normal data"},
		{false, "normal data", "normal data"},
		{true, "‹first›and‹second›", "?and?"},
		{false, "‹first›and‹second›", "firstandsecond"},
		{true, "‹›", "?"},
		{false, "‹›", ""},
		{true, "incomplete‹", "incomplete‹"},
		{false, "incomplete‹", "incomplete‹"},
		{true, "incomplete›", "incomplete›"},
		{false, "incomplete›", "incomplete›"},
		{true, "‹‹doubled", "‹doubled"},
		{false, "‹‹doubled", "‹doubled"},
		{true, "doubled››", "doubled››"},
		{false, "doubled››", "doubled››"},
	}

	for _, tc := range testCases {
		input := strings.NewReader(tc.input)
		output := &bytes.Buffer{}
		err := DeRedact(tc.remove, input, output, "")
		require.NoError(t, err)
		require.Equal(t, tc.output, output.String(),
			"DeRedact(remove=%v) failed for input '%s'", tc.remove, tc.input)
	}
}

func TestWriteRedact(t *testing.T) {
	testCases := []struct {
		mode   RedactionMode
		input  string
		output string
	}{
		{RedactionModeOff, "test", "test"},
		{RedactionModeOn, "test", "?"},
		{RedactionModeMarker, "test", "‹test›"},
	}

	for _, tc := range testCases {
		builder := &strings.Builder{}
		WriteRedact(builder, tc.input, tc.mode)
		require.Equal(t, tc.output, builder.String())
	}
}
