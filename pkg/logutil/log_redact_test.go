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
	"fmt"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/logutil/redact"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

type testStringer struct {
	str string
}

func (s *testStringer) String() string {
	return s.str
}

func TestZapRedactionFields(t *testing.T) {
	sensitiveValue := "secret_value"

	// Test ZapRedactString
	err := InitLogRedactionFromType(RedactInfoLogON)
	require.NoError(t, err)

	field := ZapRedactString("key", sensitiveValue)
	require.Equal(t, "key", field.Key)
	require.Equal(t, "?", field.String)

	// Test with redaction OFF
	err = InitLogRedactionFromType(RedactInfoLogOFF)
	require.NoError(t, err)

	field = ZapRedactString("key", sensitiveValue)
	require.Equal(t, "key", field.Key)
	require.Equal(t, sensitiveValue, field.String)

	// Reset to OFF
	err = InitLogRedactionFromType(RedactInfoLogOFF)
	require.NoError(t, err)
}

func TestZapAny(t *testing.T) {
	testCases := []struct {
		name     string
		mode     RedactInfoLogType
		key      string
		value    interface{}
		expected string
	}{
		// RedactInfoLogOFF tests
		{
			name:     "off mode with string",
			mode:     RedactInfoLogOFF,
			key:      "test",
			value:    "sensitive data",
			expected: "sensitive data",
		},
		{
			name:     "off mode with int",
			mode:     RedactInfoLogOFF,
			key:      "count",
			value:    42,
			expected: "42",
		},
		{
			name:     "off mode with struct",
			mode:     RedactInfoLogOFF,
			key:      "data",
			value:    struct{ Name string }{"Alice"},
			expected: "{\\\"Name\\\":\\\"Alice\\\"}",
		},
		// RedactInfoLogON tests
		{
			name:     "on mode with string",
			mode:     RedactInfoLogON,
			key:      "test",
			value:    "sensitive data",
			expected: "?",
		},
		{
			name:     "on mode with int",
			mode:     RedactInfoLogON,
			key:      "count",
			value:    42,
			expected: "?",
		},
		{
			name:     "on mode with struct",
			mode:     RedactInfoLogON,
			key:      "data",
			value:    struct{ Name string }{"Alice"},
			expected: "?",
		},
		// RedactInfoLogMarker tests
		{
			name:     "marker mode with string",
			mode:     RedactInfoLogMarker,
			key:      "test",
			value:    "sensitive",
			expected: "‹sensitive›",
		},
		{
			name:     "marker mode with special chars",
			mode:     RedactInfoLogMarker,
			key:      "test",
			value:    "f‹xcv",
			expected: "‹f‹‹xcv›",
		},
		{
			name:     "marker mode with int",
			mode:     RedactInfoLogMarker,
			key:      "count",
			value:    42,
			expected: "‹42›",
		},
		{
			name:     "marker mode with struct",
			mode:     RedactInfoLogMarker,
			key:      "data",
			value:    struct{ Name string }{"Alice"},
			expected: "‹{Alice}›",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := InitLogRedactionFromType(tc.mode)
			require.NoError(t, err)
			var buffer zaptest.Buffer
			err = InitLogger(&Config{Level: "info"}, WithOutputWriteSyncer(&buffer))
			require.NoError(t, err)
			log.Info("ZapRedactAnyTest", ZapRedactAny(tc.key, tc.value))
			require.Contains(t, buffer.String(), tc.expected)
			require.Contains(t, buffer.String(), tc.key)
			buffer.Reset()
		})
	}
}

func TestZapStringer(t *testing.T) {
	testCases := []struct {
		name     string
		mode     RedactInfoLogType
		key      string
		value    fmt.Stringer
		expected string
	}{
		{
			name:     "off mode",
			mode:     RedactInfoLogOFF,
			key:      "keyOffMode",
			value:    &testStringer{"sensitive data"},
			expected: "sensitive data",
		},
		{
			name:     "on mode",
			mode:     RedactInfoLogON,
			key:      "keyOnMode",
			value:    &testStringer{"sensitive data"},
			expected: "?",
		},
		{
			name:     "marker mode",
			mode:     RedactInfoLogMarker,
			key:      "keyMarkerMode",
			value:    &testStringer{"sensitive data"},
			expected: "‹sensitive data›",
		},
		{
			name:     "marker mode with special chars",
			mode:     RedactInfoLogMarker,
			key:      "keySpecial",
			value:    &testStringer{"f‹xcv"},
			expected: "‹f‹‹xcv›",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := InitLogRedactionFromType(tc.mode)
			require.NoError(t, err)
			var buffer zaptest.Buffer
			err = InitLogger(&Config{Level: "info"}, WithOutputWriteSyncer(&buffer))
			require.NoError(t, err)
			log.Info("TestZapStringer", redact.ZapStringer(tc.key, tc.value))
			require.Contains(t, buffer.String(), tc.expected)
			require.Contains(t, buffer.String(), tc.key)
			buffer.Reset()
		})
	}
}

func TestZapStrings(t *testing.T) {
	testCases := []struct {
		name     string
		mode     RedactInfoLogType
		key      string
		value    []string
		expected string
	}{
		{
			name:     "off mode",
			mode:     RedactInfoLogOFF,
			key:      "keyOffMode",
			value:    []string{"sensitive", "data"},
			expected: "[sensitive,data]",
		},
		{
			name:     "on mode",
			mode:     RedactInfoLogON,
			key:      "keyOnMode",
			value:    []string{"sensitive", "data"},
			expected: "?",
		},
		{
			name:     "marker mode",
			mode:     RedactInfoLogMarker,
			key:      "keyMarkerMode",
			value:    []string{"sensitive", "data"},
			expected: "[‹sensitive›,‹data›]",
		},
		{
			name:     "marker mode with special chars",
			mode:     RedactInfoLogMarker,
			key:      "keySpecial",
			value:    []string{"f‹xcv"},
			expected: "‹f‹‹xcv›",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := InitLogRedactionFromType(tc.mode)
			require.NoError(t, err)
			var buffer zaptest.Buffer
			err = InitLogger(&Config{Level: "info"}, WithOutputWriteSyncer(&buffer))
			require.NoError(t, err)
			log.Info("TestZapStringer", ZapRedactStrings(tc.key, tc.value))
			require.Contains(t, buffer.String(), tc.expected)
			require.Contains(t, buffer.String(), tc.key)
			buffer.Reset()
		})
	}
}
