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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestRedactString(t *testing.T) {
	originalMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })

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
		SetRedactionMode(tc.mode)
		result := String(tc.input)
		require.Equal(t, tc.output, result, "String failed for mode %v, input %s", tc.mode, tc.input)
	}
}

func TestRedactionModeSettings(t *testing.T) {
	originalMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })

	// Test setting and getting modes
	SetRedactionMode(RedactionModeOn)
	require.Equal(t, RedactionModeOn, GetRedactionMode())

	SetRedactionMode(RedactionModeOff)
	require.Equal(t, RedactionModeOff, GetRedactionMode())

	SetRedactionMode(RedactionModeMarker)
	require.Equal(t, RedactionModeMarker, GetRedactionMode())
}

func TestRedactionModeUsesSharedState(t *testing.T) {
	originalMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })

	for _, tc := range []struct {
		mode RedactionMode
		want string
	}{
		{RedactionModeOff, errors.RedactLogDisable},
		{RedactionModeOn, errors.RedactLogEnable},
		{RedactionModeMarker, errors.RedactLogMarker},
		{RedactionMode(-1), errors.RedactLogEnable},
	} {
		SetRedactionMode(tc.mode)
		require.Equal(t, tc.want, errors.RedactLogEnabled.Load())
	}

	for _, tc := range []struct {
		sharedMode string
		wantMode   RedactionMode
		wantValue  string
	}{
		{"", RedactionModeOff, "secret‹›"},
		{errors.RedactLogDisable, RedactionModeOff, "secret‹›"},
		{errors.RedactLogEnable, RedactionModeOn, "?"},
		{errors.RedactLogMarker, RedactionModeMarker, "‹secret‹‹›››"},
		{"invalid", RedactionModeOn, "?"},
	} {
		t.Run("shared mode "+tc.sharedMode, func(t *testing.T) {
			errors.RedactLogEnabled.Store(tc.sharedMode)
			require.Equal(t, tc.wantMode, GetRedactionMode())
			require.Equal(t, tc.wantValue, String("secret‹›"))
			require.Equal(t, tc.wantValue, encodeField(ZapString("value", "secret‹›"))["value"])
			if tc.wantMode == RedactionModeOn {
				calls := 0
				value := countingStringer{calls: &calls, value: "secret‹›"}
				require.Equal(t, "?", encodeField(ZapAny("value", value))["value"])
				require.Equal(t, "?", encodeField(ZapStringer("value", value))["value"])
				require.Zero(t, calls)
			}
		})
	}
}

// countingStringer records how many times String is called so tests can
// assert that redaction work is deferred until a field is encoded.
type countingStringer struct {
	calls *int
	value string
}

func (c countingStringer) String() string {
	*c.calls++
	return c.value
}

func encodeField(f zap.Field) map[string]interface{} {
	enc := zapcore.NewMapObjectEncoder()
	f.AddTo(enc)
	return enc.Fields
}

func TestZapAnyIsLazyInMarkerMode(t *testing.T) {
	originalMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })
	SetRedactionMode(RedactionModeMarker)

	calls := 0
	field := ZapAny("key", countingStringer{calls: &calls, value: "secret"})
	require.Equal(t, 0, calls, "value must not be formatted when the field is built")

	fields := encodeField(field)
	require.Equal(t, 1, calls, "value must be formatted exactly once when encoded")
	require.Equal(t, "‹secret›", fields["key"])
}

func TestZapStringIsLazyInMarkerMode(t *testing.T) {
	originalMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })

	SetRedactionMode(RedactionModeMarker)
	field := ZapString("key", "a‹b")
	require.Equal(t, zapcore.StringerType, field.Type, "marker mode must defer building the redacted copy")
	require.Equal(t, "‹a‹‹b›", encodeField(field)["key"])

	SetRedactionMode(RedactionModeOff)
	field = ZapString("key", "plain")
	require.Equal(t, zapcore.StringType, field.Type)
	require.Equal(t, "plain", field.String)

	SetRedactionMode(RedactionModeOn)
	field = ZapString("key", "plain")
	require.Equal(t, zapcore.StringType, field.Type)
	require.Equal(t, "?", field.String)
}

func TestZapStringsIsLazyInMarkerMode(t *testing.T) {
	originalMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })
	SetRedactionMode(RedactionModeMarker)

	field := ZapStrings("key", []string{"a", "b›c"})
	require.Equal(t, zapcore.ArrayMarshalerType, field.Type, "marker mode must defer building the redacted copies")
	require.Equal(t, []interface{}{"‹a›", "‹b››c›"}, encodeField(field)["key"])
}

func jsonLogger(buffer *bytes.Buffer) *zap.Logger {
	encoder := zapcore.NewJSONEncoder(zapcore.EncoderConfig{MessageKey: "message"})
	return zap.New(zapcore.NewCore(encoder, zapcore.AddSync(buffer), zap.InfoLevel))
}

func loggedFields(t *testing.T, buffer *bytes.Buffer) map[string]interface{} {
	t.Helper()
	var fields map[string]interface{}
	require.NoError(t, json.Unmarshal(buffer.Bytes(), &fields))
	return fields
}

func TestFilteredDebugDoesNotFormatValues(t *testing.T) {
	originalMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })
	SetRedactionMode(RedactionModeMarker)

	var buffer bytes.Buffer
	logger := jsonLogger(&buffer)
	calls := 0
	value := countingStringer{calls: &calls, value: "secret‹›"}
	fields := []zap.Field{
		ZapString("string", value.value),
		ZapAny("any", value),
		ZapStringer("stringer", value),
		ZapStrings("strings", []string{value.value}),
	}
	logger.Debug("filtered", fields...)
	require.Zero(t, calls)
	require.Empty(t, buffer.String())

	logger.Info("encoded", fields...)
	require.Equal(t, 2, calls)
	encoded := loggedFields(t, &buffer)
	for _, key := range []string{"string", "any", "stringer"} {
		require.Equal(t, "‹secret‹‹›››", encoded[key])
	}
	require.Equal(t, []interface{}{"‹secret‹‹›››"}, encoded["strings"])
}

func TestZapFieldsRetainModeUntilEncoding(t *testing.T) {
	originalMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })

	testCases := []struct {
		name        string
		initialMode RedactionMode
		finalMode   RedactionMode
		want        string
		wantStrings interface{}
		wantCalls   int
	}{
		{"marker to off", RedactionModeMarker, RedactionModeOff, "‹secret‹‹›››", []interface{}{"‹secret‹‹›››"}, 2},
		{"marker to on", RedactionModeMarker, RedactionModeOn, "‹secret‹‹›››", []interface{}{"‹secret‹‹›››"}, 2},
		{"on to off", RedactionModeOn, RedactionModeOff, "?", "?", 0},
		{"off to marker", RedactionModeOff, RedactionModeMarker, "secret‹›", []interface{}{"secret‹›"}, 2},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			SetRedactionMode(tc.initialMode)
			calls := 0
			value := countingStringer{calls: &calls, value: "secret‹›"}
			fields := []zap.Field{
				ZapString("string", value.value),
				ZapAny("any", value),
				ZapStringer("stringer", value),
				ZapStrings("strings", []string{value.value}),
			}
			require.Zero(t, calls)

			SetRedactionMode(tc.finalMode)
			var buffer bytes.Buffer
			jsonLogger(&buffer).Info("test", fields...)
			encoded := loggedFields(t, &buffer)
			for _, key := range []string{"string", "any", "stringer"} {
				require.Equal(t, tc.want, encoded[key])
			}
			require.Equal(t, tc.wantStrings, encoded["strings"])
			require.Equal(t, tc.wantCalls, calls)
		})
	}
}

func TestZapFieldsMatchOriginalsWhenOff(t *testing.T) {
	originalMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })
	SetRedactionMode(RedactionModeOff)

	calls := 0
	value := countingStringer{calls: &calls, value: "secret"}
	require.Equal(t, zap.String("string", "secret"), ZapString("string", "secret"))
	require.Equal(t, zap.Any("any", value), ZapAny("any", value))
	require.Equal(t, zap.Stringer("stringer", value), ZapStringer("stringer", value))
	require.Equal(t, zap.Strings("strings", []string{"secret"}), ZapStrings("strings", []string{"secret"}))
	require.Zero(t, calls)
}

func TestZapStringerNilValues(t *testing.T) {
	originalMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })

	values := []struct {
		name  string
		value fmt.Stringer
	}{
		{"nil interface", nil},
		{"nil pointer", (*countingStringer)(nil)},
	}
	for _, mode := range []RedactionMode{RedactionModeOff, RedactionModeOn, RedactionModeMarker} {
		for _, value := range values {
			t.Run(mode.String()+"/"+value.name, func(t *testing.T) {
				SetRedactionMode(mode)
				var buffer bytes.Buffer
				jsonLogger(&buffer).Info("test", ZapStringer("value", value.value))
				encoded := loggedFields(t, &buffer)
				switch mode {
				case RedactionModeOff:
					var original bytes.Buffer
					jsonLogger(&original).Info("test", zap.Stringer("value", value.value))
					require.Equal(t, loggedFields(t, &original), encoded)
				case RedactionModeOn:
					require.Equal(t, "?", encoded["value"])
				case RedactionModeMarker:
					require.Equal(t, "‹<nil>›", encoded["value"])
				}
			})
		}
	}
}
