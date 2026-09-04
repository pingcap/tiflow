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
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func TestRedactionIntegration(t *testing.T) {
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

	sensitiveData := "SELECT * FROM users WHERE password = 'secret123'"
	sensitiveKey := []byte("user_secret_key")

	// Test with redaction OFF
	err := InitLogRedaction(false, "OFF")
	require.NoError(t, err)

	buf.Reset()
	log.Info("Testing redaction OFF",
		ZapRedactString("query", sensitiveData),
		ZapRedactKey("key", sensitiveKey))

	output := buf.String()
	require.Contains(t, output, sensitiveData)
	require.Contains(t, output, "USER_SECRET_KEY") // hex encoded key

	// Test with redaction ON
	err = InitLogRedaction(true, "ON")
	require.NoError(t, err)

	buf.Reset()
	log.Info("Testing redaction ON",
		ZapRedactString("query", sensitiveData),
		ZapRedactKey("key", sensitiveKey))

	output = buf.String()
	require.Contains(t, output, `"query":"?"`)
	require.Contains(t, output, `"key":"?"`)
	require.NotContains(t, output, sensitiveData)
	require.NotContains(t, output, "USER_SECRET_KEY")

	// Test with redaction MARKER
	err = InitLogRedaction(true, "MARKER")
	require.NoError(t, err)

	buf.Reset()
	log.Info("Testing redaction MARKER",
		ZapRedactString("query", sensitiveData),
		ZapRedactKey("key", sensitiveKey))

	output = buf.String()
	require.Contains(t, output, "‹"+sensitiveData+"›")
	require.Contains(t, output, `"key":"?"`) // Keys are always ? in MARKER mode for security

	// Reset to OFF for cleanup
	err = InitLogRedaction(false, "OFF")
	require.NoError(t, err)
}

func TestBackwardCompatibilityWithDM(t *testing.T) {
	// Test that DM's log redaction still works with the unified system
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

	// Ensure redaction is disabled first
	err := InitLogRedaction(false, "OFF")
	require.NoError(t, err)
	require.False(t, IsRedactionEnabled())

	sensitiveValue := "sensitive_database_connection_string"

	// Test without redaction
	buf.Reset()
	log.Info("Test message", ZapRedactString("conn", sensitiveValue))

	output := buf.String()
	require.Contains(t, output, sensitiveValue)

	// Test with redaction enabled
	err = InitLogRedaction(true, "ON")
	require.NoError(t, err)
	require.True(t, IsRedactionEnabled())

	buf.Reset()
	log.Info("Test message", ZapRedactString("conn", sensitiveValue))

	output = buf.String()
	require.Contains(t, output, `"conn":"?"`)
	require.NotContains(t, output, sensitiveValue)

	// Reset to OFF for cleanup
	err = InitLogRedaction(false, "OFF")
	require.NoError(t, err)
}
