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

	// Test with redaction OFF
	err := InitLogRedactionFromType(RedactInfoLogOFF)
	require.NoError(t, err)

	buf.Reset()
	log.Info("Testing redaction OFF",
		ZapRedactString("query", sensitiveData))

	output := buf.String()
	require.Contains(t, output, sensitiveData)

	// Test with redaction ON
	err = InitLogRedactionFromType(RedactInfoLogON)
	require.NoError(t, err)

	buf.Reset()
	log.Info("Testing redaction ON",
		ZapRedactString("query", sensitiveData))

	output = buf.String()
	require.Contains(t, output, `"query":"?"`)
	require.NotContains(t, output, sensitiveData)

	// Test with redaction MARKER
	err = InitLogRedactionFromType(RedactInfoLogMarker)
	require.NoError(t, err)

	buf.Reset()
	log.Info("Testing redaction MARKER",
		ZapRedactString("query", sensitiveData))

	output = buf.String()
	require.Contains(t, output, "‹"+sensitiveData+"›")

	// Reset to OFF for cleanup
	err = InitLogRedactionFromType(RedactInfoLogOFF)
	require.NoError(t, err)
}
