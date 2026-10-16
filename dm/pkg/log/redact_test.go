// Copyright 2026 PingCAP, Inc.
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

package log

import (
	"bytes"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type forbiddenStringer struct{}

func (forbiddenStringer) String() string { panic("sensitive Stringer evaluated") }

func TestRedactionCore(t *testing.T) {
	previous := IsRedactLogEnabled()
	t.Cleanup(func() { SetRedactLog(previous) })
	for _, enabled := range []bool{false, true} {
		SetRedactLog(enabled)
		var output bytes.Buffer
		logger := WithRedaction(zap.New(zapcore.NewCore(
			zapcore.NewJSONEncoder(zapcore.EncoderConfig{MessageKey: "msg"}),
			zapcore.AddSync(&output), zap.DebugLevel)))
		mysqlErr := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'private-value'"}
		binlogErr := &replication.EventError{Data: []byte("private-value"), Err: "decode failed"}
		fields := []zap.Field{zap.Error(mysqlErr), zap.String("task", "task-one")}
		logger.With(fields...).Info("with error")
		logger.Error("error", zap.NamedError("cause", binlogErr))
		logger.Error("short error", ShortError(errors.Annotate(mysqlErr, "private-value")))
		logger.Debug("row", RedactField(zap.Reflect("raw event data", [][]any{{"private-value"}})))
		logger.Info("ddl", RedactField(zap.String("query", "CREATE TABLE t(c TEXT DEFAULT 'private-value')")))
		require.Equal(t, zapcore.ErrorType, fields[0].Type, "logging must not modify caller fields")
		require.Contains(t, mysqlErr.Error(), "private-value", "retry code retains the original error")
		require.Contains(t, output.String(), "task-one")
		if enabled {
			require.NotContains(t, output.String(), "private-value")
			require.Contains(t, output.String(), "1062")
			logger.Debug("lazy", RedactField(zap.Stringer("row", forbiddenStringer{})))
		} else {
			require.Contains(t, output.String(), "private-value")
		}
	}
}

func TestRedactErrorRetainsClassification(t *testing.T) {
	previous := IsRedactLogEnabled()
	SetRedactLog(true)
	t.Cleanup(func() { SetRedactLog(previous) })
	err := terror.ErrDBExecuteFailed.Delegate(errors.New("private-value"), "private-query")
	text := RedactError(err)
	require.NotContains(t, text, "private-value")
	require.NotContains(t, text, "private-query")
	require.Contains(t, text, "code=")
	require.Contains(t, err.Error(), "private-value")
	require.Empty(t, RedactError(nil))
}

func TestRedactConfigKeepsDiagnostics(t *testing.T) {
	previous := IsRedactLogEnabled()
	t.Cleanup(func() { SetRedactLog(previous) })
	input := `{"name":"task-one","server-id":18446744073709551615,"where":"private-value","nested":[{"extra-args":"private-value","threads":4}]}`
	SetRedactLog(false)
	require.Equal(t, input, RedactConfigJSON(input))
	SetRedactLog(true)
	output := RedactConfigJSON(input)
	require.NotContains(t, output, "private-value")
	require.Contains(t, output, "18446744073709551615")
	require.Contains(t, output, "task-one")
	require.Contains(t, output, `"threads":4`)
	var buffer bytes.Buffer
	logger := WithRedaction(zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zapcore.EncoderConfig{MessageKey: "msg"}),
		zapcore.AddSync(&buffer), zap.InfoLevel)))
	logger.Info("embedded config", zap.String("cfg", input))
	require.NotContains(t, buffer.String(), "private-value")
	require.Contains(t, buffer.String(), "task-one")
}
