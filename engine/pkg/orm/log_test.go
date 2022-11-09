// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package orm

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"gorm.io/gorm"
)

func TestLoggerOpt(t *testing.T) {
	t.Parallel()

	var op loggerOption
	WithSlowThreshold(30 * time.Second)(&op)
	require.Equal(t, 30*time.Second, op.slowThreshold)

	require.False(t, op.ignoreTraceRecordNotFoundErr)
	WithIgnoreTraceRecordNotFoundErr()(&op)
	require.True(t, op.ignoreTraceRecordNotFoundErr)
}

func TestNewOrmLogger(t *testing.T) {
	t.Parallel()

	var buffer zaptest.Buffer
	zapLg, _, err := log.InitLoggerWithWriteSyncer(&log.Config{Level: "warn"}, &buffer, nil)
	require.NoError(t, err)

	lg := NewOrmLogger(zapLg, WithSlowThreshold(3*time.Second), WithIgnoreTraceRecordNotFoundErr())
	lg.Info(context.TODO(), "%s test", "info")
	require.Equal(t, 0, len(buffer.Lines()))

	lg.Warn(context.TODO(), "%s test", "warn")
	require.Regexp(t, regexp.QuoteMeta("warn test"), buffer.Stripped())
	buffer.Reset()

	lg.Error(context.TODO(), "%s test", "error")
	require.Regexp(t, regexp.QuoteMeta("error test"), buffer.Stripped())
	buffer.Reset()

	fc := func() (sql string, rowsAffected int64) { return "sql test", 10 }
	lg.Trace(context.TODO(), time.Now(), fc, nil)
	require.Equal(t, 0, len(buffer.Lines()))

	lg.Trace(context.TODO(), time.Now().Add(-10*time.Second), fc, errors.New("error test"))
	require.Regexp(t, regexp.QuoteMeta("[ERROR]"), buffer.Stripped())
	require.Regexp(t, regexp.MustCompile(`\["slow log"\] \[elapsed=10.*s\] \[sql="sql test"\] \[affected-rows=10\] \[error="error test"\]`), buffer.Stripped())
	buffer.Reset()

	lg.Trace(context.TODO(), time.Now(), fc, gorm.ErrRecordNotFound)
	// expect no log here because it's a debug log
	require.Equal(t, 0, len(buffer.Lines()))
	buffer.Reset()
}
