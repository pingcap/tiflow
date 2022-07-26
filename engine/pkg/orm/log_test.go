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
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestLoggerOpt(t *testing.T) {
	t.Parallel()

	var op loggerOption
	WithSlowThreshold(30 * time.Second)(&op)
	require.Equal(t, 30*time.Second, op.slowThreshold)
}

func TestNewOrmLogger(t *testing.T) {
	t.Parallel()

	var buffer zaptest.Buffer
	lg, _, err := log.InitLoggerWithWriteSyncer(&log.Config{Level: "warn"}, &buffer, nil)
	require.NoError(t, err)

	logger := NewOrmLogger(lg, WithSlowThreshold(3*time.Second))
	logger.Info(context.TODO(), "%s test", "info")
	// expect no log here
	logger.Warn(context.TODO(), "%s test", "warn")
	require.Regexp(t, regexp.QuoteMeta("warn test"), buffer.Stripped())
	buffer.Reset()

	logger.Error(context.TODO(), "%s test", "error")
	require.Regexp(t, regexp.QuoteMeta("error test"), buffer.Stripped())
	buffer.Reset()

	fc := func() (sql string, rowsAffected int64) { return "sql test", 10 }
	logger.Trace(context.TODO(), time.Now(), fc, nil)
	// expect no log here
	logger.Trace(context.TODO(), time.Now().Add(-10*time.Second), fc, errors.New("error test"))
	require.Regexp(t, regexp.QuoteMeta("[sql=\"sql test\"] [affected-rows=10] [error=\"error test\"]"), buffer.Stripped())
	buffer.Reset()
}
