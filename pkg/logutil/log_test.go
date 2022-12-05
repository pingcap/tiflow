// Copyright 2020 PingCAP, Inc.
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

package logutil

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func TestInitLoggerAndSetLogLevel(t *testing.T) {
	f, err := os.CreateTemp("", "init-logger-test")
	require.Nil(t, err)
	defer os.Remove(f.Name())

	cfg := &Config{
		Level: "warning",
		File:  f.Name(),
	}
	cfg.Adjust()
	err = InitLogger(cfg)
	require.NoError(t, err)
	require.Equal(t, log.GetLevel(), zapcore.WarnLevel)

	// Set a different level.
	err = SetLogLevel("info")
	require.NoError(t, err)
	require.Equal(t, log.GetLevel(), zapcore.InfoLevel)

	// Set the same level.
	err = SetLogLevel("info")
	require.NoError(t, err)
	require.Equal(t, log.GetLevel(), zapcore.InfoLevel)

	// Set an invalid level.
	err = SetLogLevel("badlevel")
	require.Error(t, err)
}

func TestZapErrorFilter(t *testing.T) {
	var (
		err       = errors.New("test error")
		testCases = []struct {
			err      error
			filters  []error
			expected zap.Field
		}{
			{nil, []error{}, zap.Error(nil)},
			{err, []error{}, zap.Error(err)},
			{err, []error{context.Canceled}, zap.Error(err)},
			{err, []error{err}, zap.Error(nil)},
			{context.Canceled, []error{context.Canceled}, zap.Error(nil)},
			{errors.Annotate(context.Canceled, "annotate error"), []error{context.Canceled}, zap.Error(nil)},
		}
	)
	for _, tc := range testCases {
		require.Equal(t, ZapErrorFilter(tc.err, tc.filters...), tc.expected)
	}
}

func TestZapInternalErrorOutput(t *testing.T) {
	testCases := []struct {
		desc      string
		errOutput string
		error     bool
	}{
		{"test valid error output path", "stderr", false},
		{"test invalid error output path", filepath.Join(t.TempDir(), "/not-there/foo.log"), true},
	}

	dir := t.TempDir()
	for idx, tc := range testCases {
		f := filepath.Join(dir, fmt.Sprintf("test-file%d", idx))
		cfg := &Config{
			Level:                "info",
			File:                 f,
			ZapInternalErrOutput: tc.errOutput,
		}
		err := InitLogger(cfg)
		if tc.error {
			require.NotNil(t, err)
		} else {
			require.Nil(t, err)
		}
	}
}

func TestErrorFilterContextCanceled(t *testing.T) {
	var buffer zaptest.Buffer
	err := InitLogger(&Config{Level: "info"}, WithOutputWriteSyncer(&buffer))
	require.NoError(t, err)

	ErrorFilterContextCanceled(log.L(), "the message", zap.Int("number", 123456),
		zap.Ints("array", []int{7, 8, 9}), zap.Error(context.Canceled))
	require.Equal(t, "", buffer.Stripped())
	buffer.Reset()

	ErrorFilterContextCanceled(log.L(), "the message", zap.Int("number", 123456),
		zap.Ints("array", []int{7, 8, 9}),
		ShortError(errors.Annotate(context.Canceled, "extra info")))
	require.Equal(t, "", buffer.Stripped())
	buffer.Reset()

	ErrorFilterContextCanceled(log.L(), "the message", zap.Int("number", 123456),
		zap.Ints("array", []int{7, 8, 9}))
	require.Regexp(t, regexp.QuoteMeta("[\"the message\"]"+
		" [number=123456] [array=\"[7,8,9]\"]"), buffer.Stripped())
}

func TestShortError(t *testing.T) {
	var buffer zaptest.Buffer
	err := InitLogger(&Config{Level: "info"}, WithOutputWriteSyncer(&buffer))
	require.NoError(t, err)

	err = errors.Normalize(
		"meta not exists in region",
		errors.RFCCodeText("CDC:ErrMetaNotInRegion"),
	).GenWithStackByArgs("extra info")
	log.L().Warn("short error", ShortError(err))
	require.Regexp(t, regexp.QuoteMeta("[\"short error\"] "+
		"[error=\"[CDC:ErrMetaNotInRegion]meta not exists in region"), buffer.Stripped())
	buffer.Reset()

	log.L().Warn("short error", ShortError(nil))
	require.Regexp(t, regexp.QuoteMeta("[\"short error\"] []"), buffer.Stripped())
	buffer.Reset()

	log.L().Warn("short error", zap.Error(err))
	require.Regexp(t, regexp.QuoteMeta("errors.AddStack"), buffer.Stripped())
	buffer.Reset()
}

func TestLoggerOption(t *testing.T) {
	t.Parallel()

	var op loggerOp
	require.False(t, op.isInitGRPCLogger)
	require.False(t, op.isInitSaramaLogger)
	require.Nil(t, op.output)

	op.applyOpts([]LoggerOpt{WithInitGRPCLogger(), WithInitSaramaLogger()})
	require.True(t, op.isInitGRPCLogger)
	require.True(t, op.isInitSaramaLogger)
	require.Nil(t, op.output)

	var buffer zaptest.Buffer
	op.applyOpts([]LoggerOpt{WithOutputWriteSyncer(&buffer)})
	require.Equal(t, &buffer, op.output)
}

func TestWithComponent(t *testing.T) {
	var buffer zaptest.Buffer
	err := InitLogger(&Config{Level: "info"}, WithOutputWriteSyncer(&buffer))
	require.NoError(t, err)

	lg := WithComponent("grpc")
	lg.Warn("component test", zap.String("other", "other"))
	require.Regexp(t, regexp.QuoteMeta("[\"component test\"] [component=grpc] [other=other]"), buffer.Stripped())
	buffer.Reset()
}

func TestCallerSkip(t *testing.T) {
	// Using log before init logger should not affect
	// any other log after init logger.
	//
	// See https://github.com/pingcap/log/issues/30.
	log.Debug("debug")
	log.L().Debug("debug")

	var buffer zaptest.Buffer
	err := InitLogger(&Config{Level: "info"}, WithOutputWriteSyncer(&buffer))
	require.NoError(t, err)

	_, file, line, _ := runtime.Caller(0)
	_, filename := filepath.Split(file)
	log.Info("caller skip test", zap.String("other", "other"))
	require.Contains(t, buffer.Stripped(), fmt.Sprintf("%s:%d", filename, line+2))

	buffer.Reset()
	_, file, line, _ = runtime.Caller(0)
	_, filename = filepath.Split(file)
	log.L().Info("caller skip test", zap.String("other", "other"))
	require.Contains(t, buffer.Stripped(), fmt.Sprintf("%s:%d", filename, line+2))
}

func TestMySQLLogger(t *testing.T) {
	var buffer zaptest.Buffer
	err := InitLogger(&Config{Level: "info"}, WithOutputWriteSyncer(&buffer))
	require.NoError(t, err)

	require.Nil(t, initMySQLLogger())

	// Mock MySQL server
	ms, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := ms.Accept()
		require.NoError(t, err)
		err = conn.Close()
		require.NoError(t, err)
	}()

	dsnStr := fmt.Sprintf("root:@tcp(%s)/", ms.Addr().String())
	db, err := sql.Open("mysql", dsnStr)
	require.Nil(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = db.PingContext(ctx)
	require.Error(t, err)
	// Log: [ERROR] [packets.go:37] ["unexpected EOF"] [component="[mysql]"]
	require.Contains(t, buffer.Stripped(), "[ERROR]")
	require.Contains(t, buffer.Stripped(), "packets.go")
	require.Contains(t, buffer.Stripped(), "unexpected EOF")
	require.Contains(t, buffer.Stripped(), "[mysql]")
	wg.Wait()
}
