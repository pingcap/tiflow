// Copyright 2019 PingCAP, Inc.
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

package log

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	pclog "github.com/pingcap/log"
	lightningLog "github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func TestTestLogger(t *testing.T) {
	logger, buffer := makeTestLogger()
	logger.Warn("the message", zap.Int("number", 123456), zap.Ints("array", []int{7, 8, 9}))
	require.Equal(t, `{"$lvl":"WARN","$msg":"the message","number":123456,"array":[7,8,9]}`, buffer.Stripped())
	buffer.Reset()
	logger.ErrorFilterContextCanceled("the message", zap.Int("number", 123456),
		zap.Ints("array", []int{7, 8, 9}), zap.Error(context.Canceled))
	require.Empty(t, buffer.Stripped())
	buffer.Reset()
	logger.ErrorFilterContextCanceled("the message", zap.Int("number", 123456),
		zap.Ints("array", []int{7, 8, 9}), ShortError(errors.Annotate(context.Canceled, "extra info")))
	require.Empty(t, buffer.Stripped())
}

// makeTestLogger creates a Logger instance which produces JSON logs.
func makeTestLogger() (Logger, *zaptest.Buffer) {
	buffer := new(zaptest.Buffer)
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zapcore.EncoderConfig{
			LevelKey:       "$lvl",
			MessageKey:     "$msg",
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
		}),
		buffer,
		zap.DebugLevel,
	))
	return Logger{Logger: logger}, buffer
}

func TestLogLevel(t *testing.T) {
	logLevel := "warning"
	cfg := &Config{
		Level: logLevel,
	}
	cfg.Adjust()

	require.NoError(t, InitLogger(cfg))
	require.Equal(t, zap.WarnLevel.String(), Props().Level.String())
	require.Nil(t, L().Check(zap.InfoLevel, "This is an info log"))
	require.NotNil(t, L().Check(zap.ErrorLevel, "This is an error log"))

	SetLevel(zap.InfoLevel)
	require.Equal(t, zap.InfoLevel.String(), Props().Level.String())
	require.NotNil(t, L().Check(zap.WarnLevel, "This is a warn log"))
	require.Nil(t, L().Check(zap.DebugLevel, "This is a debug log"))
}

func captureStdout(f func()) ([]string, error) {
	r, w, _ := os.Pipe()
	stdout := os.Stdout
	os.Stdout = w

	f()

	var buf bytes.Buffer
	output := make(chan string, 1)
	errs := make(chan error, 1)

	go func() {
		_, err := io.Copy(&buf, r)
		output <- buf.String()
		errs <- err
		r.Close()
	}()

	os.Stdout = stdout
	w.Close()
	return strings.Split(<-output, "\n"), <-errs
}

func TestInitSlowQueryLoggerInDebugLevel(t *testing.T) {
	// test slow query logger can write debug log
	logLevel := "debug"
	cfg := &Config{Level: logLevel, Format: "json"}
	cfg.Adjust()
	output, err := captureStdout(func() {
		require.NoError(t, InitLogger(cfg))
		logutil.SlowQueryLogger.Debug("this is test info")
		appLogger.Debug("this is from applogger")
	})
	require.NoError(t, err)
	require.Regexp(t, "this is test info.*component.*slow query logger", output[0])
	require.Contains(t, output[1], "this is from applogger")
	// test log is json formart
	type jsonLog struct {
		Component string `json:"component"`
	}
	oneLog := jsonLog{}
	require.NoError(t, json.Unmarshal([]byte(output[0]), &oneLog))
	require.Equal(t, "slow query logger", oneLog.Component)
}

func TestInitSlowQueryLoggerNotInDebugLevel(t *testing.T) {
	// test slow query logger can not write log in other log level
	logLevel := "info"
	cfg := &Config{Level: logLevel, Format: "json"}
	cfg.Adjust()
	output, err := captureStdout(func() {
		require.NoError(t, InitLogger(cfg))
		logutil.SlowQueryLogger.Info("this is test info")
		appLogger.Info("this is from applogger")
	})
	require.NoError(t, err)
	require.Len(t, output, 2)
	require.Contains(t, output[0], "this is from applogger")
	require.Empty(t, output[1]) // no output
}

func TestWithCtx(t *testing.T) {
	// test slow query logger can write debug log
	logLevel := "debug"
	cfg := &Config{Level: logLevel, Format: "json"}
	cfg.Adjust()

	ctx := context.Background()
	ctx = AppendZapFieldToCtx(ctx, zap.String("key1", "value1"))
	ctx = AppendZapFieldToCtx(ctx, zap.String("key2", "value2"))

	output, err := captureStdout(func() {
		require.NoError(t, InitLogger(cfg))
		WithCtx(ctx).Info("test1")
	})
	require.NoError(t, err)
	require.Regexp(t, `"key1":"value1".*"key2":"value2"`, output[0])
}

func TestLogToFile(t *testing.T) {
	d := t.TempDir()

	logFile := filepath.Join(d, "test.log")

	logLevel := "debug"
	cfg := &Config{
		Level:  logLevel,
		Format: "json",
		File:   logFile,
	}
	cfg.Adjust()
	require.NoError(t, InitLogger(cfg))

	var lastOff int64
	newLog := func() string {
		require.NoError(t, L().Sync())
		data, err := os.ReadFile(logFile)
		require.NoError(t, err)
		require.Greater(t, len(data), int(lastOff))
		result := string(data[lastOff:])
		lastOff = int64(len(data))
		return strings.TrimSpace(result)
	}

	L().Info("test dm log to file")
	require.Contains(t, newLog(), `"message":"test dm log to file"`)
	lightningLog.L().Info("test lightning log to file")
	require.Contains(t, newLog(), `"message":"test lightning log to file"`)
	pclog.Info("test pingcap/log to file")
	require.Contains(t, newLog(), `"message":"test pingcap/log to file"`)
	version.LogVersionInfo("DM")
	require.Contains(t, newLog(), `"message":"Welcome to DM"`)
}

func BenchmarkBaseline(b *testing.B) {
	logger := L().With(zap.String("key1", "value1"))
	for i := 0; i < b.N; i++ {
		subLogger := logger.With(zap.String("key2", "value2"))
		subLogger.Info("test-test-test")
	}
}

func BenchmarkWithCtx(b *testing.B) {
	ctx := AppendZapFieldToCtx(context.Background(), zap.String("key1", "value1"))
	for i := 0; i < b.N; i++ {
		subCtx := AppendZapFieldToCtx(ctx, zap.String("key2", "value2"))
		WithCtx(subCtx).Info("test-test-test")
	}
}
