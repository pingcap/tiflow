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

package util

import (
	"bytes"
	"os"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/grpclog"
)

// _globalP is the global ZapProperties in log
var _globalP *log.ZapProperties

const (
	defaultLogLevel   = "info"
	defaultLogMaxDays = 7
	defaultLogMaxSize = 512 // MB
)

// Config serializes log related config in toml/json.
type Config struct {
	// Log level.
	Level string `toml:"level" json:"level"`
	// Log filename, leave empty to disable file log.
	File string `toml:"file" json:"file"`
	// Max size for a single file, in MB.
	FileMaxSize int `toml:"max-size" json:"max-size"`
	// Max log keep days, default is never deleting.
	FileMaxDays int `toml:"max-days" json:"max-days"`
	// Maximum number of old log files to retain.
	FileMaxBackups int `toml:"max-backups" json:"max-backups"`
}

// Adjust adjusts config
func (cfg *Config) Adjust() {
	if len(cfg.Level) == 0 {
		cfg.Level = defaultLogLevel
	}
	if cfg.Level == "warning" {
		cfg.Level = "warn"
	}
	if cfg.FileMaxSize == 0 {
		cfg.FileMaxSize = defaultLogMaxSize
	}
	if cfg.FileMaxDays == 0 {
		cfg.FileMaxDays = defaultLogMaxDays
	}
}

// InitLogger initializes logger
func InitLogger(cfg *Config) error {
	pclogConfig := &log.Config{
		Level: cfg.Level,
		File: log.FileLogConfig{
			Filename:   cfg.File,
			MaxSize:    cfg.FileMaxSize,
			MaxDays:    cfg.FileMaxDays,
			MaxBackups: cfg.FileMaxBackups,
		},
	}

	var lg *zap.Logger
	var err error
	lg, _globalP, err = log.InitLogger(pclogConfig)
	if err != nil {
		return err
	}

	// Do not log stack traces at all, as we'll get the stack trace from the
	// error itself.
	lg = lg.WithOptions(zap.AddStacktrace(zap.DPanicLevel))

	log.ReplaceGlobals(lg, _globalP)

	var level zapcore.Level
	err = level.UnmarshalText([]byte(cfg.Level))
	if err != nil {
		return errors.Trace(err)
	}
	err = initSaramaLogger(level)
	if err != nil {
		return err
	}
	err = initGRPCLogger(level)
	if err != nil {
		return err
	}

	return nil
}

// ZapErrorFilter wraps zap.Error, if err is in given filterErrors, it will be set to nil
func ZapErrorFilter(err error, filterErrors ...error) zap.Field {
	cause := errors.Cause(err)
	for _, ferr := range filterErrors {
		if cause == ferr {
			return zap.Error(nil)
		}
	}
	return zap.Error(err)
}

// InitSaramaLogger hacks logger used in sarama lib
func initSaramaLogger(level zapcore.Level) error {
	// only available less than info level
	if !zapcore.InfoLevel.Enabled(level) {
		logger, err := zap.NewStdLogAt(log.L().With(zap.String("name", "sarama")), level)
		if err != nil {
			return errors.Trace(err)
		}
		sarama.Logger = logger
	}
	return nil
}

type grpcLoggerWriter struct {
	logFunc func(msg string, fields ...zap.Field)
}

func (l *grpcLoggerWriter) Write(p []byte) (int, error) {
	p = bytes.TrimSpace(p)
	l.logFunc(string(p))
	return len(p), nil
}

func levelToFunc(logger *zap.Logger, level zapcore.Level) (func(string, ...zap.Field), error) {
	switch level {
	case zap.DebugLevel:
		return logger.Debug, nil
	case zap.InfoLevel:
		return logger.Info, nil
	case zap.WarnLevel:
		return logger.Warn, nil
	case zap.ErrorLevel:
		return logger.Error, nil
	case zap.DPanicLevel:
		return logger.DPanic, nil
	case zap.PanicLevel:
		return logger.Panic, nil
	case zap.FatalLevel:
		return logger.Fatal, nil
	}
	return nil, errors.Errorf("unrecognized level: %q", level)
}

func initGRPCLogger(level zapcore.Level) error {
	// Inherit Golang gRPC verbosity setting.
	// https://github.com/grpc/grpc-go/blob/v1.26.0/grpclog/loggerv2.go#L134-L138
	var v int
	vLevel := os.Getenv("GRPC_GO_LOG_VERBOSITY_LEVEL")
	if vl, err := strconv.Atoi(vLevel); err == nil {
		v = vl
	}
	if v <= 0 && level.Enabled(zapcore.ErrorLevel) {
		// Sometimes gRPC log is very verbose.
		level = zapcore.ErrorLevel
		return nil
	}

	logger := log.L().With(zap.String("name", "grpc"))
	// For gRPC 1.26.0, logging call stack:
	//
	// github.com/pingcap/ticdc/pkg/util.levelToFunc.func1
	// github.com/pingcap/ticdc/pkg/util.(*grpcLoggerWriter).Write
	// log.(*Logger).Output
	// log.(*Logger).Printf
	// google.golang.org/grpc/grpclog.(*loggerT).Infof
	// google.golang.org/grpc/grpclog.Infof(...)
	// Caller
	logger = logger.WithOptions(zap.AddCallerSkip(5))
	logFunc, err := levelToFunc(logger, level)
	if err != nil {
		return err
	}
	writer := &grpcLoggerWriter{logFunc: logFunc}
	grpclog.SetLoggerV2(grpclog.NewLoggerV2WithVerbosity(writer, writer, writer, v))
	return nil
}
