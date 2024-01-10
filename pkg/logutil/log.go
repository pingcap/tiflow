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
	"bytes"
	"context"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/grpclog"
)

// globalP is the global ZapProperties in log
var globalP *log.ZapProperties

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
	// ZapInternalErrOutput specify where the internal error of zap logger should be send to.
	ZapInternalErrOutput string `toml:"error-output" json:"error-output"`
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

// SetLogLevel changes TiCDC log level dynamically.
func SetLogLevel(level string) error {
	oldLevel := log.GetLevel()
	if strings.EqualFold(oldLevel.String(), level) {
		return nil
	}
	var lv zapcore.Level
	err := lv.UnmarshalText([]byte(level))
	if err != nil {
		return errors.Trace(err)
	}
	log.SetLevel(lv)
	return nil
}

// loggerOp is the op for logger control
type loggerOp struct {
	isInitGRPCLogger   bool
	isInitSaramaLogger bool
	isInitMySQLLogger  bool
	output             zapcore.WriteSyncer
}

func (op *loggerOp) applyOpts(opts []LoggerOpt) {
	for _, opt := range opts {
		opt(op)
	}
}

// LoggerOpt is the logger option
type LoggerOpt func(*loggerOp)

// WithInitGRPCLogger enables grpc logger initialization when initializes global logger
func WithInitGRPCLogger() LoggerOpt {
	return func(op *loggerOp) {
		op.isInitGRPCLogger = true
	}
}

// WithInitSaramaLogger enables sarama logger initialization when initializes global logger
func WithInitSaramaLogger() LoggerOpt {
	return func(op *loggerOp) {
		op.isInitSaramaLogger = true
	}
}

// WithInitMySQLLogger enables mysql logger initialization when initializes global logger
func WithInitMySQLLogger() LoggerOpt {
	return func(op *loggerOp) {
		op.isInitMySQLLogger = true
	}
}

// WithOutputWriteSyncer will replace the WriteSyncer of global logger with customized WriteSyncer
// Easy for test when using zaptest.Buffer as WriteSyncer
func WithOutputWriteSyncer(output zapcore.WriteSyncer) LoggerOpt {
	return func(op *loggerOp) {
		op.output = output
	}
}

// InitLogger initializes logger
func InitLogger(cfg *Config, opts ...LoggerOpt) error {
	var op loggerOp
	op.applyOpts(opts)

	pclogConfig := &log.Config{
		Level: cfg.Level,
		File: log.FileLogConfig{
			Filename:   cfg.File,
			MaxSize:    cfg.FileMaxSize,
			MaxDays:    cfg.FileMaxDays,
			MaxBackups: cfg.FileMaxBackups,
		},
		ErrorOutputPath: cfg.ZapInternalErrOutput,
	}

	var lg *zap.Logger
	var err error
	if op.output == nil {
		lg, globalP, err = log.InitLogger(pclogConfig)
	} else {
		lg, globalP, err = log.InitLoggerWithWriteSyncer(pclogConfig, op.output, nil)
	}
	if err != nil {
		return err
	}

	// Do not log stack traces at all, as we'll get the stack trace from the
	// error itself.
	lg = lg.WithOptions(zap.AddStacktrace(zap.DPanicLevel))
	log.ReplaceGlobals(lg, globalP)

	return initOptionalComponent(&op, cfg)
}

// initOptionalComponent initializes some optional components
func initOptionalComponent(op *loggerOp, cfg *Config) error {
	var level zapcore.Level
	if op.isInitGRPCLogger || op.isInitSaramaLogger {
		err := level.UnmarshalText([]byte(cfg.Level))
		if err != nil {
			return errors.Trace(err)
		}
	}

	if op.isInitGRPCLogger {
		if err := initGRPCLogger(level); err != nil {
			return err
		}
	}

	if op.isInitSaramaLogger {
		if err := initSaramaLogger(level); err != nil {
			return err
		}
	}

	if op.isInitMySQLLogger {
		if err := initMySQLLogger(); err != nil {
			return err
		}
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

// initMySQLLogger setup logger used in mysql lib
func initMySQLLogger() error {
	// MySQL lib only prints error logs.
	level := zapcore.ErrorLevel
	logger, err := zap.NewStdLogAt(log.L().With(zap.String("component", "[mysql]")), level)
	if err != nil {
		return errors.Trace(err)
	}
	return mysql.SetLogger(logger)
}

// initSaramaLogger hacks logger used in sarama lib
func initSaramaLogger(level zapcore.Level) error {
	// only available less than info level
	if !zapcore.InfoLevel.Enabled(level) {
		logger, err := zap.NewStdLogAt(log.L().With(zap.String("component", "sarama")), level)
		if err != nil {
			return errors.Trace(err)
		}
		sarama.Logger = logger
	}
	return nil
}

type loggerWriter struct {
	logFunc func(msg string, fields ...zap.Field)
}

func (l *loggerWriter) Write(p []byte) (int, error) {
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

	logger := log.L().With(zap.String("component", "grpc"))
	// For gRPC 1.26.0, logging call stack:
	//
	// github.com/pingcap/tiflow/pkg/util.levelToFunc.func1
	// github.com/pingcap/tiflow/pkg/util.(*grpcLoggerWriter).Write
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
	writer := &loggerWriter{logFunc: logFunc}
	grpclog.SetLoggerV2(grpclog.NewLoggerV2WithVerbosity(writer, writer, writer, v))
	return nil
}

// ErrorFilterContextCanceled log the msg and fields but do nothing if fields have cancel error
func ErrorFilterContextCanceled(logger *zap.Logger, msg string, fields ...zap.Field) {
	for _, field := range fields {
		switch field.Type {
		case zapcore.StringType:
			if field.Key == "error" && strings.Contains(field.String, context.Canceled.Error()) {
				return
			}
		case zapcore.ErrorType:
			err, ok := field.Interface.(error)
			if ok && errors.Cause(err) == context.Canceled {
				return
			}
		}
	}

	logger.WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
}

// ShortError contructs a field which only records the error message without the
// verbose text (i.e. excludes the stack trace).
func ShortError(err error) zap.Field {
	if err == nil {
		return zap.Skip()
	}
	return zap.String("error", err.Error())
}

// WithComponent return a logger with specified component scope
func WithComponent(component string) *zap.Logger {
	return log.L().With(zap.String("component", component))
}

// InitGinLogWritter initialize loggers for Gin.
func InitGinLogWritter() io.Writer {
	currentLevel := log.GetLevel()
	if currentLevel == zapcore.DebugLevel {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}
	logger := WithComponent("gin")
	logFunc, _ := levelToFunc(logger, currentLevel)
	gin.DefaultWriter = &loggerWriter{logFunc: logFunc}
	logFunc, _ = levelToFunc(logger, zapcore.ErrorLevel)
	gin.DefaultErrorWriter = &loggerWriter{logFunc: logFunc}
	return gin.DefaultErrorWriter
}
