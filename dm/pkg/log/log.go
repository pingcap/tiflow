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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/pingcap/errors"
	pclog "github.com/pingcap/log"
	lightningLog "github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tiflow/dm/pkg/helper"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	defaultLogLevel   = "info"
	defaultLogMaxDays = 7
	defaultLogMaxSize = 512 // MB
)

// Config serializes log related config in toml/json.
type Config struct {
	// Log level.
	Level string `toml:"level" json:"level"`
	// the format of the log, "text" or "json"
	Format string `toml:"format" json:"format"`
	// Log filename, leave empty to disable file log.
	File string `toml:"file" json:"file"`
	// Max size for a single file, in MB.
	FileMaxSize int `toml:"max-size" json:"max-size"`
	// Max log keep days, default is never deleting.
	FileMaxDays int `toml:"max-days" json:"max-days"`
	// Maximum number of old log files to retain.
	FileMaxBackups int `toml:"max-backups" json:"max-backups"`
}

// Adjust adjusts config.
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

// Logger is a simple wrapper around *zap.Logger which provides some extra
// methods to simplify DM's log usage.
type Logger struct {
	*zap.Logger
}

// WithFields return new Logger with specified fields.
func (l Logger) WithFields(fields ...zap.Field) Logger {
	return Logger{l.With(fields...)}
}

// ErrorFilterContextCanceled wraps Logger.Error() and will filter error log when error is context.Canceled.
func (l Logger) ErrorFilterContextCanceled(msg string, fields ...zap.Field) {
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
	l.Logger.WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
}

// logger for DM.
var (
	appLogger = Logger{zap.NewNop()}
	appLevel  zap.AtomicLevel
	appProps  *pclog.ZapProperties
)

// InitLogger initializes DM's and also the TiDB library's loggers.
func InitLogger(cfg *Config) error {
	inDev := strings.ToLower(cfg.Level) == "debug"
	// init DM logger
	logger, props, err := pclog.InitLogger(&pclog.Config{
		Level:  cfg.Level,
		Format: cfg.Format,
		File: pclog.FileLogConfig{
			Filename:   cfg.File,
			MaxSize:    cfg.FileMaxSize,
			MaxDays:    cfg.FileMaxDays,
			MaxBackups: cfg.FileMaxBackups,
		},
		Development: inDev,
	})
	if err != nil {
		return terror.ErrInitLoggerFail.Delegate(err)
	}

	// Do not log stack traces at all, as we'll get the stack trace from the
	// error itself.
	logger = logger.WithOptions(zap.AddStacktrace(zap.DPanicLevel))
	pclog.ReplaceGlobals(logger, props)

	lightningLogger := logger.With(zap.String("component", "lightning"))
	lightningLog.SetAppLogger(lightningLogger)

	appLogger = Logger{logger}
	appLevel = props.Level
	appProps = props
	// init and set tidb slow query logger to stdout if log level is debug
	if inDev {
		slowQueryLogger := zap.NewExample()
		slowQueryLogger = slowQueryLogger.With(zap.String("component", "slow query logger"))
		logutil.SlowQueryLogger = slowQueryLogger
	} else {
		logutil.SlowQueryLogger = zap.NewNop()
	}
	return nil
}

// With creates a child logger from the global logger and adds structured
// context to it.
func With(fields ...zap.Field) Logger {
	return Logger{appLogger.With(fields...)}
}

// SetLevel modifies the log level of the global logger. Returns the previous
// level.
func SetLevel(level zapcore.Level) zapcore.Level {
	oldLevel := appLevel.Level()
	appLevel.SetLevel(level)
	return oldLevel
}

// ShortError contructs a field which only records the error message without the
// verbose text (i.e. excludes the stack trace).
//
// In DM, all errors are almost always propagated back to `main()` where
// the error stack is written. Including the stack in the middle thus usually
// just repeats known information. You should almost always use `ShortError`
// instead of `zap.Error`, unless the error is no longer propagated upwards.
func ShortError(err error) zap.Field {
	if err == nil {
		return zap.Skip()
	}
	return zap.String("error", err.Error())
}

// L returns the current logger for DM.
func L() Logger {
	return appLogger
}

// Props returns the current logger's props.
func Props() *pclog.ZapProperties {
	return appProps
}

// WrapStringerField returns a wrap stringer field.
func WrapStringerField(message string, object fmt.Stringer) zap.Field {
	if helper.IsNil(object) {
		return zap.String(message, "NULL")
	}

	return zap.Stringer(message, object)
}

// WithCtx adds fields from ctx to the logger.
func WithCtx(ctx context.Context) Logger {
	return Logger{appLogger.With(getZapFieldsFromCtx(ctx)...)}
}

// RedactInfoLogType is the behavior of redacting sensitive information in logs.
type RedactInfoLogType int

const (
	// RedactInfoLogOFF means log redaction is disabled.
	RedactInfoLogOFF RedactInfoLogType = iota
	// RedactInfoLogON means log redaction is enabled, and will replace the sensitive information with "?".
	RedactInfoLogON
	// RedactInfoLogMarker means log redaction is enabled, and will use single guillemets ‹› to enclose the sensitive information.
	RedactInfoLogMarker
)

// String implements flag.Value interface.
func (t *RedactInfoLogType) String() string {
	switch *t {
	case RedactInfoLogOFF:
		return "false"
	case RedactInfoLogON:
		return "true"
	case RedactInfoLogMarker:
		return "marker"
	default:
		return "false"
	}
}

// Set implements flag.Value interface.
func (t *RedactInfoLogType) Set(s string) error {
	switch strings.ToLower(s) {
	case "false":
		*t = RedactInfoLogOFF
		return nil
	case "true":
		*t = RedactInfoLogON
		return nil
	case "marker":
		*t = RedactInfoLogMarker
		return nil
	default:
		return fmt.Errorf("invalid redact-info-log value %q, must be false/true/marker", s)
	}
}

// Type implements pflag.Value interface.
func (t *RedactInfoLogType) Type() string {
	return "RedactInfoLogType"
}

// MarshalJSON implements the `json.Marshaler` interface to ensure the compatibility.
func (t RedactInfoLogType) MarshalJSON() ([]byte, error) {
	switch t {
	case RedactInfoLogON:
		return json.Marshal(true)
	case RedactInfoLogMarker:
		return json.Marshal("MARKER")
	default:
	}
	return json.Marshal(false)
}

const invalidRedactInfoLogTypeErrMsg = `the "redact-info-log" value is invalid; it should be either false, true, or "MARKER"`

// UnmarshalJSON implements the `json.Marshaler` interface to ensure the compatibility.
func (t *RedactInfoLogType) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	if err == nil && strings.ToUpper(s) == "MARKER" {
		*t = RedactInfoLogMarker
		return nil
	}
	var b bool
	err = json.Unmarshal(data, &b)
	if err != nil {
		return errors.New(invalidRedactInfoLogTypeErrMsg)
	}
	if b {
		*t = RedactInfoLogON
	} else {
		*t = RedactInfoLogOFF
	}
	return nil
}

// UnmarshalTOML implements the `toml.Unmarshaler` interface to ensure the compatibility.
func (t *RedactInfoLogType) UnmarshalTOML(data any) error {
	switch v := data.(type) {
	case bool:
		if v {
			*t = RedactInfoLogON
		} else {
			*t = RedactInfoLogOFF
		}
		return nil
	case string:
		if strings.ToUpper(v) == "MARKER" {
			*t = RedactInfoLogMarker
			return nil
		}
		return errors.New(invalidRedactInfoLogTypeErrMsg)
	default:
	}
	return errors.New(invalidRedactInfoLogTypeErrMsg)
}

var curRedactType atomic.Value

func init() {
	SetRedactType(RedactInfoLogOFF)
}

func getRedactType() RedactInfoLogType {
	return curRedactType.Load().(RedactInfoLogType)
}

func SetRedactType(redactInfoLogType RedactInfoLogType) {
	curRedactType.Store(redactInfoLogType)
}

const (
	leftMark  = '‹'
	rightMark = '›'
)

func redactInfo(input string) string {
	res := &strings.Builder{}
	res.Grow(len(input) + 2)
	_, _ = res.WriteRune(leftMark)
	for _, c := range input {
		// Double the mark character if it is already in the input string.
		// to avoid the ambiguity of the redacted content.
		if c == leftMark || c == rightMark {
			_, _ = res.WriteRune(c)
			_, _ = res.WriteRune(c)
		} else {
			_, _ = res.WriteRune(c)
		}
	}
	_, _ = res.WriteRune(rightMark)
	return res.String()
}

// ZapRedactString receives string argument and returns a zap.Field with the value either:
// - unchanged if redaction is disabled
// - replaced with "?" if redaction is enabled
// - wrapped in markers ‹› if marker mode is enabled
func ZapRedactString(key, arg string) zap.Field {
	switch getRedactType() {
	case RedactInfoLogON:
		return zap.String(key, "?")
	case RedactInfoLogMarker:
		return zap.String(key, redactInfo(arg))
	default:
	}
	return zap.String(key, arg)
}
