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
	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

	err = initSaramaLogger(cfg.Level)
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
func initSaramaLogger(logLevel string) error {
	var level zapcore.Level
	err := level.UnmarshalText([]byte(logLevel))
	if err != nil {
		return errors.Trace(err)
	}
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
