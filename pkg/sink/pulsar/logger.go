// Copyright 2023 PingCAP, Inc.
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

package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar/log"
	"go.uber.org/zap"
)

// Logger wrapper cdc logger to adapt pulsar logger
type Logger struct {
	zapLogger *zap.Logger
}

// SubLogger sub
func (p *Logger) SubLogger(pulsarFields log.Fields) log.Logger {
	zapFields := make([]zap.Field, 0, len(pulsarFields))
	for k, v := range pulsarFields {
		zapFields = append(zapFields, zap.Any(k, v))
	}
	return &Logger{p.zapLogger.With(zapFields...)}
}

// WithFields with fields
func (p *Logger) WithFields(fields log.Fields) log.Entry {
	return p.SubLogger(fields)
}

// WithField with field
func (p *Logger) WithField(name string, value interface{}) log.Entry {
	return &Logger{p.zapLogger.With(zap.Any(name, value))}
}

// WithError error
func (p *Logger) WithError(err error) log.Entry {
	return &Logger{p.zapLogger.With(zap.Error(err))}
}

// Debug debug
func (p *Logger) Debug(args ...interface{}) {
	p.zapLogger.Sugar().Debug(args...)
}

// Info info
func (p *Logger) Info(args ...interface{}) {
	p.zapLogger.Sugar().Info(args...)
}

// Warn warn
func (p *Logger) Warn(args ...interface{}) {
	p.zapLogger.Sugar().Warn(args...)
}

// Error error
func (p *Logger) Error(args ...interface{}) {
	p.zapLogger.Sugar().Error(args...)
}

// Debugf debugf
func (p *Logger) Debugf(format string, args ...interface{}) {
	p.zapLogger.Sugar().Debugf(format, args...)
}

// Infof infof
func (p *Logger) Infof(format string, args ...interface{}) {
	p.zapLogger.Sugar().Infof(format, args...)
}

// Warnf warnf
func (p *Logger) Warnf(format string, args ...interface{}) {
	p.zapLogger.Sugar().Warnf(format, args...)
}

// Errorf errorf
func (p *Logger) Errorf(format string, args ...interface{}) {
	p.zapLogger.Sugar().Errorf(format, args...)
}

// NewPulsarLogger new pulsar logger
func NewPulsarLogger(base *zap.Logger) *Logger {
	return &Logger{
		zapLogger: base.WithOptions(zap.AddCallerSkip(1)),
	}
}
