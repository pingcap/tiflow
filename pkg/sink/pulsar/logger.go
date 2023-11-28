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
	plog "github.com/pingcap/log"
	"go.uber.org/zap"
)

// Logger wrapper cdc logger to adapt pulsar logger
type Logger struct {
	zapLogger *zap.Logger
}

// SubLogger sub
func (p *Logger) SubLogger(fields log.Fields) log.Logger {
	subLogger := p.zapLogger
	for k, v := range fields {
		subLogger = subLogger.With(zap.Any(k, v))
	}
	return &Logger{subLogger}
}

// WithFields with fields
func (p *Logger) WithFields(fields log.Fields) log.Entry {
	return &LoggerEntry{
		fields: fields,
		logger: p.zapLogger,
	}
}

// WithField with field
func (p *Logger) WithField(name string, value interface{}) log.Entry {
	return &LoggerEntry{
		fields: log.Fields{name: value},
		logger: p.zapLogger,
	}
}

// WithError error
func (p *Logger) WithError(err error) log.Entry {
	return &LoggerEntry{
		fields: log.Fields{"error": err},
		logger: p.zapLogger,
	}
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
func NewPulsarLogger() *Logger {
	return &Logger{
		zapLogger: plog.L(),
	}
}

// LoggerEntry pulsar logger entry
type LoggerEntry struct {
	fields log.Fields
	logger *zap.Logger
}

// WithFields with fields
func (p *LoggerEntry) WithFields(fields log.Fields) log.Entry {
	p.fields = fields
	return p
}

// WithField with field
func (p *LoggerEntry) WithField(name string, value interface{}) log.Entry {
	p.fields[name] = value
	return p
}

// Debug debug
func (p *LoggerEntry) Debug(args ...interface{}) {
	p.logger.Sugar().Debug(args...)
}

// Info info
func (p *LoggerEntry) Info(args ...interface{}) {
	p.logger.Sugar().Info(args...)
}

// Warn warn
func (p *LoggerEntry) Warn(args ...interface{}) {
	p.logger.Sugar().Warn(args...)
}

// Error error
func (p *LoggerEntry) Error(args ...interface{}) {
	p.logger.Sugar().Error(args...)
}

// Debugf debugf
func (p *LoggerEntry) Debugf(format string, args ...interface{}) {
	p.logger.Sugar().Debugf(format, args...)
}

// Infof infof
func (p *LoggerEntry) Infof(format string, args ...interface{}) {
	p.logger.Sugar().Infof(format, args...)
}

// Warnf warnf
func (p *LoggerEntry) Warnf(format string, args ...interface{}) {
	p.logger.Sugar().Warnf(format, args...)
}

// Errorf errorf
func (p *LoggerEntry) Errorf(format string, args ...interface{}) {
	p.logger.Sugar().Errorf(format, args...)
}
