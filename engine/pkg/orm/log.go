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
	"fmt"
	"time"

	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type loggerOption struct {
	slowThreshold                time.Duration
	ignoreTraceRecordNotFoundErr bool
}

type optionFunc func(*loggerOption)

// WithSlowThreshold sets the slow log threshold for gorm log
func WithSlowThreshold(thres time.Duration) optionFunc {
	return func(op *loggerOption) {
		op.slowThreshold = thres
	}
}

// WithIgnoreTraceRecordNotFoundErr sets if ignore 'record not found' error for trace
func WithIgnoreTraceRecordNotFoundErr() optionFunc {
	return func(op *loggerOption) {
		op.ignoreTraceRecordNotFoundErr = true
	}
}

// NewOrmLogger returns a logger which implements logger.Interface
func NewOrmLogger(lg *zap.Logger, opts ...optionFunc) logger.Interface {
	var op loggerOption
	for _, opt := range opts {
		opt(&op)
	}

	return &ormLogger{
		op: op,
		lg: lg,
	}
}

type ormLogger struct {
	op loggerOption
	lg *zap.Logger
}

func (l *ormLogger) LogMode(logger.LogLevel) logger.Interface {
	// TODO implement me
	return l
}

func (l *ormLogger) Info(ctx context.Context, format string, args ...interface{}) {
	l.lg.Info(fmt.Sprintf(format, args...))
}

func (l *ormLogger) Warn(ctx context.Context, format string, args ...interface{}) {
	l.lg.Warn(fmt.Sprintf(format, args...))
}

func (l *ormLogger) Error(ctx context.Context, format string, args ...interface{}) {
	l.lg.Error(fmt.Sprintf(format, args...))
}

func (l *ormLogger) Trace(ctx context.Context, begin time.Time, resFunc func() (sql string, rowsAffected int64), err error) {
	elapsed := time.Since(begin)
	sql, rows := resFunc()
	var logFunc func(string, ...zap.Field)
	if err != nil && (!errors.Is(err, gorm.ErrRecordNotFound) || !l.op.ignoreTraceRecordNotFoundErr) {
		logFunc = l.lg.Error
	} else {
		logFunc = l.lg.Debug
	}
	var fields []zap.Field
	fields = append(fields, zap.Duration("elapsed", elapsed), zap.String("sql", sql), zap.Int64("affected-rows", rows))
	if err != nil {
		fields = append(fields, zap.Error(err))
	}

	logFunc("trace log", fields...)

	// Split the slow log if we need
	if elapsed > l.op.slowThreshold && l.op.slowThreshold != 0 {
		l.lg.Warn("slow log", fields...)
	}
}
