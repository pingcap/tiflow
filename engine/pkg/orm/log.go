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

	"go.uber.org/zap"
	"gorm.io/gorm/logger"
)

type loggerOption struct {
	slowThreshold time.Duration
}

type optionFunc func(*loggerOption)

// WithSlowThreshold set the slow log threshold for gorm log
func WithSlowThreshold(thres time.Duration) optionFunc {
	return func(op *loggerOption) {
		op.slowThreshold = thres
	}
}

// NewOrmLogger return a logger which implements logger.Interface
func NewOrmLogger(logger *zap.Logger, opts ...optionFunc) logger.Interface {
	var op loggerOption
	for _, opt := range opts {
		opt(&op)
	}

	return &ormLogger{
		op:     op,
		logger: logger,
	}
}

type ormLogger struct {
	op     loggerOption
	logger *zap.Logger
}

func (l *ormLogger) LogMode(logger.LogLevel) logger.Interface {
	// TODO implement me
	return l
}

func (l *ormLogger) Info(ctx context.Context, format string, args ...interface{}) {
	l.logger.Info("gorm info log", zap.String("detail", fmt.Sprintf(format, args...)))
}

func (l *ormLogger) Warn(ctx context.Context, format string, args ...interface{}) {
	l.logger.Warn("gorm warn log", zap.String("detail", fmt.Sprintf(format, args...)))
}

func (l *ormLogger) Error(ctx context.Context, format string, args ...interface{}) {
	l.logger.Error("gorm error log", zap.String("detail", fmt.Sprintf(format, args...)))
}

func (l *ormLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	elapsed := time.Since(begin)
	if elapsed > l.op.slowThreshold && l.op.slowThreshold != 0 {
		sql, rows := fc()
		l.logger.Warn("grom slow log", zap.Duration("elapsed", elapsed), zap.String("sql", sql),
			zap.Int64("affected-rows", rows))
	}
}
