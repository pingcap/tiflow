// Copyright 2026 PingCAP, Inc.
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
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	retryLogSampleInterval = 5 * time.Minute
	retryLogSampleFirst    = 1
)

func sampleLoggerFactory(base *zap.Logger, tick time.Duration, first int, fields ...zap.Field) func() *zap.Logger {
	if base == nil {
		base = zap.NewNop()
	}

	var (
		once   sync.Once
		logger *zap.Logger
	)

	return func() *zap.Logger {
		once.Do(func() {
			sampleCore := zap.WrapCore(func(core zapcore.Core) zapcore.Core {
				return zapcore.NewSamplerWithOptions(core, tick, first, 0)
			})
			logger = base.With(fields...).With(zap.String("sampled", "")).WithOptions(sampleCore)
		})
		return logger
	}
}

// NewRetrySampleLogger creates a logger that only prints the first repeated
// retry log with the same level and message during the sampling window.
func NewRetrySampleLogger(base Logger, fields ...zap.Field) *zap.Logger {
	return sampleLoggerFactory(base.Logger, retryLogSampleInterval, retryLogSampleFirst, fields...)()
}
