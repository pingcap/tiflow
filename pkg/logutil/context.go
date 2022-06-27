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

package logutil

import (
	"context"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type ctxLogKeyType struct{}

// ctxLogKey indicates the context key for logger
var ctxLogKey = ctxLogKeyType{}

// FromContext return the logger in context, or return global logger
// if logger not found in context
func FromContext(ctx context.Context) *zap.Logger {
	if ctxlogger, ok := ctx.Value(ctxLogKey).(*zap.Logger); ok {
		return ctxlogger
	}

	return log.L()
}

// NewContextWithLogger attaches a new logger to context and return the context
func NewContextWithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, ctxLogKey, logger)
}
