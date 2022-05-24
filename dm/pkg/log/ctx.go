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

package log

import (
	"context"

	"go.uber.org/zap"
)

type logCtxKeyType string

var logCtxKey = logCtxKeyType("zap-fields")

func getZapFieldsFromCtx(ctx context.Context) []zap.Field {
	// Use a pointer to key to save one allocation.
	fields := ctx.Value(&logCtxKey)
	if fields == nil {
		return nil
	}
	// Note that the value is a pointer to slice, to save allocation.
	return *fields.(*[]zap.Field)
}

// AppendZapFieldToCtx attaches new fields to the context, which can be then
// used with log.WithCtx().
func AppendZapFieldToCtx(ctx context.Context, newFields ...zap.Field) context.Context {
	fields := getZapFieldsFromCtx(ctx)
	if fields == nil {
		// 8 fields should be enough for most situations.
		fields = make([]zap.Field, 0, 8)
	}
	fields = append(fields, newFields...)

	return context.WithValue(ctx, &logCtxKey, &fields)
}
