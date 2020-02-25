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

package util

import "context"

type ctxKey string

const (
	CtxKeyCaptureID    = ctxKey("captureID")
	CtxKeyChangefeedID = ctxKey("changefeedID")
	CtxKeyTableID      = ctxKey("tableID")
)

// GetValueFromCtx returns a string value stored in context based on given ctxKey.
// It returns an empty string if there's no valid ctxKey found.
func GetValueFromCtx(ctx context.Context, key ctxKey) string {
	value, ok := ctx.Value(key).(string)
	if !ok {
		return ""
	}
	return value
}

// PutValueInCtx returns a new child context with the specified value stored.
func PutValueInCtx(ctx context.Context, key ctxKey, value string) context.Context {
	return context.WithValue(ctx, key, value)
}
