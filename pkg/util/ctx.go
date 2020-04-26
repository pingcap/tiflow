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

import (
	"context"
	"time"
)

type ctxKey string

const (
	ctxKeyTableID      = ctxKey("tableID")
	ctxKeyCaptureID    = ctxKey("captureID")
	ctxKeyChangefeedID = ctxKey("changefeedID")
	ctxKeyIsOwner      = ctxKey("isOwner")
	ctxKeyTimezone     = ctxKey("timezone")
)

// CaptureIDFromCtx returns a capture ID stored in the specified context.
// It returns an empty string if there's no valid capture ID found.
func CaptureIDFromCtx(ctx context.Context) string {
	captureID, ok := ctx.Value(ctxKeyCaptureID).(string)
	if !ok {
		return ""
	}
	return captureID
}

// PutCaptureIDInCtx returns a new child context with the specified capture ID stored.
func PutCaptureIDInCtx(ctx context.Context, captureID string) context.Context {
	return context.WithValue(ctx, ctxKeyCaptureID, captureID)
}

// PutTimezoneInCtx returns a new child context with the given timezone
func PutTimezoneInCtx(ctx context.Context, timezone *time.Location) context.Context {
	return context.WithValue(ctx, ctxKeyTimezone, timezone)
}

// PutTableIDInCtx returns a new child context with the specified table ID stored.
func PutTableIDInCtx(ctx context.Context, tableID int64) context.Context {
	return context.WithValue(ctx, ctxKeyTableID, tableID)
}

// TableIDFromCtx returns a table ID
func TableIDFromCtx(ctx context.Context) int64 {
	tableID, ok := ctx.Value(ctxKeyTableID).(int64)
	if !ok {
		return 0
	}
	return tableID
}

// TimezoneFromCtx returns a timezone
func TimezoneFromCtx(ctx context.Context) *time.Location {
	tz, ok := ctx.Value(ctxKeyTimezone).(*time.Location)
	if !ok {
		return nil
	}
	return tz
}

// SetOwnerInCtx returns a new child context with the owner flag set.
func SetOwnerInCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeyIsOwner, true)
}

// IsOwnerFromCtx returns true if this capture is owner
func IsOwnerFromCtx(ctx context.Context) bool {
	isOwner := ctx.Value(ctxKeyIsOwner)
	return isOwner != nil && isOwner.(bool)
}

// ChangefeedIDFromCtx returns a changefeedID stored in the specified context.
// It returns an empty string if there's no valid changefeed ID found.
func ChangefeedIDFromCtx(ctx context.Context) string {
	changefeedID, ok := ctx.Value(ctxKeyChangefeedID).(string)
	if !ok {
		return ""
	}
	return changefeedID
}

// PutChangefeedIDInCtx returns a new child context with the specified changefeed ID stored.
func PutChangefeedIDInCtx(ctx context.Context, changefeedID string) context.Context {
	return context.WithValue(ctx, ctxKeyChangefeedID, changefeedID)
}
