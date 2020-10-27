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
	"context"
	"time"

	"github.com/pingcap/tidb/kv"
)

type ctxKey string

const (
	ctxKeyTableID      = ctxKey("tableID")
	ctxKeyCaptureAddr  = ctxKey("captureAddr")
	ctxKeyChangefeedID = ctxKey("changefeedID")
	ctxKeyIsOwner      = ctxKey("isOwner")
	ctxKeyTimezone     = ctxKey("timezone")
	ctxKeyKVStorage    = ctxKey("kvStorage")
)

// CaptureAddrFromCtx returns a capture ID stored in the specified context.
// It returns an empty string if there's no valid capture ID found.
func CaptureAddrFromCtx(ctx context.Context) string {
	captureAddr, ok := ctx.Value(ctxKeyCaptureAddr).(string)
	if !ok {
		return ""
	}
	return captureAddr
}

// PutCaptureAddrInCtx returns a new child context with the specified capture ID stored.
func PutCaptureAddrInCtx(ctx context.Context, captureAddr string) context.Context {
	return context.WithValue(ctx, ctxKeyCaptureAddr, captureAddr)
}

// PutTimezoneInCtx returns a new child context with the given timezone
func PutTimezoneInCtx(ctx context.Context, timezone *time.Location) context.Context {
	return context.WithValue(ctx, ctxKeyTimezone, timezone)
}

// PutKVStorageInCtx returns a new child context with the given tikv store
func PutKVStorageInCtx(ctx context.Context, store kv.Storage) context.Context {
	return context.WithValue(ctx, ctxKeyKVStorage, store)
}

type tableinfo struct {
	id   int64
	name string
}

// PutTableInfoInCtx returns a new child context with the specified table ID and name stored.
func PutTableInfoInCtx(ctx context.Context, tableID int64, tableName string) context.Context {
	return context.WithValue(ctx, ctxKeyTableID, tableinfo{id: tableID, name: tableName})
}

// TableIDFromCtx returns a table ID
func TableIDFromCtx(ctx context.Context) (int64, string) {
	info, ok := ctx.Value(ctxKeyTableID).(tableinfo)
	if !ok {
		return 0, ""
	}
	return info.id, info.name
}

// TimezoneFromCtx returns a timezone
func TimezoneFromCtx(ctx context.Context) *time.Location {
	tz, ok := ctx.Value(ctxKeyTimezone).(*time.Location)
	if !ok {
		return nil
	}
	return tz
}

// KVStorageFromCtx returns a tikv store
func KVStorageFromCtx(ctx context.Context) kv.Storage {
	store, ok := ctx.Value(ctxKeyKVStorage).(kv.Storage)
	if !ok {
		return nil
	}
	return store
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
