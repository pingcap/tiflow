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

package contextutil

import (
	"context"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
)

type ctxKey string

const (
	ctxKeyCaptureAddr  = ctxKey("captureAddr")
	ctxKeyChangefeedID = ctxKey("changefeedID")
	ctxKeyTimezone     = ctxKey("timezone")
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

// TimezoneFromCtx returns a timezone
func TimezoneFromCtx(ctx context.Context) *time.Location {
	tz, ok := ctx.Value(ctxKeyTimezone).(*time.Location)
	if !ok {
		return nil
	}
	return tz
}

// ChangefeedIDFromCtx returns a changefeedID stored in the specified context.
// It returns an empty model.changefeedID if there's no changefeedID found.
func ChangefeedIDFromCtx(ctx context.Context) model.ChangeFeedID {
	changefeedID, ok := ctx.Value(ctxKeyChangefeedID).(model.ChangeFeedID)
	if !ok {
		return model.ChangeFeedID{}
	}
	return changefeedID
}

// PutChangefeedIDInCtx returns a new child context with the specified changefeedID stored.
func PutChangefeedIDInCtx(ctx context.Context, changefeedID model.ChangeFeedID) context.Context {
	return context.WithValue(ctx, ctxKeyChangefeedID, changefeedID)
}
