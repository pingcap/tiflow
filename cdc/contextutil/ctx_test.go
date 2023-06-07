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
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestShouldReturnCaptureID(t *testing.T) {
	ctx := PutCaptureAddrInCtx(context.Background(), "ello")
	require.Equal(t, "ello", CaptureAddrFromCtx(ctx))
}

func TestCaptureIDNotSet(t *testing.T) {
	require.Equal(t, "", CaptureAddrFromCtx(context.Background()))
	captureAddr := CaptureAddrFromCtx(context.Background())
	require.Equal(t, "", captureAddr)
	ctx := context.WithValue(context.Background(), ctxKeyCaptureAddr, 1321)
	require.Equal(t, "", CaptureAddrFromCtx(ctx))
}

func TestShouldReturnChangefeedID(t *testing.T) {
	ctx := PutChangefeedIDInCtx(context.Background(), model.DefaultChangeFeedID("ello"))
	require.Equal(t, model.DefaultChangeFeedID("ello"), ChangefeedIDFromCtx(ctx))
}

func TestCanceledContext(t *testing.T) {
	ctx := PutChangefeedIDInCtx(context.Background(), model.DefaultChangeFeedID("test-cf"))
	require.Equal(t, model.DefaultChangeFeedID("test-cf"), ChangefeedIDFromCtx(ctx))
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	require.Equal(t, model.DefaultChangeFeedID("test-cf"), ChangefeedIDFromCtx(ctx))
}

func TestChangefeedIDNotSet(t *testing.T) {
	require.Equal(t, "", ChangefeedIDFromCtx(context.Background()).ID)
	changefeedID := ChangefeedIDFromCtx(context.Background())
	require.Equal(t, "", changefeedID.ID)
	ctx := context.WithValue(context.Background(), ctxKeyChangefeedID, 1321)
	changefeedID = ChangefeedIDFromCtx(ctx)
	require.Equal(t, "", changefeedID.ID)
}

func TestShouldReturnTimezone(t *testing.T) {
	tz, _ := util.GetTimezoneFromZonefile("UTC")
	ctx := PutTimezoneInCtx(context.Background(), tz)
	tz = TimezoneFromCtx(ctx)
	require.Equal(t, "UTC", tz.String())
}

func TestTimezoneNotSet(t *testing.T) {
	tz := TimezoneFromCtx(context.Background())
	require.Nil(t, tz)
	ctx := context.WithValue(context.Background(), ctxKeyTimezone, 1321)
	tz = TimezoneFromCtx(ctx)
	require.Nil(t, tz)
}
