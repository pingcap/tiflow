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

	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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

func TestShouldReturnTableInfo(t *testing.T) {
	ctx := PutTableInfoInCtx(context.Background(), 1321, "ello")
	tableID, tableName := TableIDFromCtx(ctx)
	require.Equal(t, int64(1321), tableID)
	require.Equal(t, "ello", tableName)
}

func TestTableInfoNotSet(t *testing.T) {
	tableID, tableName := TableIDFromCtx(context.Background())
	require.Equal(t, int64(0), tableID)
	require.Equal(t, "", tableName)
	ctx := context.WithValue(context.Background(), ctxKeyTableID, 1321)
	tableID, tableName = TableIDFromCtx(ctx)
	require.Equal(t, int64(0), tableID)
	require.Equal(t, "", tableName)
}

func TestShouldReturnKVStorage(t *testing.T) {
	kvStorage, _ := mockstore.NewMockStore()
	defer kvStorage.Close()
	ctx := PutKVStorageInCtx(context.Background(), kvStorage)
	kvStorage2, err := KVStorageFromCtx(ctx)
	require.Equal(t, kvStorage, kvStorage2)
	require.Nil(t, err)
}

func TestKVStorageNotSet(t *testing.T) {
	// Context not set value
	kvStorage, err := KVStorageFromCtx(context.Background())
	require.Nil(t, kvStorage)
	require.NotNil(t, err)
	// Type of value is not kv.Storage
	ctx := context.WithValue(context.Background(), ctxKeyKVStorage, 1321)
	kvStorage, err = KVStorageFromCtx(ctx)
	require.Nil(t, kvStorage)
	require.NotNil(t, err)
}

func TestZapFieldWithContext(t *testing.T) {
	var (
		capture    string = "127.0.0.1:8200"
		changefeed        = model.DefaultChangeFeedID("test-cf")
	)
	ctx := context.Background()
	ctx = PutCaptureAddrInCtx(ctx, capture)
	ctx = PutChangefeedIDInCtx(ctx, changefeed)
	require.Equal(t, zap.String("capture", capture), ZapFieldCapture(ctx))
	require.Equal(t, zap.String("changefeed", changefeed.ID), ZapFieldChangefeed(ctx))
}
