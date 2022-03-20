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
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"go.uber.org/zap"
)

func TestShouldReturnCaptureID(t *testing.T) {
	defer testleak.AfterTest(t)()
	ctx := PutCaptureAddrInCtx(context.Background(), "ello")
	require.Equal(t, CaptureAddrFromCtx(ctx), "ello")
}

func TestCaptureIDNotSet(t *testing.T) {
	defer testleak.AfterTest(t)()
	require.Equal(t, CaptureAddrFromCtx(context.Background()), "")
	captureAddr := CaptureAddrFromCtx(context.Background())
	require.Equal(t, captureAddr, "")
	ctx := context.WithValue(context.Background(), ctxKeyCaptureAddr, 1321)
	require.Equal(t, CaptureAddrFromCtx(ctx), "")
}

func TestShouldReturnChangefeedID(t *testing.T) {
	defer testleak.AfterTest(t)()
	ctx := PutChangefeedIDInCtx(context.Background(), "ello")
	require.Equal(t, ChangefeedIDFromCtx(ctx), "ello")
}

func TestCanceledContext(t *testing.T) {
	defer testleak.AfterTest(t)()
	ctx := PutChangefeedIDInCtx(context.Background(), "test-cf")
	require.Equal(t, ChangefeedIDFromCtx(ctx), "test-cf")
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	require.Equal(t, ChangefeedIDFromCtx(ctx), "test-cf")
}

func TestChangefeedIDNotSet(t *testing.T) {
	defer testleak.AfterTest(t)()
	require.Equal(t, ChangefeedIDFromCtx(context.Background()), "")
	changefeedID := ChangefeedIDFromCtx(context.Background())
	require.Equal(t, changefeedID, "")
	ctx := context.WithValue(context.Background(), ctxKeyChangefeedID, 1321)
	changefeedID = ChangefeedIDFromCtx(ctx)
	require.Equal(t, changefeedID, "")
}

func TestShouldReturnTimezone(t *testing.T) {
	defer testleak.AfterTest(t)()
	tz, _ := getTimezoneFromZonefile("UTC")
	ctx := PutTimezoneInCtx(context.Background(), tz)
	tz = TimezoneFromCtx(ctx)
	require.Equal(t, tz.String(), "UTC")
}

func TestTimezoneNotSet(t *testing.T) {
	defer testleak.AfterTest(t)()
	tz := TimezoneFromCtx(context.Background())
	require.Nil(t, tz)
	ctx := context.WithValue(context.Background(), ctxKeyTimezone, 1321)
	tz = TimezoneFromCtx(ctx)
	require.Nil(t, tz)
}

func TestShouldReturnTableInfo(t *testing.T) {
	defer testleak.AfterTest(t)()
	ctx := PutTableInfoInCtx(context.Background(), 1321, "ello")
	tableID, tableName := TableIDFromCtx(ctx)
	require.Equal(t, tableID, int64(1321))
	require.Equal(t, tableName, "ello")
}

func TestTableInfoNotSet(t *testing.T) {
	defer testleak.AfterTest(t)()
	tableID, tableName := TableIDFromCtx(context.Background())
	require.Equal(t, tableID, int64(0))
	require.Equal(t, tableName, "")
	ctx := context.WithValue(context.Background(), ctxKeyTableID, 1321)
	tableID, tableName = TableIDFromCtx(ctx)
	require.Equal(t, tableID, int64(0))
	require.Equal(t, tableName, "")
}

func TestShouldReturnKVStorage(t *testing.T) {
	defer testleak.AfterTest(t)()
	kvStorage, _ := mockstore.NewMockStore()
	defer kvStorage.Close()
	ctx := PutKVStorageInCtx(context.Background(), kvStorage)
	kvStorage2, err := KVStorageFromCtx(ctx)
	require.Equal(t, kvStorage2, kvStorage)
	require.Nil(t, err)
}

func TestKVStorageNotSet(t *testing.T) {
	defer testleak.AfterTest(t)()
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
	defer testleak.AfterTest(t)()
	var (
		capture    string = "127.0.0.1:8200"
		changefeed string = "test-cf"
	)
	ctx := context.Background()
	ctx = PutCaptureAddrInCtx(ctx, capture)
	ctx = PutChangefeedIDInCtx(ctx, changefeed)
	require.Equal(t, ZapFieldCapture(ctx), zap.String("capture", capture))
	require.Equal(t, ZapFieldChangefeed(ctx), zap.String("changefeed", changefeed))
}
