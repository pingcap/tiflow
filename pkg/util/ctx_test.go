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

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"go.uber.org/zap"
)

type ctxValueSuite struct{}

var _ = check.Suite(&ctxValueSuite{})

func (s *ctxValueSuite) TestShouldReturnCaptureID(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := PutCaptureAddrInCtx(context.Background(), "ello")
	c.Assert(CaptureAddrFromCtx(ctx), check.Equals, "ello")
}

func (s *ctxValueSuite) TestCaptureIDNotSet(c *check.C) {
	defer testleak.AfterTest(c)()
	c.Assert(CaptureAddrFromCtx(context.Background()), check.Equals, "")
	captureAddr := CaptureAddrFromCtx(context.Background())
	c.Assert(captureAddr, check.Equals, "")
	ctx := context.WithValue(context.Background(), ctxKeyCaptureAddr, 1321)
	c.Assert(CaptureAddrFromCtx(ctx), check.Equals, "")
}

func (s *ctxValueSuite) TestShouldReturnChangefeedID(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := PutChangefeedIDInCtx(context.Background(), "ello")
	c.Assert(ChangefeedIDFromCtx(ctx), check.Equals, "ello")
}

func (s *ctxValueSuite) TestCanceledContext(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := PutChangefeedIDInCtx(context.Background(), "test-cf")
	c.Assert(ChangefeedIDFromCtx(ctx), check.Equals, "test-cf")
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	c.Assert(ChangefeedIDFromCtx(ctx), check.Equals, "test-cf")
}

func (s *ctxValueSuite) TestChangefeedIDNotSet(c *check.C) {
	defer testleak.AfterTest(c)()
	c.Assert(ChangefeedIDFromCtx(context.Background()), check.Equals, "")
	changefeedID := ChangefeedIDFromCtx(context.Background())
	c.Assert(changefeedID, check.Equals, "")
	ctx := context.WithValue(context.Background(), ctxKeyChangefeedID, 1321)
	changefeedID = ChangefeedIDFromCtx(ctx)
	c.Assert(changefeedID, check.Equals, "")
}

func (s *ctxValueSuite) TestShouldReturnTimezone(c *check.C) {
	defer testleak.AfterTest(c)()
	tz, _ := getTimezoneFromZonefile("UTC")
	ctx := PutTimezoneInCtx(context.Background(), tz)
	tz = TimezoneFromCtx(ctx)
	c.Assert(tz.String(), check.Equals, "UTC")
}

func (s *ctxValueSuite) TestTimezoneNotSet(c *check.C) {
	defer testleak.AfterTest(c)()
	tz := TimezoneFromCtx(context.Background())
	c.Assert(tz, check.IsNil)
	ctx := context.WithValue(context.Background(), ctxKeyTimezone, 1321)
	tz = TimezoneFromCtx(ctx)
	c.Assert(tz, check.IsNil)
}

func (s *ctxValueSuite) TestShouldReturnTableInfo(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := PutTableInfoInCtx(context.Background(), 1321, "ello")
	tableID, tableName := TableIDFromCtx(ctx)
	c.Assert(tableID, check.Equals, int64(1321))
	c.Assert(tableName, check.Equals, "ello")
}

func (s *ctxValueSuite) TestTableInfoNotSet(c *check.C) {
	defer testleak.AfterTest(c)()
	tableID, tableName := TableIDFromCtx(context.Background())
	c.Assert(tableID, check.Equals, int64(0))
	c.Assert(tableName, check.Equals, "")
	ctx := context.WithValue(context.Background(), ctxKeyTableID, 1321)
	tableID, tableName = TableIDFromCtx(ctx)
	c.Assert(tableID, check.Equals, int64(0))
	c.Assert(tableName, check.Equals, "")
}

func (s *ctxValueSuite) TestShouldReturnKVStorage(c *check.C) {
	defer testleak.AfterTest(c)()
	kvStorage, _ := mockstore.NewMockStore()
	defer kvStorage.Close()
	ctx := PutKVStorageInCtx(context.Background(), kvStorage)
	kvStorage2, err := KVStorageFromCtx(ctx)
	c.Assert(kvStorage2, check.DeepEquals, kvStorage)
	c.Assert(err, check.IsNil)
}

func (s *ctxValueSuite) TestKVStorageNotSet(c *check.C) {
	defer testleak.AfterTest(c)()
	// Context not set value
	kvStorage, err := KVStorageFromCtx(context.Background())
	c.Assert(kvStorage, check.IsNil)
	c.Assert(err, check.NotNil)
	// Type of value is not kv.Storage
	ctx := context.WithValue(context.Background(), ctxKeyKVStorage, 1321)
	kvStorage, err = KVStorageFromCtx(ctx)
	c.Assert(kvStorage, check.IsNil)
	c.Assert(err, check.NotNil)
}

func (s *ctxValueSuite) TestZapFieldWithContext(c *check.C) {
	defer testleak.AfterTest(c)()
	var (
		capture    string = "127.0.0.1:8200"
		changefeed string = "test-cf"
	)
	ctx := context.Background()
	ctx = PutCaptureAddrInCtx(ctx, capture)
	ctx = PutChangefeedIDInCtx(ctx, changefeed)
	c.Assert(ZapFieldCapture(ctx), check.DeepEquals, zap.String("capture", capture))
	c.Assert(ZapFieldChangefeed(ctx), check.DeepEquals, zap.String("changefeed", changefeed))
}
