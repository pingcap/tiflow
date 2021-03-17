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

package cdclog

import (
	"context"
	"io/ioutil"
	"net/url"
	"time"

	"github.com/pingcap/ticdc/pkg/util/testleak"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/codec"
)

type testFileSuite struct{}

var _ = check.Suite(&testFileSuite{})

func (tfs *testFileSuite) SetUpSuite(c *check.C) {
}

func (tfs *testFileSuite) TearDownSuite(c *check.C) {
}

func (tfs *testFileSuite) TestFileFlush(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fiSink, err := NewLocalFileSink(ctx, &url.URL{Scheme: "local", Path: "/tmp/"}, make(chan error))
	if err != nil || fiSink == nil {
		c.Fail()
	}

	time.Sleep(time.Second)

	for i := 0; i < 10000; i++ {
		rowChange := &model.RowChangedEvent{
			Table:           &model.TableName{Schema: "db1", Table: "test", TableID: 1},
			ApproximateSize: 1000,
		}
		if fiSink.EmitRowChangedEvents(ctx, rowChange) != nil {
			c.Fail()
		}
	}

	_, errs := fiSink.logSink.flushRowChangedEvents(ctx, 10000)
	if errs != nil {
		c.Fail()
	}

	// ensure 5 seconds later
	time.Sleep(time.Second * 6)

	// simulate restart
	fiSink.logSink.units = make([]logUnit, 0)

	var key interface{} = int64(1)
	fiSink.logSink.hashMap.Delete(key)

	// write data again
	for i := 0; i < 10000; i++ {
		rowChange := &model.RowChangedEvent{
			Table:           &model.TableName{Schema: "db1", Table: "test", TableID: 1},
			ApproximateSize: 1000,
		}
		if fiSink.EmitRowChangedEvents(ctx, rowChange) != nil {
			c.Fail()
		}
	}

	_, e := fiSink.logSink.flushRowChangedEvents(ctx, 10001)

	if e != nil {
		c.Fail()
	}

	time.Sleep(time.Second * 5)

	bytes, err := ioutil.ReadFile("/tmp/t_1/cdclog")
	if err != nil {
		c.Fail()
	}

	decoder, err := codec.NewJSONEventBatchDecoder(bytes, nil)
	if err != nil {
		c.Fail()
	}
	for {
		_, hasNext, err := decoder.HasNext()
		if err != nil {
			c.Fail()
			break
		}

		if !hasNext {
			break
		}

		event, e := decoder.NextRowChangedEvent()
		if e != nil {
			c.Fail()
			break
		}

		// TableID is omited...
		c.Assert(event.Table, check.DeepEquals, &model.TableName{Schema: "db1", Table: "test", TableID: 0})
	}
}
