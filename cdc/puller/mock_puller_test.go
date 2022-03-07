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

package puller

/*
import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
)

type mockPullerSuite struct{}

var _ = check.Suite(&mockPullerSuite{})

func (s *mockPullerSuite) TestTxnSort(c *check.C) {
		defer testleak.AfterTest(c)()
	pm := NewMockPullerManager(c, true)
	plr := pm.CreatePuller(0, []regionspan.Span{regionspan.Span{}.Hack()})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := uint64(0)
	go func() {
		err := plr.CollectRawTxns(ctx, func(ctx context.Context, txn model.RawTxn) error {
			c.Assert(ts, check.Less, txn.Ts)
			atomic.StoreUint64(&ts, txn.Ts)
			return nil
		})
		c.Assert(err, check.IsNil)
	}()
	pm.Run(context.Background())
	pm.MustExec("create table test.test(id varchar(255) primary key, a int)")
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 1, 1)
	pm.MustExec("update test.test set id = ? where a = ?", 6, 1)
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 2, 2)
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 3, 3)
	pm.MustExec("delete from test.test")
	waitForGrowingTs(&ts, oracle.GoTimeToTS(time.Now()))
}

func (s *mockPullerSuite) TestDDLPuller(c *check.C) {
		defer testleak.AfterTest(c)()
	pm := NewMockPullerManager(c, true)
	plr := pm.CreatePuller(0, []regionspan.Span{regionspan.GetDDLSpan()})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := uint64(0)
	txnMounter := entry.NewTxnMounter(nil)
	go func() {
		err := plr.CollectRawTxns(ctx, func(ctx context.Context, rawTxn model.RawTxn) error {
			c.Assert(ts, check.Less, rawTxn.Ts)
			atomic.StoreUint64(&ts, rawTxn.Ts)
			if len(rawTxn.Entries) == 0 {
				return nil
			}
			for _, e := range rawTxn.Entries {
				c.Assert(util.KeyInSpan(e.Key, regionspan.GetDDLSpan()), check.IsTrue)
			}
			t, err := txnMounter.Mount(rawTxn)
			c.Assert(err, check.IsNil)
			if !t.IsDDL() {
				return nil
			}
			c.Assert(t.DDL.Table, check.Equals, "test")
			c.Assert(t.DDL.Database, check.Equals, "test")
			return nil
		})
		c.Assert(err, check.IsNil)
	}()
	pm.Run(context.Background())
	pm.MustExec("create table test.test(id varchar(255) primary key, a int)")
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 1, 1)
	pm.MustExec("update test.test set id = ? where a = ?", 6, 1)
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 2, 2)
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 3, 3)
	pm.MustExec("delete from test.test")
	waitForGrowingTs(&ts, oracle.GoTimeToTS(time.Now()))
}

func (s *mockPullerSuite) TestStartTs(c *check.C) {
		defer testleak.AfterTest(c)()
	pm := NewMockPullerManager(c, true)
	plrA := pm.CreatePuller(0, []regionspan.Span{regionspan.Span{}.Hack()})
	ctx, cancel := context.WithCancel(context.Background())
	ts := uint64(0)
	var rawTxns []model.RawTxn
	var mu sync.Mutex
	go func() {
		err := plrA.CollectRawTxns(ctx, func(ctx context.Context, txn model.RawTxn) error {
			mu.Lock()
			defer mu.Unlock()
			c.Assert(ts, check.Less, txn.Ts)
			atomic.StoreUint64(&ts, txn.Ts)
			rawTxns = append(rawTxns, txn)
			return nil
		})
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}()
	pm.Run(context.Background())
	pm.MustExec("create table test.test(id varchar(255) primary key, a int)")
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 1, 1)
	pm.MustExec("update test.test set id = ? where a = ?", 6, 1)
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 2, 2)
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 3, 3)
	pm.MustExec("delete from test.test")
	waitForGrowingTs(&ts, oracle.GoTimeToTS(time.Now()))
	cancel()
	mu.Lock()
	index := len(rawTxns) / 2
	plrB := pm.CreatePuller(rawTxns[index].Ts, []regionspan.Span{regionspan.Span{}.Hack()})
	mu.Unlock()
	ctx, cancel = context.WithCancel(context.Background())
	err := plrB.CollectRawTxns(ctx, func(ctx context.Context, txn model.RawTxn) error {
		mu.Lock()
		defer mu.Unlock()
		if index >= len(rawTxns) {
			cancel()
			return nil
		}
		c.Assert(rawTxns[index], check.DeepEquals, txn)
		index++
		return nil
	})
	c.Assert(errors.Cause(err), check.Equals, context.Canceled)
}

func waitForGrowingTs(growingTs *uint64, targetTs uint64) {
	for {
		growingTsLocal := atomic.LoadUint64(growingTs)
		if growingTsLocal >= targetTs {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
*/
