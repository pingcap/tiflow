package puller

/*
import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

type mockPullerSuite struct{}

var _ = check.Suite(&mockPullerSuite{})

func (s *mockPullerSuite) TestTxnSort(c *check.C) {
	pm := NewMockPullerManager(c, true)
	plr := pm.CreatePuller(0, []util.Span{util.Span{}.Hack()})
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
	waitForGrowingTs(&ts, oracle.EncodeTSO(time.Now().Unix()*1000))
}

func (s *mockPullerSuite) TestDDLPuller(c *check.C) {
	pm := NewMockPullerManager(c, true)
	plr := pm.CreatePuller(0, []util.Span{util.GetDDLSpan()})
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
				c.Assert(util.KeyInSpan(e.Key, util.GetDDLSpan()), check.IsTrue)
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
	waitForGrowingTs(&ts, oracle.EncodeTSO(time.Now().Unix()*1000))
}

func (s *mockPullerSuite) TestStartTs(c *check.C) {
	pm := NewMockPullerManager(c, true)
	plrA := pm.CreatePuller(0, []util.Span{util.Span{}.Hack()})
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
	waitForGrowingTs(&ts, oracle.EncodeTSO(time.Now().Unix()*1000))
	cancel()
	mu.Lock()
	index := len(rawTxns) / 2
	plrB := pm.CreatePuller(rawTxns[index].Ts, []util.Span{util.Span{}.Hack()})
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
