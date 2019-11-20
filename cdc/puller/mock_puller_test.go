package puller

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

type mockPullerSuite struct{}

var _ = check.Suite(&mockPullerSuite{})

// TODO add a test kit easier-to-use
func (s *mockPullerSuite) TestTxnSort(c *check.C) {
	pm := NewMockPullerManager(c)
	defer pm.Close()
	plr := pm.CreatePuller([]util.Span{util.Span{}.Hack()})
	ctx := context.Background()
	ts := uint64(0)
	err := plr.CollectRawTxns(ctx, func(ctx context.Context, txn model.RawTxn) error {
		c.Assert(ts, check.Less, txn.Ts)
		atomic.StoreUint64(&ts, txn.Ts)
		return nil
	})
	c.Assert(err, check.IsNil)
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
	pm := NewMockPullerManager(c)
	defer pm.Close()
	plr := pm.CreatePuller([]util.Span{util.GetDDLSpan()})
	ctx := context.Background()
	ts := uint64(0)
	txnMounter := entry.NewTxnMounter(nil, time.UTC)
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
	pm.Run(context.Background())
	pm.MustExec("create table test.test(id varchar(255) primary key, a int)")
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 1, 1)
	pm.MustExec("update test.test set id = ? where a = ?", 6, 1)
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 2, 2)
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 3, 3)
	pm.MustExec("delete from test.test")
	waitForGrowingTs(&ts, oracle.EncodeTSO(time.Now().Unix()*1000))
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
