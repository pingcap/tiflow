package puller

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb-cdc/cdc/txn"
	"github.com/pingcap/tidb-cdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

func Test(t *testing.T) { check.TestingT(t) }

type mockPullerSuite struct{}

var _ = check.Suite(&mockPullerSuite{})

// TODO add a test kit easier-to-use
func (s *mockPullerSuite) TestTxnSort(c *check.C) {
	pm := NewMockPullerManager(c)
	plr := pm.CreatePuller([]util.Span{util.Span{}.Hack()})
	ctx := context.Background()
	ts := uint64(0)
	var wg sync.WaitGroup
	finishTs := uint64(math.MaxUint64)
	wg.Add(1)
	err := plr.CollectRawTxns(ctx, func(ctx context.Context, txn txn.RawTxn) error {
		if finishTs < txn.Ts {
			wg.Done()
		}
		c.Assert(ts, check.LessEqual, txn.Ts)
		ts = txn.Ts
		return nil
	})
	c.Assert(err, check.IsNil)
	err = pm.Run(context.Background())
	c.Assert(err, check.IsNil)
	pm.MustExec("create table test.test(id varchar(255) primary key, a int)")
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 1, 1)
	pm.MustExec("update test.test set id = ? where a = ?", 6, 1)
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 2, 2)
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 3, 3)
	pm.MustExec("delete from test.test")
	finishTs = oracle.EncodeTSO(time.Now().Unix() * 1000)
	wg.Wait()
	pm.Close()
}

func (s *mockPullerSuite) TestDDLPuller(c *check.C) {
	pm := NewMockPullerManager(c)
	plr := pm.CreatePuller([]util.Span{util.GetDDLSpan()})
	ctx := context.Background()
	ts := uint64(0)
	var wg sync.WaitGroup
	finishTs := uint64(math.MaxUint64)
	wg.Add(1)
	txnMounter := txn.NewTxnMounter(nil, time.UTC)
	err := plr.CollectRawTxns(ctx, func(ctx context.Context, rawTxn txn.RawTxn) error {
		if finishTs < rawTxn.Ts {
			wg.Done()
		}
		c.Assert(ts, check.LessEqual, rawTxn.Ts)
		ts = rawTxn.Ts
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
	err = pm.Run(context.Background())
	c.Assert(err, check.IsNil)
	pm.MustExec("create table test.test(id varchar(255) primary key, a int)")
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 1, 1)
	pm.MustExec("update test.test set id = ? where a = ?", 6, 1)
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 2, 2)
	pm.MustExec("insert into test.test(id, a) values(?, ?)", 3, 3)
	pm.MustExec("delete from test.test")
	finishTs = oracle.EncodeTSO(time.Now().Unix() * 1000)
	wg.Wait()
	pm.Close()
}
