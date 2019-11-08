package puller

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-cdc/cdc/txn"
	"github.com/pingcap/tidb-cdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
)

func Test(t *testing.T) { check.TestingT(t) }

type mockPullerSuite struct{}

var _ = check.Suite(&mockPullerSuite{})

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
		log.Info("i", zap.Reflect("txn", txn))
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
