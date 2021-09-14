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

package sink

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/davecgh/go-spew/spew"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/notify"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/pingcap/tidb/infoschema"
	"golang.org/x/sync/errgroup"
)

type MySQLSinkSuite struct{}

func Test(t *testing.T) { check.TestingT(t) }

var _ = check.Suite(&MySQLSinkSuite{})

func newMySQLSink4Test(ctx context.Context, c *check.C) *mysqlSink {
	f, err := filter.NewFilter(config.GetDefaultReplicaConfig())
	c.Assert(err, check.IsNil)
	params := defaultParams.Clone()
	params.batchReplaceEnabled = false
	return &mysqlSink{
		txnCache:   common.NewUnresolvedTxnCache(),
		filter:     f,
		statistics: NewStatistics(ctx, "test", make(map[string]string)),
		params:     params,
	}
}

func (s MySQLSinkSuite) TestMysqlSinkWorker(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		txns                     []*model.SingleTableTxn
		expectedOutputRows       [][]*model.RowChangedEvent
		exportedOutputReplicaIDs []uint64
		maxTxnRow                int
	}{
		{
			txns:      []*model.SingleTableTxn{},
			maxTxnRow: 4,
		}, {
			txns: []*model.SingleTableTxn{
				{
					CommitTs:  1,
					Rows:      []*model.RowChangedEvent{{CommitTs: 1}},
					ReplicaID: 1,
				},
			},
			expectedOutputRows:       [][]*model.RowChangedEvent{{{CommitTs: 1}}},
			exportedOutputReplicaIDs: []uint64{1},
			maxTxnRow:                2,
		}, {
			txns: []*model.SingleTableTxn{
				{
					CommitTs:  1,
					Rows:      []*model.RowChangedEvent{{CommitTs: 1}, {CommitTs: 1}, {CommitTs: 1}},
					ReplicaID: 1,
				},
			},
			expectedOutputRows: [][]*model.RowChangedEvent{
				{{CommitTs: 1}, {CommitTs: 1}, {CommitTs: 1}},
			},
			exportedOutputReplicaIDs: []uint64{1},
			maxTxnRow:                2,
		}, {
			txns: []*model.SingleTableTxn{
				{
					CommitTs:  1,
					Rows:      []*model.RowChangedEvent{{CommitTs: 1}, {CommitTs: 1}},
					ReplicaID: 1,
				},
				{
					CommitTs:  2,
					Rows:      []*model.RowChangedEvent{{CommitTs: 2}},
					ReplicaID: 1,
				},
				{
					CommitTs:  3,
					Rows:      []*model.RowChangedEvent{{CommitTs: 3}, {CommitTs: 3}},
					ReplicaID: 1,
				},
			},
			expectedOutputRows: [][]*model.RowChangedEvent{
				{{CommitTs: 1}, {CommitTs: 1}, {CommitTs: 2}},
				{{CommitTs: 3}, {CommitTs: 3}},
			},
			exportedOutputReplicaIDs: []uint64{1, 1},
			maxTxnRow:                4,
		}, {
			txns: []*model.SingleTableTxn{
				{
					CommitTs:  1,
					Rows:      []*model.RowChangedEvent{{CommitTs: 1}},
					ReplicaID: 1,
				},
				{
					CommitTs:  2,
					Rows:      []*model.RowChangedEvent{{CommitTs: 2}},
					ReplicaID: 2,
				},
				{
					CommitTs:  3,
					Rows:      []*model.RowChangedEvent{{CommitTs: 3}},
					ReplicaID: 3,
				},
			},
			expectedOutputRows: [][]*model.RowChangedEvent{
				{{CommitTs: 1}},
				{{CommitTs: 2}},
				{{CommitTs: 3}},
			},
			exportedOutputReplicaIDs: []uint64{1, 2, 3},
			maxTxnRow:                4,
		}, {
			txns: []*model.SingleTableTxn{
				{
					CommitTs:  1,
					Rows:      []*model.RowChangedEvent{{CommitTs: 1}},
					ReplicaID: 1,
				},
				{
					CommitTs:  2,
					Rows:      []*model.RowChangedEvent{{CommitTs: 2}, {CommitTs: 2}, {CommitTs: 2}},
					ReplicaID: 1,
				},
				{
					CommitTs:  3,
					Rows:      []*model.RowChangedEvent{{CommitTs: 3}},
					ReplicaID: 1,
				},
				{
					CommitTs:  4,
					Rows:      []*model.RowChangedEvent{{CommitTs: 4}},
					ReplicaID: 1,
				},
			},
			expectedOutputRows: [][]*model.RowChangedEvent{
				{{CommitTs: 1}},
				{{CommitTs: 2}, {CommitTs: 2}, {CommitTs: 2}},
				{{CommitTs: 3}, {CommitTs: 4}},
			},
			exportedOutputReplicaIDs: []uint64{1, 1, 1},
			maxTxnRow:                2,
		},
	}
	ctx := context.Background()

	notifier := new(notify.Notifier)
	for i, tc := range testCases {
		cctx, cancel := context.WithCancel(ctx)
		var outputRows [][]*model.RowChangedEvent
		var outputReplicaIDs []uint64
		receiver, err := notifier.NewReceiver(-1)
		c.Assert(err, check.IsNil)
		w := newMySQLSinkWorker(tc.maxTxnRow, 1,
			bucketSizeCounter.WithLabelValues("capture", "changefeed", "1"),
			receiver,
			func(ctx context.Context, events []*model.RowChangedEvent, replicaID uint64, bucket int) error {
				outputRows = append(outputRows, events)
				outputReplicaIDs = append(outputReplicaIDs, replicaID)
				return nil
			})
		errg, cctx := errgroup.WithContext(cctx)
		errg.Go(func() error {
			return w.run(cctx)
		})
		for _, txn := range tc.txns {
			w.appendTxn(cctx, txn)
		}
		var wg sync.WaitGroup
		w.appendFinishTxn(&wg)
		// ensure all txns are fetched from txn channel in sink worker
		time.Sleep(time.Millisecond * 100)
		notifier.Notify()
		wg.Wait()
		cancel()
		c.Assert(errors.Cause(errg.Wait()), check.Equals, context.Canceled)
		c.Assert(outputRows, check.DeepEquals, tc.expectedOutputRows,
			check.Commentf("case %v, %s, %s", i, spew.Sdump(outputRows), spew.Sdump(tc.expectedOutputRows)))
		c.Assert(outputReplicaIDs, check.DeepEquals, tc.exportedOutputReplicaIDs,
			check.Commentf("case %v, %s, %s", i, spew.Sdump(outputReplicaIDs), spew.Sdump(tc.exportedOutputReplicaIDs)))
	}
}

func (s MySQLSinkSuite) TestMySQLSinkWorkerExitWithError(c *check.C) {
	defer testleak.AfterTest(c)()
	txns1 := []*model.SingleTableTxn{
		{
			CommitTs: 1,
			Rows:     []*model.RowChangedEvent{{CommitTs: 1}},
		},
		{
			CommitTs: 2,
			Rows:     []*model.RowChangedEvent{{CommitTs: 2}},
		},
		{
			CommitTs: 3,
			Rows:     []*model.RowChangedEvent{{CommitTs: 3}},
		},
		{
			CommitTs: 4,
			Rows:     []*model.RowChangedEvent{{CommitTs: 4}},
		},
	}
	txns2 := []*model.SingleTableTxn{
		{
			CommitTs: 5,
			Rows:     []*model.RowChangedEvent{{CommitTs: 5}},
		},
		{
			CommitTs: 6,
			Rows:     []*model.RowChangedEvent{{CommitTs: 6}},
		},
	}
	maxTxnRow := 1
	ctx := context.Background()

	errExecFailed := errors.New("sink worker exec failed")
	notifier := new(notify.Notifier)
	cctx, cancel := context.WithCancel(ctx)
	receiver, err := notifier.NewReceiver(-1)
	c.Assert(err, check.IsNil)
	w := newMySQLSinkWorker(maxTxnRow, 1, /*bucket*/
		bucketSizeCounter.WithLabelValues("capture", "changefeed", "1"),
		receiver,
		func(ctx context.Context, events []*model.RowChangedEvent, replicaID uint64, bucket int) error {
			return errExecFailed
		})
	errg, cctx := errgroup.WithContext(cctx)
	errg.Go(func() error {
		return w.run(cctx)
	})
	// txn in txns1 will be sent to worker txnCh
	for _, txn := range txns1 {
		w.appendTxn(cctx, txn)
	}

	// simulate notify sink worker to flush existing txns
	var wg sync.WaitGroup
	w.appendFinishTxn(&wg)
	time.Sleep(time.Millisecond * 100)
	// txn in txn2 will be blocked since the worker has exited
	for _, txn := range txns2 {
		w.appendTxn(cctx, txn)
	}
	notifier.Notify()

	// simulate sink shutdown and send closed singal to sink worker
	w.closedCh <- struct{}{}
	w.cleanup()

	// the flush notification wait group should be done
	wg.Wait()

	cancel()
	c.Assert(errg.Wait(), check.Equals, errExecFailed)
}

func (s MySQLSinkSuite) TestMySQLSinkWorkerExitCleanup(c *check.C) {
	defer testleak.AfterTest(c)()
	txns1 := []*model.SingleTableTxn{
		{
			CommitTs: 1,
			Rows:     []*model.RowChangedEvent{{CommitTs: 1}},
		},
		{
			CommitTs: 2,
			Rows:     []*model.RowChangedEvent{{CommitTs: 2}},
		},
	}
	txns2 := []*model.SingleTableTxn{
		{
			CommitTs: 5,
			Rows:     []*model.RowChangedEvent{{CommitTs: 5}},
		},
	}

	maxTxnRow := 1
	ctx := context.Background()

	errExecFailed := errors.New("sink worker exec failed")
	notifier := new(notify.Notifier)
	cctx, cancel := context.WithCancel(ctx)
	receiver, err := notifier.NewReceiver(-1)
	c.Assert(err, check.IsNil)
	w := newMySQLSinkWorker(maxTxnRow, 1, /*bucket*/
		bucketSizeCounter.WithLabelValues("capture", "changefeed", "1"),
		receiver,
		func(ctx context.Context, events []*model.RowChangedEvent, replicaID uint64, bucket int) error {
			return errExecFailed
		})
	errg, cctx := errgroup.WithContext(cctx)
	errg.Go(func() error {
		err := w.run(cctx)
		return err
	})
	for _, txn := range txns1 {
		w.appendTxn(cctx, txn)
	}

	// sleep to let txns flushed by tick
	time.Sleep(time.Millisecond * 100)

	// simulate more txns are sent to txnCh after the sink worker run has exited
	for _, txn := range txns2 {
		w.appendTxn(cctx, txn)
	}
	var wg sync.WaitGroup
	w.appendFinishTxn(&wg)
	notifier.Notify()

	// simulate sink shutdown and send closed singal to sink worker
	w.closedCh <- struct{}{}
	w.cleanup()

	// the flush notification wait group should be done
	wg.Wait()

	cancel()
	c.Assert(errg.Wait(), check.Equals, errExecFailed)
}

func (s MySQLSinkSuite) TestPrepareDML(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		input    []*model.RowChangedEvent
		expected *preparedDMLs
	}{{
		input:    []*model.RowChangedEvent{},
		expected: &preparedDMLs{sqls: []string{}, values: [][]interface{}{}},
	}, {
		input: []*model.RowChangedEvent{
			{
				StartTs:  418658114257813514,
				CommitTs: 418658114257813515,
				Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
				PreColumns: []*model.Column{nil, {
					Name:  "a1",
					Type:  mysql.TypeLong,
					Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 1,
				}, {
					Name:  "a3",
					Type:  mysql.TypeLong,
					Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 1,
				}},
				IndexColumns: [][]int{{1, 2}},
			},
		},
		expected: &preparedDMLs{
			sqls:     []string{"DELETE FROM `common_1`.`uk_without_pk` WHERE `a1` = ? AND `a3` = ? LIMIT 1;"},
			values:   [][]interface{}{{1, 1}},
			rowCount: 1,
		},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms := newMySQLSink4Test(ctx, c)
	for i, tc := range testCases {
		dmls := ms.prepareDMLs(tc.input, 0, 0)
		c.Assert(dmls, check.DeepEquals, tc.expected, check.Commentf("%d", i))
	}
}

func (s MySQLSinkSuite) TestPrepareUpdate(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		quoteTable   string
		preCols      []*model.Column
		cols         []*model.Column
		expectedSQL  string
		expectedArgs []interface{}
	}{
		{
			quoteTable:   "`test`.`t1`",
			preCols:      []*model.Column{},
			cols:         []*model.Column{},
			expectedSQL:  "",
			expectedArgs: nil,
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test2"},
			},
			expectedSQL:  "UPDATE `test`.`t1` SET `a`=?,`b`=? WHERE `a`=? LIMIT 1;",
			expectedArgs: []interface{}{1, "test2", 1},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarString, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: "test"},
				{Name: "c", Type: mysql.TypeLong, Flag: model.GeneratedColumnFlag, Value: 100},
			},
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: 2},
				{Name: "b", Type: mysql.TypeVarString, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: "test2"},
				{Name: "c", Type: mysql.TypeLong, Flag: model.GeneratedColumnFlag, Value: 100},
			},
			expectedSQL:  "UPDATE `test`.`t1` SET `a`=?,`b`=? WHERE `a`=? AND `b`=? LIMIT 1;",
			expectedArgs: []interface{}{2, "test2", 1, "test"},
		},
	}
	for _, tc := range testCases {
		query, args := prepareUpdate(tc.quoteTable, tc.preCols, tc.cols, false)
		c.Assert(query, check.Equals, tc.expectedSQL)
		c.Assert(args, check.DeepEquals, tc.expectedArgs)
	}
}

func (s MySQLSinkSuite) TestPrepareDelete(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		quoteTable   string
		preCols      []*model.Column
		expectedSQL  string
		expectedArgs []interface{}
	}{
		{
			quoteTable:   "`test`.`t1`",
			preCols:      []*model.Column{},
			expectedSQL:  "",
			expectedArgs: nil,
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? LIMIT 1;",
			expectedArgs: []interface{}{1},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarString, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: "test"},
				{Name: "c", Type: mysql.TypeLong, Flag: model.GeneratedColumnFlag, Value: 100},
			},
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? AND `b` = ? LIMIT 1;",
			expectedArgs: []interface{}{1, "test"},
		},
	}
	for _, tc := range testCases {
		query, args := prepareDelete(tc.quoteTable, tc.preCols, false)
		c.Assert(query, check.Equals, tc.expectedSQL)
		c.Assert(args, check.DeepEquals, tc.expectedArgs)
	}
}

func (s MySQLSinkSuite) TestWhereSlice(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		cols             []*model.Column
		forceReplicate   bool
		expectedColNames []string
		expectedArgs     []interface{}
	}{
		{
			cols:             []*model.Column{},
			forceReplicate:   false,
			expectedColNames: nil,
			expectedArgs:     nil,
		},
		{
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
			forceReplicate:   false,
			expectedColNames: []string{"a"},
			expectedArgs:     []interface{}{1},
		},
		{
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarString, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: "test"},
				{Name: "c", Type: mysql.TypeLong, Flag: model.GeneratedColumnFlag, Value: 100},
			},
			forceReplicate:   false,
			expectedColNames: []string{"a", "b"},
			expectedArgs:     []interface{}{1, "test"},
		},
		{
			cols:             []*model.Column{},
			forceReplicate:   true,
			expectedColNames: []string{},
			expectedArgs:     []interface{}{},
		},
		{
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
			forceReplicate:   true,
			expectedColNames: []string{"a"},
			expectedArgs:     []interface{}{1},
		},
		{
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarString, Flag: model.MultipleKeyFlag | model.HandleKeyFlag, Value: "test"},
				{Name: "c", Type: mysql.TypeLong, Flag: model.GeneratedColumnFlag, Value: 100},
			},
			forceReplicate:   true,
			expectedColNames: []string{"a", "b"},
			expectedArgs:     []interface{}{1, "test"},
		},
		{
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.UniqueKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
			forceReplicate:   true,
			expectedColNames: []string{"a", "b"},
			expectedArgs:     []interface{}{1, "test"},
		},
		{
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.MultipleKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarString, Flag: model.MultipleKeyFlag, Value: "test"},
				{Name: "c", Type: mysql.TypeLong, Flag: model.GeneratedColumnFlag, Value: 100},
			},
			forceReplicate:   true,
			expectedColNames: []string{"a", "b", "c"},
			expectedArgs:     []interface{}{1, "test", 100},
		},
	}
	for _, tc := range testCases {
		colNames, args := whereSlice(tc.cols, tc.forceReplicate)
		c.Assert(colNames, check.DeepEquals, tc.expectedColNames)
		c.Assert(args, check.DeepEquals, tc.expectedArgs)
	}
}

func (s MySQLSinkSuite) TestMapReplace(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		quoteTable    string
		cols          []*model.Column
		expectedQuery string
		expectedArgs  []interface{}
	}{
		{
			quoteTable: "`test`.`t1`",
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Value: "varchar"},
				{Name: "c", Type: mysql.TypeLong, Value: 1, Flag: model.GeneratedColumnFlag},
				{Name: "d", Type: mysql.TypeTiny, Value: uint8(255)},
			},
			expectedQuery: "REPLACE INTO `test`.`t1`(`a`,`b`,`d`) VALUES ",
			expectedArgs:  []interface{}{1, "varchar", uint8(255)},
		},
		{
			quoteTable: "`test`.`t1`",
			cols: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Value: "varchar"},
				{Name: "c", Type: mysql.TypeLong, Value: 1},
				{Name: "d", Type: mysql.TypeTiny, Value: uint8(255)},
			},
			expectedQuery: "REPLACE INTO `test`.`t1`(`a`,`b`,`c`,`d`) VALUES ",
			expectedArgs:  []interface{}{1, "varchar", 1, uint8(255)},
		},
	}
	for _, tc := range testCases {
		// multiple times to verify the stability of column sequence in query string
		for i := 0; i < 10; i++ {
			query, args := prepareReplace(tc.quoteTable, tc.cols, false, false)
			c.Assert(query, check.Equals, tc.expectedQuery)
			c.Assert(args, check.DeepEquals, tc.expectedArgs)
		}
	}
}

type sqlArgs [][]interface{}

func (a sqlArgs) Len() int           { return len(a) }
func (a sqlArgs) Less(i, j int) bool { return fmt.Sprintf("%s", a[i]) < fmt.Sprintf("%s", a[j]) }
func (a sqlArgs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (s MySQLSinkSuite) TestReduceReplace(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		replaces   map[string][][]interface{}
		batchSize  int
		sort       bool
		expectSQLs []string
		expectArgs [][]interface{}
	}{
		{
			replaces: map[string][][]interface{}{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES ": {
					[]interface{}{1, "1"},
					[]interface{}{2, "2"},
					[]interface{}{3, "3"},
				},
			},
			batchSize: 1,
			sort:      false,
			expectSQLs: []string{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?)",
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?)",
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?)",
			},
			expectArgs: [][]interface{}{
				{1, "1"},
				{2, "2"},
				{3, "3"},
			},
		},
		{
			replaces: map[string][][]interface{}{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES ": {
					[]interface{}{1, "1"},
					[]interface{}{2, "2"},
					[]interface{}{3, "3"},
					[]interface{}{4, "3"},
					[]interface{}{5, "5"},
				},
			},
			batchSize: 3,
			sort:      false,
			expectSQLs: []string{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?),(?,?),(?,?)",
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?),(?,?)",
			},
			expectArgs: [][]interface{}{
				{1, "1", 2, "2", 3, "3"},
				{4, "3", 5, "5"},
			},
		},
		{
			replaces: map[string][][]interface{}{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES ": {
					[]interface{}{1, "1"},
					[]interface{}{2, "2"},
					[]interface{}{3, "3"},
					[]interface{}{4, "3"},
					[]interface{}{5, "5"},
				},
			},
			batchSize: 10,
			sort:      false,
			expectSQLs: []string{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?),(?,?),(?,?),(?,?),(?,?)",
			},
			expectArgs: [][]interface{}{
				{1, "1", 2, "2", 3, "3", 4, "3", 5, "5"},
			},
		},
		{
			replaces: map[string][][]interface{}{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES ": {
					[]interface{}{1, "1"},
					[]interface{}{2, "2"},
					[]interface{}{3, "3"},
					[]interface{}{4, "3"},
					[]interface{}{5, "5"},
					[]interface{}{6, "6"},
				},
				"REPLACE INTO `test`.`t2`(`a`,`b`) VALUES ": {
					[]interface{}{7, ""},
					[]interface{}{8, ""},
					[]interface{}{9, ""},
				},
			},
			batchSize: 3,
			sort:      true,
			expectSQLs: []string{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?),(?,?),(?,?)",
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?),(?,?),(?,?)",
				"REPLACE INTO `test`.`t2`(`a`,`b`) VALUES (?,?),(?,?),(?,?)",
			},
			expectArgs: [][]interface{}{
				{1, "1", 2, "2", 3, "3"},
				{4, "3", 5, "5", 6, "6"},
				{7, "", 8, "", 9, ""},
			},
		},
	}
	for _, tc := range testCases {
		sqls, args := reduceReplace(tc.replaces, tc.batchSize)
		if tc.sort {
			sort.Strings(sqls)
			sort.Sort(sqlArgs(args))
		}
		c.Assert(sqls, check.DeepEquals, tc.expectSQLs)
		c.Assert(args, check.DeepEquals, tc.expectArgs)
	}
}

func (s MySQLSinkSuite) TestSinkParamsClone(c *check.C) {
	defer testleak.AfterTest(c)()
	param1 := defaultParams.Clone()
	param2 := param1.Clone()
	param2.changefeedID = "123"
	param2.batchReplaceEnabled = false
	param2.maxTxnRow = 1
	c.Assert(param1, check.DeepEquals, &sinkParams{
		workerCount:         defaultWorkerCount,
		maxTxnRow:           defaultMaxTxnRow,
		tidbTxnMode:         defaultTiDBTxnMode,
		batchReplaceEnabled: defaultBatchReplaceEnabled,
		batchReplaceSize:    defaultBatchReplaceSize,
		readTimeout:         defaultReadTimeout,
		writeTimeout:        defaultWriteTimeout,
		dialTimeout:         defaultDialTimeout,
		safeMode:            defaultSafeMode,
	})
	c.Assert(param2, check.DeepEquals, &sinkParams{
		changefeedID:        "123",
		workerCount:         defaultWorkerCount,
		maxTxnRow:           1,
		tidbTxnMode:         defaultTiDBTxnMode,
		batchReplaceEnabled: false,
		batchReplaceSize:    defaultBatchReplaceSize,
		readTimeout:         defaultReadTimeout,
		writeTimeout:        defaultWriteTimeout,
		dialTimeout:         defaultDialTimeout,
		safeMode:            defaultSafeMode,
	})
}

func (s MySQLSinkSuite) TestConfigureSinkURI(c *check.C) {
	defer testleak.AfterTest(c)()

	testDefaultParams := func() {
		db, err := mockTestDB()
		c.Assert(err, check.IsNil)
		defer db.Close()

		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		c.Assert(err, check.IsNil)
		params := defaultParams.Clone()
		dsnStr, err := configureSinkURI(context.TODO(), dsn, params, db)
		c.Assert(err, check.IsNil)
		expectedParams := []string{
			"tidb_txn_mode=optimistic",
			"readTimeout=2m",
			"writeTimeout=2m",
			"allow_auto_random_explicit_insert=1",
		}
		for _, param := range expectedParams {
			c.Assert(strings.Contains(dsnStr, param), check.IsTrue)
		}
		c.Assert(strings.Contains(dsnStr, "time_zone"), check.IsFalse)
	}

	testTimezoneParam := func() {
		db, err := mockTestDB()
		c.Assert(err, check.IsNil)
		defer db.Close()

		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		c.Assert(err, check.IsNil)
		params := defaultParams.Clone()
		params.timezone = `"UTC"`
		dsnStr, err := configureSinkURI(context.TODO(), dsn, params, db)
		c.Assert(err, check.IsNil)
		c.Assert(strings.Contains(dsnStr, "time_zone=%22UTC%22"), check.IsTrue)
	}

	testTimeoutParams := func() {
		db, err := mockTestDB()
		c.Assert(err, check.IsNil)
		defer db.Close()

		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		c.Assert(err, check.IsNil)
		uri, err := url.Parse("mysql://127.0.0.1:3306/?read-timeout=4m&write-timeout=5m&timeout=3m")
		c.Assert(err, check.IsNil)
		params, err := parseSinkURI(context.TODO(), uri, map[string]string{})
		c.Assert(err, check.IsNil)
		dsnStr, err := configureSinkURI(context.TODO(), dsn, params, db)
		c.Assert(err, check.IsNil)
		expectedParams := []string{
			"readTimeout=4m",
			"writeTimeout=5m",
			"timeout=3m",
		}
		for _, param := range expectedParams {
			c.Assert(strings.Contains(dsnStr, param), check.IsTrue)
		}
	}

	testDefaultParams()
	testTimezoneParam()
	testTimeoutParams()
}

func (s MySQLSinkSuite) TestParseSinkURI(c *check.C) {
	defer testleak.AfterTest(c)()
	expected := defaultParams.Clone()
	expected.workerCount = 64
	expected.maxTxnRow = 20
	expected.batchReplaceEnabled = true
	expected.batchReplaceSize = 50
	expected.safeMode = true
	expected.timezone = `"UTC"`
	expected.changefeedID = "cf-id"
	expected.captureAddr = "127.0.0.1:8300"
	expected.tidbTxnMode = "pessimistic"
	uriStr := "mysql://127.0.0.1:3306/?worker-count=64&max-txn-row=20" +
		"&batch-replace-enable=true&batch-replace-size=50&safe-mode=true" +
		"&tidb-txn-mode=pessimistic"
	opts := map[string]string{
		OptChangefeedID: expected.changefeedID,
		OptCaptureAddr:  expected.captureAddr,
	}
	uri, err := url.Parse(uriStr)
	c.Assert(err, check.IsNil)
	params, err := parseSinkURI(context.TODO(), uri, opts)
	c.Assert(err, check.IsNil)
	c.Assert(params, check.DeepEquals, expected)
}

func (s MySQLSinkSuite) TestParseSinkURITimezone(c *check.C) {
	defer testleak.AfterTest(c)()
	uris := []string{
		"mysql://127.0.0.1:3306/?time-zone=Asia/Shanghai&worker-count=32",
		"mysql://127.0.0.1:3306/?time-zone=&worker-count=32",
		"mysql://127.0.0.1:3306/?worker-count=32",
	}
	expected := []string{
		"\"Asia/Shanghai\"",
		"",
		"\"UTC\"",
	}
	ctx := context.TODO()
	opts := map[string]string{}
	for i, uriStr := range uris {
		uri, err := url.Parse(uriStr)
		c.Assert(err, check.IsNil)
		params, err := parseSinkURI(ctx, uri, opts)
		c.Assert(err, check.IsNil)
		c.Assert(params.timezone, check.Equals, expected[i])
	}
}

func (s MySQLSinkSuite) TestParseSinkURIBadQueryString(c *check.C) {
	defer testleak.AfterTest(c)()
	uris := []string{
		"",
		"postgre://127.0.0.1:3306",
		"mysql://127.0.0.1:3306/?worker-count=not-number",
		"mysql://127.0.0.1:3306/?max-txn-row=not-number",
		"mysql://127.0.0.1:3306/?ssl-ca=only-ca-exists",
		"mysql://127.0.0.1:3306/?batch-replace-enable=not-bool",
		"mysql://127.0.0.1:3306/?batch-replace-enable=true&batch-replace-size=not-number",
		"mysql://127.0.0.1:3306/?safe-mode=not-bool",
	}
	ctx := context.TODO()
	opts := map[string]string{OptChangefeedID: "changefeed-01"}
	var uri *url.URL
	var err error
	for _, uriStr := range uris {
		if uriStr != "" {
			uri, err = url.Parse(uriStr)
			c.Assert(err, check.IsNil)
		} else {
			uri = nil
		}
		_, err = parseSinkURI(ctx, uri, opts)
		c.Assert(err, check.NotNil)
	}
}

func (s MySQLSinkSuite) TestCheckTiDBVariable(c *check.C) {
	defer testleak.AfterTest(c)()
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)
	defer db.Close() //nolint:errcheck
	columns := []string{"Variable_name", "Value"}

	mock.ExpectQuery("show session variables like 'allow_auto_random_explicit_insert';").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("allow_auto_random_explicit_insert", "0"),
	)
	val, err := checkTiDBVariable(context.TODO(), db, "allow_auto_random_explicit_insert", "1")
	c.Assert(err, check.IsNil)
	c.Assert(val, check.Equals, "1")

	mock.ExpectQuery("show session variables like 'no_exist_variable';").WillReturnError(sql.ErrNoRows)
	val, err = checkTiDBVariable(context.TODO(), db, "no_exist_variable", "0")
	c.Assert(err, check.IsNil)
	c.Assert(val, check.Equals, "")

	mock.ExpectQuery("show session variables like 'version';").WillReturnError(sql.ErrConnDone)
	_, err = checkTiDBVariable(context.TODO(), db, "version", "5.7.25-TiDB-v4.0.0")
	c.Assert(err, check.ErrorMatches, ".*"+sql.ErrConnDone.Error())
}

func mockTestDB() (*sql.DB, error) {
	// mock for test db, which is used querying TiDB session variable
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, err
	}
	columns := []string{"Variable_name", "Value"}
	mock.ExpectQuery("show session variables like 'allow_auto_random_explicit_insert';").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("allow_auto_random_explicit_insert", "0"),
	)
	mock.ExpectQuery("show session variables like 'tidb_txn_mode';").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("tidb_txn_mode", "pessimistic"),
	)
	mock.ExpectClose()
	return db, nil
}

func (s MySQLSinkSuite) TestAdjustSQLMode(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			c.Assert(err, check.IsNil)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		c.Assert(err, check.IsNil)
		mock.ExpectQuery("SELECT @@SESSION.sql_mode;").
			WillReturnRows(sqlmock.NewRows([]string{"@@SESSION.sql_mode"}).
				AddRow("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE"))
		mock.ExpectExec("SET sql_mode = 'ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,NO_ZERO_DATE';").
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	c.Assert(err, check.IsNil)
	rc := config.GetDefaultReplicaConfig()
	rc.Cyclic = &config.CyclicConfig{
		Enable:          true,
		ReplicaID:       1,
		FilterReplicaID: []uint64{2},
	}
	f, err := filter.NewFilter(rc)
	c.Assert(err, check.IsNil)
	cyclicConfig, err := rc.Cyclic.Marshal()
	c.Assert(err, check.IsNil)
	opts := map[string]string{
		mark.OptCyclicConfig: cyclicConfig,
	}
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, opts)
	c.Assert(err, check.IsNil)

	err = sink.Close(ctx)
	c.Assert(err, check.IsNil)
}

type mockUnavailableMySQL struct {
	listener net.Listener
	quit     chan interface{}
	wg       sync.WaitGroup
}

func newMockUnavailableMySQL(addr string, c *check.C) *mockUnavailableMySQL {
	s := &mockUnavailableMySQL{
		quit: make(chan interface{}),
	}
	l, err := net.Listen("tcp", addr)
	c.Assert(err, check.IsNil)
	s.listener = l
	s.wg.Add(1)
	go s.serve(c)
	return s
}

func (s *mockUnavailableMySQL) serve(c *check.C) {
	defer s.wg.Done()

	for {
		_, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				c.Error(err)
			}
		} else {
			s.wg.Add(1)
			go func() {
				// don't read from TCP connection, to simulate database service unavailable
				<-s.quit
				s.wg.Done()
			}()
		}
	}
}

func (s *mockUnavailableMySQL) Stop() {
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func (s MySQLSinkSuite) TestNewMySQLTimeout(c *check.C) {
	defer testleak.AfterTest(c)()

	addr := "127.0.0.1:33333"
	mockMySQL := newMockUnavailableMySQL(addr, c)
	defer mockMySQL.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse(fmt.Sprintf("mysql://%s/?read-timeout=2s&timeout=2s", addr))
	c.Assert(err, check.IsNil)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	c.Assert(err, check.IsNil)
	_, err = newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	c.Assert(errors.Cause(err), check.Equals, driver.ErrBadConn)
}

func (s MySQLSinkSuite) TestNewMySQLSinkExecDML(c *check.C) {
	defer testleak.AfterTest(c)()

	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			c.Assert(err, check.IsNil)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		c.Assert(err, check.IsNil)
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t1`(`a`,`b`) VALUES (?,?),(?,?)").
			WithArgs(1, "test", 2, "test").
			WillReturnResult(sqlmock.NewResult(2, 2))
		mock.ExpectCommit()
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t2`(`a`,`b`) VALUES (?,?),(?,?)").
			WithArgs(1, "test", 2, "test").
			WillReturnResult(sqlmock.NewResult(2, 2))
		mock.ExpectCommit()
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	c.Assert(err, check.IsNil)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	c.Assert(err, check.IsNil)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	c.Assert(err, check.IsNil)

	rows := []*model.RowChangedEvent{
		{
			StartTs:  1,
			CommitTs: 2,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
		},
		{
			StartTs:  1,
			CommitTs: 2,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 2},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
		},
		{
			StartTs:  5,
			CommitTs: 6,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 3},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
		},
		{
			StartTs:  3,
			CommitTs: 4,
			Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
		},
		{
			StartTs:  3,
			CommitTs: 4,
			Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 2},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test"},
			},
		},
	}

	err = sink.EmitRowChangedEvents(ctx, rows...)
	c.Assert(err, check.IsNil)

	err = retry.Do(context.Background(), func() error {
		ts, err := sink.FlushRowChangedEvents(ctx, uint64(2))
		c.Assert(err, check.IsNil)
		if ts < uint64(2) {
			return errors.Errorf("checkpoint ts %d less than resolved ts %d", ts, 2)
		}
		return nil
	}, retry.WithBackoffBaseDelay(20), retry.WithMaxTries(10), retry.WithIsRetryableErr(cerror.IsRetryableError))

	c.Assert(err, check.IsNil)

	err = retry.Do(context.Background(), func() error {
		ts, err := sink.FlushRowChangedEvents(ctx, uint64(4))
		c.Assert(err, check.IsNil)
		if ts < uint64(4) {
			return errors.Errorf("checkpoint ts %d less than resolved ts %d", ts, 4)
		}
		return nil
	}, retry.WithBackoffBaseDelay(20), retry.WithMaxTries(10), retry.WithIsRetryableErr(cerror.IsRetryableError))
	c.Assert(err, check.IsNil)

	err = sink.Barrier(ctx)
	c.Assert(err, check.IsNil)

	err = sink.Close(ctx)
	c.Assert(err, check.IsNil)
}

func (s MySQLSinkSuite) TestExecDMLRollbackErrDatabaseNotExists(c *check.C) {
	defer testleak.AfterTest(c)()

	rows := []*model.RowChangedEvent{
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
			},
		},
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 2},
			},
		},
	}

	errDatabaseNotExists := &dmysql.MySQLError{
		Number: uint16(infoschema.ErrDatabaseNotExists.Code()),
	}

	dbIndex := 0
	mockGetDBConnErrDatabaseNotExists := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			c.Assert(err, check.IsNil)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		c.Assert(err, check.IsNil)
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t1`(`a`) VALUES (?),(?)").
			WithArgs(1, 2).
			WillReturnError(errDatabaseNotExists)
		mock.ExpectRollback()
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConnErrDatabaseNotExists
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	c.Assert(err, check.IsNil)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	c.Assert(err, check.IsNil)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	c.Assert(err, check.IsNil)

	err = sink.(*mysqlSink).execDMLs(ctx, rows, 1 /* replicaID */, 1 /* bucket */)
	c.Assert(errors.Cause(err), check.Equals, errDatabaseNotExists)

	err = sink.Close(ctx)
	c.Assert(err, check.IsNil)
}

func (s MySQLSinkSuite) TestExecDMLRollbackErrTableNotExists(c *check.C) {
	defer testleak.AfterTest(c)()

	rows := []*model.RowChangedEvent{
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
			},
		},
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 2},
			},
		},
	}

	errTableNotExists := &dmysql.MySQLError{
		Number: uint16(infoschema.ErrTableNotExists.Code()),
	}

	dbIndex := 0
	mockGetDBConnErrDatabaseNotExists := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			c.Assert(err, check.IsNil)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		c.Assert(err, check.IsNil)
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t1`(`a`) VALUES (?),(?)").
			WithArgs(1, 2).
			WillReturnError(errTableNotExists)
		mock.ExpectRollback()
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConnErrDatabaseNotExists
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	c.Assert(err, check.IsNil)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	c.Assert(err, check.IsNil)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	c.Assert(err, check.IsNil)

	err = sink.(*mysqlSink).execDMLs(ctx, rows, 1 /* replicaID */, 1 /* bucket */)
	c.Assert(errors.Cause(err), check.Equals, errTableNotExists)

	err = sink.Close(ctx)
	c.Assert(err, check.IsNil)
}

func (s MySQLSinkSuite) TestExecDMLRollbackErrRetryable(c *check.C) {
	defer testleak.AfterTest(c)()

	rows := []*model.RowChangedEvent{
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 1},
			},
		},
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{Name: "a", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Value: 2},
			},
		},
	}

	errLockDeadlock := &dmysql.MySQLError{
		Number: mysql.ErrLockDeadlock,
	}

	dbIndex := 0
	mockGetDBConnErrDatabaseNotExists := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			c.Assert(err, check.IsNil)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		c.Assert(err, check.IsNil)
		for i := 0; i < defaultDMLMaxRetryTime; i++ {
			mock.ExpectBegin()
			mock.ExpectExec("REPLACE INTO `s1`.`t1`(`a`) VALUES (?),(?)").
				WithArgs(1, 2).
				WillReturnError(errLockDeadlock)
			mock.ExpectRollback()
		}
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConnErrDatabaseNotExists
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	c.Assert(err, check.IsNil)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	c.Assert(err, check.IsNil)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	c.Assert(err, check.IsNil)

	err = sink.(*mysqlSink).execDMLs(ctx, rows, 1 /* replicaID */, 1 /* bucket */)
	c.Assert(errors.Cause(err), check.Equals, errLockDeadlock)

	err = sink.Close(ctx)
	c.Assert(err, check.IsNil)
}

func (s MySQLSinkSuite) TestNewMySQLSinkExecDDL(c *check.C) {
	defer testleak.AfterTest(c)()

	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			c.Assert(err, check.IsNil)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		c.Assert(err, check.IsNil)
		mock.ExpectBegin()
		mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("ALTER TABLE test.t1 ADD COLUMN a int").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		mock.ExpectBegin()
		mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("ALTER TABLE test.t1 ADD COLUMN a int").
			WillReturnError(&dmysql.MySQLError{
				Number: uint16(infoschema.ErrColumnExists.Code()),
			})
		mock.ExpectRollback()
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	c.Assert(err, check.IsNil)
	rc := config.GetDefaultReplicaConfig()
	rc.Filter = &config.FilterConfig{
		Rules: []string{"test.t1"},
	}
	f, err := filter.NewFilter(rc)
	c.Assert(err, check.IsNil)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	c.Assert(err, check.IsNil)

	ddl1 := &model.DDLEvent{
		StartTs:  1000,
		CommitTs: 1010,
		TableInfo: &model.SimpleTableInfo{
			Schema: "test",
			Table:  "t1",
		},
		Type:  timodel.ActionAddColumn,
		Query: "ALTER TABLE test.t1 ADD COLUMN a int",
	}
	ddl2 := &model.DDLEvent{
		StartTs:  1020,
		CommitTs: 1030,
		TableInfo: &model.SimpleTableInfo{
			Schema: "test",
			Table:  "t2",
		},
		Type:  timodel.ActionAddColumn,
		Query: "ALTER TABLE test.t1 ADD COLUMN a int",
	}

	err = sink.EmitDDLEvent(ctx, ddl1)
	c.Assert(err, check.IsNil)
	err = sink.EmitDDLEvent(ctx, ddl2)
	c.Assert(cerror.ErrDDLEventIgnored.Equal(err), check.IsTrue)
	// DDL execute failed, but error can be ignored
	err = sink.EmitDDLEvent(ctx, ddl1)
	c.Assert(err, check.IsNil)

	err = sink.Close(ctx)
	c.Assert(err, check.IsNil)
}

func (s MySQLSinkSuite) TestNewMySQLSink(c *check.C) {
	defer testleak.AfterTest(c)()

	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB()
			c.Assert(err, check.IsNil)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mock.ExpectClose()
		c.Assert(err, check.IsNil)
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	c.Assert(err, check.IsNil)
	rc := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(rc)
	c.Assert(err, check.IsNil)
	sink, err := newMySQLSink(ctx, changefeed, sinkURI, f, rc, map[string]string{})
	c.Assert(err, check.IsNil)
	err = sink.Close(ctx)
	c.Assert(err, check.IsNil)
}
