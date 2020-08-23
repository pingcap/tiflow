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
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/notify"
	"golang.org/x/sync/errgroup"
)

type MySQLSinkSuite struct{}

func Test(t *testing.T) { check.TestingT(t) }

var _ = check.Suite(&MySQLSinkSuite{})

func newMySQLSink4Test(c *check.C) *mysqlSink {
	f, err := filter.NewFilter(config.GetDefaultReplicaConfig())
	c.Assert(err, check.IsNil)
	params := defaultParams
	params.batchReplaceEnabled = false
	return &mysqlSink{
		txnCache:   common.NewUnresolvedTxnCache(),
		filter:     f,
		statistics: NewStatistics(context.TODO(), "test", make(map[string]string)),
		params:     params,
	}
}

func (s MySQLSinkSuite) TestEmitRowChangedEvents(c *check.C) {
	testCases := []struct {
		input    []*model.RowChangedEvent
		expected map[model.TableID][]*model.SingleTableTxn
	}{{
		input:    []*model.RowChangedEvent{},
		expected: map[model.TableID][]*model.SingleTableTxn{},
	}, {
		input: []*model.RowChangedEvent{
			{
				StartTs:  1,
				CommitTs: 2,
				Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			},
			{
				StartTs:  1,
				CommitTs: 2,
				Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			},
			{
				StartTs:  1,
				CommitTs: 2,
				Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			},
			{
				StartTs:  3,
				CommitTs: 4,
				Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			},
			{
				StartTs:  3,
				CommitTs: 4,
				Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			},
			{
				StartTs:  3,
				CommitTs: 4,
				Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			},
		},
		expected: map[model.TableID][]*model.SingleTableTxn{
			1: {
				{
					Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
					StartTs:  1,
					CommitTs: 2,
					Rows: []*model.RowChangedEvent{
						{
							StartTs:  1,
							CommitTs: 2,
							Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
						},
						{
							StartTs:  1,
							CommitTs: 2,
							Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
						},
						{
							StartTs:  1,
							CommitTs: 2,
							Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
						}},
				},
				{
					Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
					StartTs:  3,
					CommitTs: 4,
					Rows: []*model.RowChangedEvent{
						{
							StartTs:  3,
							CommitTs: 4,
							Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
						},
						{
							StartTs:  3,
							CommitTs: 4,
							Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
						},
						{
							StartTs:  3,
							CommitTs: 4,
							Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
						}},
				},
			},
		},
	}, {
		input: []*model.RowChangedEvent{
			{
				StartTs:  1,
				CommitTs: 2,
				Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			},
			{
				StartTs:  3,
				CommitTs: 4,
				Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			},
			{
				StartTs:  5,
				CommitTs: 6,
				Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			},
			{
				StartTs:  1,
				CommitTs: 2,
				Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
			},
			{
				StartTs:  3,
				CommitTs: 4,
				Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
			},
			{
				StartTs:  5,
				CommitTs: 6,
				Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
			},
		},
		expected: map[model.TableID][]*model.SingleTableTxn{
			1: {
				{
					Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
					StartTs:  1,
					CommitTs: 2,
					Rows: []*model.RowChangedEvent{
						{
							StartTs:  1,
							CommitTs: 2,
							Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
						}},
				},
				{
					Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
					StartTs:  3,
					CommitTs: 4,
					Rows: []*model.RowChangedEvent{
						{
							StartTs:  3,
							CommitTs: 4,
							Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
						}},
				},
				{
					Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
					StartTs:  5,
					CommitTs: 6,
					Rows: []*model.RowChangedEvent{
						{
							StartTs:  5,
							CommitTs: 6,
							Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
						}},
				},
			},
			2: {
				{
					Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
					StartTs:  1,
					CommitTs: 2,
					Rows: []*model.RowChangedEvent{
						{
							StartTs:  1,
							CommitTs: 2,
							Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
						}},
				},
				{
					Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
					StartTs:  3,
					CommitTs: 4,
					Rows: []*model.RowChangedEvent{
						{
							StartTs:  3,
							CommitTs: 4,
							Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
						}},
				},
				{
					Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
					StartTs:  5,
					CommitTs: 6,
					Rows: []*model.RowChangedEvent{
						{
							StartTs:  5,
							CommitTs: 6,
							Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
						}},
				},
			},
		},
	}}
	ctx := context.Background()

	for _, tc := range testCases {
		ms := newMySQLSink4Test(c)
		err := ms.EmitRowChangedEvents(ctx, tc.input...)
		c.Assert(err, check.IsNil)
		c.Assert(ms.txnCache.Unresolved(), check.DeepEquals, tc.expected)
	}
}

func (s MySQLSinkSuite) TestMysqlSinkWorker(c *check.C) {
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
		}}
	ctx := context.Background()

	notifier := new(notify.Notifier)
	for i, tc := range testCases {
		cctx, cancel := context.WithCancel(ctx)
		var outputRows [][]*model.RowChangedEvent
		var outputReplicaIDs []uint64
		w := newMySQLSinkWorker(tc.maxTxnRow, 1,
			bucketSizeCounter.WithLabelValues("capture", "changefeed", "1"),
			notifier.NewReceiver(-1),
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
		// ensure all txns are fetched from txn channel in sink worker
		time.Sleep(time.Millisecond * 100)
		notifier.Notify()
		w.waitAllTxnsExecuted()
		cancel()
		c.Assert(errg.Wait(), check.IsNil)
		c.Assert(outputRows, check.DeepEquals, tc.expectedOutputRows,
			check.Commentf("case %v, %s, %s", i, spew.Sdump(outputRows), spew.Sdump(tc.expectedOutputRows)))
		c.Assert(outputReplicaIDs, check.DeepEquals, tc.exportedOutputReplicaIDs,
			check.Commentf("case %v, %s, %s", i, spew.Sdump(outputReplicaIDs), spew.Sdump(tc.exportedOutputReplicaIDs)))
	}
}

func (s MySQLSinkSuite) TestPrepareDML(c *check.C) {
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
	ms := newMySQLSink4Test(c)
	for i, tc := range testCases {
		dmls := ms.prepareDMLs(tc.input, 0, 0)
		c.Assert(dmls, check.DeepEquals, tc.expected, check.Commentf("%d", i))
	}
}

func (s MySQLSinkSuite) TestMapReplace(c *check.C) {
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
			query, args := prepareReplace(tc.quoteTable, tc.cols, false)
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
