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
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/pingcap/check"
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
	return &mysqlSink{
		txnCache:   common.NewUnresolvedTxnCache(),
		filter:     f,
		statistics: NewStatistics(context.TODO(), "test", make(map[string]string)),
	}
}

func (s MySQLSinkSuite) TestEmitRowChangedEvents(c *check.C) {
	testCases := []struct {
		input    []*model.RowChangedEvent
		expected map[model.TableName][]*model.Txn
	}{{
		input:    []*model.RowChangedEvent{},
		expected: map[model.TableName][]*model.Txn{},
	}, {
		input: []*model.RowChangedEvent{
			{
				StartTs:  1,
				CommitTs: 2,
				Table:    &model.TableName{Schema: "s1", Table: "t1"},
				Keys:     []string{"a", "b"},
			},
			{
				StartTs:  1,
				CommitTs: 2,
				Table:    &model.TableName{Schema: "s1", Table: "t1"},
				Keys:     []string{"b", "c"},
			},
			{
				StartTs:  1,
				CommitTs: 2,
				Table:    &model.TableName{Schema: "s1", Table: "t1"},
				Keys:     []string{"a", "b"},
			},
			{
				StartTs:  3,
				CommitTs: 4,
				Table:    &model.TableName{Schema: "s1", Table: "t1"},
			},
			{
				StartTs:  3,
				CommitTs: 4,
				Table:    &model.TableName{Schema: "s1", Table: "t1"},
			},
			{
				StartTs:  3,
				CommitTs: 4,
				Table:    &model.TableName{Schema: "s1", Table: "t1"},
			},
		},
		expected: map[model.TableName][]*model.Txn{
			{Schema: "s1", Table: "t1"}: {
				{
					StartTs:  1,
					CommitTs: 2,
					Rows: []*model.RowChangedEvent{
						{
							StartTs:  1,
							CommitTs: 2,
							Table:    &model.TableName{Schema: "s1", Table: "t1"},
							Keys:     []string{"a", "b"},
						},
						{
							StartTs:  1,
							CommitTs: 2,
							Table:    &model.TableName{Schema: "s1", Table: "t1"},
							Keys:     []string{"b", "c"},
						},
						{
							StartTs:  1,
							CommitTs: 2,
							Table:    &model.TableName{Schema: "s1", Table: "t1"},
							Keys:     []string{"a", "b"},
						}},
					Keys: []string{"a", "b", "b", "c", "a", "b"},
				},
				{
					StartTs:  3,
					CommitTs: 4,
					Rows: []*model.RowChangedEvent{
						{
							StartTs:  3,
							CommitTs: 4,
							Table:    &model.TableName{Schema: "s1", Table: "t1"},
						},
						{
							StartTs:  3,
							CommitTs: 4,
							Table:    &model.TableName{Schema: "s1", Table: "t1"},
						},
						{
							StartTs:  3,
							CommitTs: 4,
							Table:    &model.TableName{Schema: "s1", Table: "t1"},
						}},
					Keys: []string{"`s1`.`t1`"},
				},
			},
		},
	}, {
		input: []*model.RowChangedEvent{
			{
				StartTs:  1,
				CommitTs: 2,
				Table:    &model.TableName{Schema: "s1", Table: "t1"},
			},
			{
				StartTs:  3,
				CommitTs: 4,
				Table:    &model.TableName{Schema: "s1", Table: "t1"},
			},
			{
				StartTs:  5,
				CommitTs: 6,
				Table:    &model.TableName{Schema: "s1", Table: "t1"},
			},
			{
				StartTs:  1,
				CommitTs: 2,
				Table:    &model.TableName{Schema: "s1", Table: "t2"},
			},
			{
				StartTs:  3,
				CommitTs: 4,
				Table:    &model.TableName{Schema: "s1", Table: "t2"},
			},
			{
				StartTs:  5,
				CommitTs: 6,
				Table:    &model.TableName{Schema: "s1", Table: "t2"},
			},
		},
		expected: map[model.TableName][]*model.Txn{
			{Schema: "s1", Table: "t1"}: {
				{
					StartTs:  1,
					CommitTs: 2,
					Rows: []*model.RowChangedEvent{
						{
							StartTs:  1,
							CommitTs: 2,
							Table:    &model.TableName{Schema: "s1", Table: "t1"},
						}},
					Keys: []string{"`s1`.`t1`"},
				},
				{
					StartTs:  3,
					CommitTs: 4,
					Rows: []*model.RowChangedEvent{
						{
							StartTs:  3,
							CommitTs: 4,
							Table:    &model.TableName{Schema: "s1", Table: "t1"},
						}},
					Keys: []string{"`s1`.`t1`"},
				},
				{
					StartTs:  5,
					CommitTs: 6,
					Rows: []*model.RowChangedEvent{
						{
							StartTs:  5,
							CommitTs: 6,
							Table:    &model.TableName{Schema: "s1", Table: "t1"},
						}},
					Keys: []string{"`s1`.`t1`"},
				},
			},
			{Schema: "s1", Table: "t2"}: {
				{
					StartTs:  1,
					CommitTs: 2,
					Rows: []*model.RowChangedEvent{
						{
							StartTs:  1,
							CommitTs: 2,
							Table:    &model.TableName{Schema: "s1", Table: "t2"},
						}},
					Keys: []string{"`s1`.`t2`"},
				},
				{
					StartTs:  3,
					CommitTs: 4,
					Rows: []*model.RowChangedEvent{
						{
							StartTs:  3,
							CommitTs: 4,
							Table:    &model.TableName{Schema: "s1", Table: "t2"},
						}},
					Keys: []string{"`s1`.`t2`"},
				},
				{
					StartTs:  5,
					CommitTs: 6,
					Rows: []*model.RowChangedEvent{
						{
							StartTs:  5,
							CommitTs: 6,
							Table:    &model.TableName{Schema: "s1", Table: "t2"},
						}},
					Keys: []string{"`s1`.`t2`"},
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
		txns                     []*model.Txn
		expectedOutputRows       [][]*model.RowChangedEvent
		exportedOutputReplicaIDs []uint64
		maxTxnRow                int
	}{
		{
			txns:      []*model.Txn{},
			maxTxnRow: 4,
		}, {
			txns: []*model.Txn{
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
			txns: []*model.Txn{
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
			txns: []*model.Txn{
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
			txns: []*model.Txn{
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
			txns: []*model.Txn{
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

/*
   import (
   	"context"
   	"sort"
   	"testing"

   	"github.com/DATA-DOG/go-sqlmock"
   	dmysql "github.com/go-sql-driver/mysql"
   	"github.com/pingcap/check"
   	timodel "github.com/pingcap/parser/model"
   	"github.com/pingcap/parser/mysql"
   	"github.com/pingcap/parser/types"
   	"github.com/pingcap/ticdc/cdc/model"
   	"github.com/pingcap/ticdc/cdc/schema"
   	"github.com/pingcap/tidb/infoschema"
   	dbtypes "github.com/pingcap/tidb/types"
   )

   type EmitSuite struct{}

   func Test(t *testing.T) { check.TestingT(t) }

   var _ = check.Suite(&EmitSuite{})

   func (s EmitSuite) TestShouldExecDDL(c *check.C) {
   	// Set up
   	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
   	c.Assert(err, check.IsNil)
   	defer db.Close()

   	sink := mysqlSink{
   		db: db,
   	}

   	t := model.Txn{
   		DDL: &model.DDL{
   			Database: "test",
   			Table:    "user",
   			Job: &timodel.Job{
   				Query: "CREATE TABLE user (id INT PRIMARY KEY);",
   			},
   		},
   	}

   	mock.ExpectBegin()
   	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
   	mock.ExpectExec(t.DDL.Job.Query).WillReturnResult(sqlmock.NewResult(1, 1))
   	mock.ExpectCommit()

   	// Execute
   	err = sink.EmitDDL(context.Background(), t)

   	// Validate
   	c.Assert(err, check.IsNil)
   	c.Assert(mock.ExpectationsWereMet(), check.IsNil)
   }

   func (s EmitSuite) TestShouldIgnoreCertainDDLError(c *check.C) {
   	// Set up
   	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
   	c.Assert(err, check.IsNil)
   	defer db.Close()

   	sink := mysqlSink{
   		db: db,
   	}

   	t := model.Txn{
   		DDL: &model.DDL{
   			Database: "test",
   			Table:    "user",
   			Job: &timodel.Job{
   				Query: "CREATE TABLE user (id INT PRIMARY KEY);",
   			},
   		},
   	}

   	mock.ExpectBegin()
   	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
   	ignorable := dmysql.MySQLError{
   		Number: uint16(infoschema.ErrTableExists.Code()),
   	}
   	mock.ExpectExec(t.DDL.Job.Query).WillReturnError(&ignorable)

   	// Execute
   	err = sink.EmitDDL(context.Background(), t)

   	// Validate
   	c.Assert(err, check.IsNil)
   	c.Assert(mock.ExpectationsWereMet(), check.IsNil)
   }

   type tableHelper struct {
   }

   func (h *tableHelper) TableByID(id int64) (info *schema.TableInfo, ok bool) {
   	return schema.WrapTableInfo(&timodel.TableInfo{
   		Columns: []*timodel.ColumnInfo{
   			{
   				Name:  timodel.CIStr{O: "id"},
   				State: timodel.StatePublic,
   				FieldType: types.FieldType{
   					Tp:      mysql.TypeLong,
   					Flen:    types.UnspecifiedLength,
   					Decimal: types.UnspecifiedLength,
   				},
   			},
   			{
   				Name:  timodel.CIStr{O: "name"},
   				State: timodel.StatePublic,
   				FieldType: types.FieldType{
   					Tp:      mysql.TypeString,
   					Flen:    types.UnspecifiedLength,
   					Decimal: types.UnspecifiedLength,
   				},
   			},
   		},
   	}), true
   }

   func (h *tableHelper) GetTableByName(schema, table string) (*schema.TableInfo, bool) {
   	return h.TableByID(42)
   }

   func (h *tableHelper) GetTableIDByName(schema, table string) (int64, bool) {
   	return 42, true
   }

   func (s EmitSuite) TestShouldExecReplaceInto(c *check.C) {
   	// Set up
   	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
   	c.Assert(err, check.IsNil)
   	defer db.Close()

   	helper := tableHelper{}
   	sink := mysqlSink{
   		db:         db,
   		infoGetter: &helper,
   	}

   	t := model.Txn{
   		DMLs: []*model.DML{
   			{
   				Database: "test",
   				Table:    "user",
   				Tp:       model.InsertDMLType,
   				Values: map[string]dbtypes.Datum{
   					"id":   dbtypes.NewDatum(42),
   					"name": dbtypes.NewDatum("tester1"),
   				},
   			},
   		},
   	}

   	mock.ExpectBegin()
   	mock.ExpectExec("REPLACE INTO `test`.`user`(`id`,`name`) VALUES (?,?);").
   		WithArgs(42, "tester1").
   		WillReturnResult(sqlmock.NewResult(1, 1))
   	mock.ExpectCommit()

   	// Execute
   	err = sink.EmitDMLs(context.Background(), t)

   	// Validate
   	c.Assert(err, check.IsNil)
   	c.Assert(mock.ExpectationsWereMet(), check.IsNil)
   }

   func (s EmitSuite) TestShouldExecDelete(c *check.C) {
   	// Set up
   	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
   	c.Assert(err, check.IsNil)
   	defer db.Close()

   	helper := tableHelper{}
   	sink := mysqlSink{
   		db:         db,
   		infoGetter: &helper,
   	}

   	t := model.Txn{
   		DMLs: []*model.DML{
   			{
   				Database: "test",
   				Table:    "user",
   				Tp:       model.DeleteDMLType,
   				Values: map[string]dbtypes.Datum{
   					"id":   dbtypes.NewDatum(123),
   					"name": dbtypes.NewDatum("tester1"),
   				},
   			},
   		},
   	}

   	mock.ExpectBegin()
   	mock.ExpectExec("DELETE FROM `test`.`user` WHERE `id` = ? AND `name` = ? LIMIT 1;").
   		WithArgs(123, "tester1").
   		WillReturnResult(sqlmock.NewResult(1, 1))
   	mock.ExpectCommit()

   	// Execute
   	err = sink.EmitDMLs(context.Background(), t)

   	// Validate
   	c.Assert(err, check.IsNil)
   	c.Assert(mock.ExpectationsWereMet(), check.IsNil)
   }

   type splitSuite struct{}

   var _ = check.Suite(&splitSuite{})

   func (s *splitSuite) TestCanHandleEmptyInput(c *check.C) {
   	c.Assert(splitIndependentGroups(nil), check.HasLen, 0)
   }

   func (s *splitSuite) TestShouldSplitByTable(c *check.C) {
   	var dmls []*model.DML
   	addDMLs := func(n int, db, tbl string) {
   		for i := 0; i < n; i++ {
   			dml := model.DML{
   				Database: db,
   				Table:    tbl,
   			}
   			dmls = append(dmls, &dml)
   		}
   	}
   	addDMLs(3, "db", "tbl1")
   	addDMLs(2, "db", "tbl2")
   	addDMLs(2, "db", "tbl1")
   	addDMLs(2, "db2", "tbl2")

   	groups := splitIndependentGroups(dmls)

   	assertAllAreFromTbl := func(dmls []*model.DML, db, tbl string) {
   		for _, dml := range dmls {
   			c.Assert(dml.Database, check.Equals, db)
   			c.Assert(dml.Table, check.Equals, tbl)
   		}
   	}
   	c.Assert(groups, check.HasLen, 3)
   	sort.Slice(groups, func(i, j int) bool {
   		tblI := groups[i][0]
   		tblJ := groups[j][0]
   		if tblI.Database != tblJ.Database {
   			return tblI.Database < tblJ.Database
   		}
   		return tblI.Table < tblJ.Table
   	})
   	assertAllAreFromTbl(groups[0], "db", "tbl1")
   	assertAllAreFromTbl(groups[1], "db", "tbl2")
   	assertAllAreFromTbl(groups[2], "db2", "tbl2")
   }

   type mysqlSinkSuite struct{}

   var _ = check.Suite(&mysqlSinkSuite{})

   func (s *mysqlSinkSuite) TestBuildDBAndParams(c *check.C) {
   	tests := []struct {
   		sinkURI string
   		opts    map[string]string
   		params  params
   	}{
   		{
   			sinkURI: "mysql://root:123@localhost:4000?worker-count=20",
   			opts:    map[string]string{dryRunOpt: ""},
   			params: params{
   				workerCount: 20,
   				dryRun:      true,
   			},
   		},
   		{
   			sinkURI: "tidb://root:123@localhost:4000?worker-count=20",
   			opts:    map[string]string{dryRunOpt: ""},
   			params: params{
   				workerCount: 20,
   				dryRun:      true,
   			},
   		},
   		{
   			sinkURI: "root@tcp(127.0.0.1:3306)/", // dsn not uri
   			opts:    nil,
   			params:  defaultParams,
   		},
   	}

   	for _, t := range tests {
   		c.Log("case sink: ", t.sinkURI)
   		db, params, err := buildDBAndParams(t.sinkURI, t.opts)
   		c.Assert(err, check.IsNil)
   		c.Assert(params, check.Equals, t.params)
   		c.Assert(db, check.NotNil)
   	}
   }

*/
