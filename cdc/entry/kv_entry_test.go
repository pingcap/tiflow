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

package entry

/*
import (
	"context"
	"reflect"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/types"
)

type kvEntrySuite struct {
}

var _ = check.Suite(&kvEntrySuite{})

func (s *kvEntrySuite) testCreateTable(c *check.C, newRowFormat bool) {
	ctx, cancel := context.WithCancel(context.Background())
	pm, schema := setUpPullerAndSchema(ctx, c, newRowFormat)
	m := NewTxnMounter(schema)

	pm.MustExec("create table test.test1(id varchar(255) primary key, a int, index i1 (a))")

	plr := pm.CreatePuller(0, []regionspan.Span{regionspan.GetDDLSpan()})
	existDDLJobHistoryKVEntry := false

	// create another context with canceled, we can close this puller but not affect puller manager
	plrCtx, plrCancel := context.WithCancel(ctx)
	err := plr.CollectRawTxns(plrCtx, func(ctx context.Context, rawTxn model.RawTxn) error {
		for _, raw := range rawTxn.Entries {
			entry, err := m.unmarshal(raw)
			c.Assert(err, check.IsNil)
			if e, ok := entry.(*ddlJobKVEntry); ok {
				existDDLJobHistoryKVEntry = true
				c.Assert(e.JobID, check.Equals, e.Job.ID)
				c.Assert(e.Job.SchemaName, check.Equals, "test")
				c.Assert(e.Job.Type, check.Equals, timodel.ActionCreateTable)
				c.Assert(e.Job.Query, check.Equals, "create table test.test1(id varchar(255) primary key, a int, index i1 (a))")

				tableInfo := e.Job.BinlogInfo.TableInfo
				c.Assert(tableInfo.ID, check.Equals, e.Job.TableID)
				c.Assert(tableInfo.Name.O, check.Equals, "test1")
				c.Assert(len(tableInfo.Columns), check.Equals, 2)
				c.Assert(tableInfo.Columns[0].Name.O, check.Equals, "id")
				c.Assert(tableInfo.Columns[1].Name.O, check.Equals, "a")
				c.Assert(tableInfo.Columns[0].Tp, check.Equals, mysql.TypeVarchar)
				c.Assert(tableInfo.Columns[1].Tp, check.Equals, mysql.TypeLong)
				c.Assert(tableInfo.PKIsHandle, check.IsFalse)
				c.Assert(len(tableInfo.Indices), check.Equals, 2)
				// i1 index
				c.Assert(tableInfo.Indices[0].Name.O, check.Equals, "i1")
				c.Assert(tableInfo.Indices[0].Tp, check.Equals, timodel.IndexTypeBtree)
				c.Assert(tableInfo.Indices[0].Unique, check.IsFalse)
				c.Assert(tableInfo.Indices[0].Columns[0].Name.O, check.Equals, "a")
				c.Assert(tableInfo.Indices[0].Columns[0].Offset, check.Equals, 1)
				// primary index
				c.Assert(tableInfo.Indices[1].Name.O, check.Equals, "PRIMARY")
				c.Assert(tableInfo.Indices[1].Tp, check.Equals, timodel.IndexTypeBtree)
				c.Assert(tableInfo.Indices[1].Unique, check.IsTrue)
				c.Assert(tableInfo.Indices[1].Columns[0].Name.O, check.Equals, "id")
				c.Assert(tableInfo.Indices[1].Columns[0].Offset, check.Equals, 0)
				plrCancel()
			}
		}
		return nil
	})
	c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	c.Assert(existDDLJobHistoryKVEntry, check.IsTrue)
	cancel()

	// create another puller, pull KVs from now
	ctx, cancel = context.WithCancel(context.Background())
	pm, schema = setUpPullerAndSchema(ctx, c, newRowFormat)
	m = NewTxnMounter(schema)

	pm.MustExec("create table test.test2(id int primary key, b varchar(255) unique key)")

	plr = pm.CreatePuller(0, []regionspan.Span{regionspan.GetDDLSpan()})
	existDDLJobHistoryKVEntry = false
	plrCtx, plrCancel = context.WithCancel(ctx)
	err = plr.CollectRawTxns(plrCtx, func(ctx context.Context, rawTxn model.RawTxn) error {
		for _, raw := range rawTxn.Entries {
			entry, err := m.unmarshal(raw)
			c.Assert(err, check.IsNil)
			if e, ok := entry.(*ddlJobKVEntry); ok {
				existDDLJobHistoryKVEntry = true
				c.Assert(e.JobID, check.Equals, e.Job.ID)
				c.Assert(e.Job.SchemaName, check.Equals, "test")
				c.Assert(e.Job.Type, check.Equals, timodel.ActionCreateTable)
				c.Assert(e.Job.Query, check.Equals, "create table test.test2(id int primary key, b varchar(255) unique key)")

				tableInfo := e.Job.BinlogInfo.TableInfo
				c.Assert(tableInfo.ID, check.Equals, e.Job.TableID)
				c.Assert(tableInfo.Name.O, check.Equals, "test2")
				c.Assert(len(tableInfo.Columns), check.Equals, 2)
				c.Assert(tableInfo.Columns[0].Name.O, check.Equals, "id")
				c.Assert(tableInfo.Columns[1].Name.O, check.Equals, "b")
				c.Assert(tableInfo.Columns[0].Tp, check.Equals, mysql.TypeLong)
				c.Assert(tableInfo.Columns[1].Tp, check.Equals, mysql.TypeVarchar)
				c.Assert(tableInfo.PKIsHandle, check.IsTrue)
				c.Assert(len(tableInfo.Indices), check.Equals, 1)
				c.Assert(tableInfo.Indices[0].Name.O, check.Equals, "b")
				c.Assert(tableInfo.Indices[0].Tp, check.Equals, timodel.IndexTypeBtree)
				c.Assert(tableInfo.Indices[0].Unique, check.IsTrue)
				c.Assert(tableInfo.Indices[0].Columns[0].Name.O, check.Equals, "b")
				plrCancel()
			}
		}
		return nil
	})
	c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	c.Assert(existDDLJobHistoryKVEntry, check.IsTrue)
	cancel()
}

func (s *kvEntrySuite) testPkIsNotHandleDML(c *check.C, newRowFormat bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm, schema := setUpPullerAndSchema(ctx, c, newRowFormat,
		"create table test.test1(id varchar(255) primary key, a int, index ci (a))",
	)
	m := NewTxnMounter(schema)

	tableInfo := pm.GetTableInfo("test", "test1")
	tableID := tableInfo.ID

	plr := pm.CreatePuller(0, []regionspan.Span{regionspan.GetTableSpan(tableID, false)})

	pm.MustExec("insert into test.test1 values('ttt',666)")
	expect := []kvEntry{
		&rowKVEntry{
			TableID:  tableID,
			RecordID: 1,
			Delete:   false,
			Row:      map[int64]types.Datum{1: types.NewBytesDatum([]byte("ttt")), 2: types.NewIntDatum(666)},
		}, &indexKVEntry{
			TableID:    tableID,
			RecordID:   1,
			IndexID:    1,
			Delete:     false,
			IndexValue: []types.Datum{types.NewIntDatum(666)},
		}, &indexKVEntry{
			TableID:    tableID,
			RecordID:   1,
			IndexID:    2,
			Delete:     false,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("ttt"))},
		}}
	checkDMLKVEntries(ctx, c, tableInfo, m, plr, expect)

	pm.MustExec("update test.test1 set id = '777' where a = 666")
	expect = []kvEntry{
		&rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 1,
			Delete:   false,
			Row:      map[int64]types.Datum{1: types.NewBytesDatum([]byte("777")), 2: types.NewIntDatum(666)},
		}, &indexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   1,
			IndexID:    2,
			Delete:     false,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("777"))},
		}, &indexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   0,
			IndexID:    2,
			Delete:     true,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("ttt"))},
		}}
	checkDMLKVEntries(ctx, c, tableInfo, m, plr, expect)

	pm.MustExec("delete from test.test1 where id = '777'")
	expect = []kvEntry{
		&rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 1,
			Delete:   true,
			Row:      map[int64]types.Datum{},
		}, &indexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   0,
			IndexID:    2,
			Delete:     true,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("777"))},
		}, &indexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   1,
			IndexID:    1,
			Delete:     true,
			IndexValue: []types.Datum{types.NewIntDatum(666)},
		}}
	checkDMLKVEntries(ctx, c, tableInfo, m, plr, expect)
}

func (s *kvEntrySuite) testPkIsHandleDML(c *check.C, newRowFormat bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm, schema := setUpPullerAndSchema(ctx, c, newRowFormat,
		"create table test.test2(id int primary key, b varchar(255) unique key)",
	)
	m := NewTxnMounter(schema)

	tableInfo := pm.GetTableInfo("test", "test2")
	tableID := tableInfo.ID

	plr := pm.CreatePuller(0, []regionspan.Span{regionspan.GetTableSpan(tableID, false)})

	pm.MustExec("insert into test.test2 values(666,'aaa')")
	expect := []kvEntry{
		&rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 666,
			Delete:   false,
			Row:      map[int64]types.Datum{1: types.NewIntDatum(666), 2: types.NewBytesDatum([]byte("aaa"))},
		}, &indexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   666,
			IndexID:    1,
			Delete:     false,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("aaa"))},
		}}
	checkDMLKVEntries(ctx, c, tableInfo, m, plr, expect)

	pm.MustExec("update test.test2 set id = 888,b = 'bbb' where id = 666")
	expect = []kvEntry{
		&rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 666,
			Delete:   true,
			Row:      map[int64]types.Datum{1: types.NewIntDatum(666)},
		}, &rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 888,
			Delete:   false,
			Row:      map[int64]types.Datum{1: types.NewIntDatum(888), 2: types.NewBytesDatum([]byte("bbb"))},
		}, &indexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   0,
			IndexID:    1,
			Delete:     true,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("aaa"))},
		}, &indexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   888,
			IndexID:    1,
			Delete:     false,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("bbb"))},
		}}
	checkDMLKVEntries(ctx, c, tableInfo, m, plr, expect)

	pm.MustExec("delete from test.test2 where id = 888")
	expect = []kvEntry{
		&rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 888,
			Delete:   true,
			Row:      map[int64]types.Datum{1: types.NewIntDatum(888)},
		}, &indexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   0,
			IndexID:    1,
			Delete:     true,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("bbb"))},
		}}
	checkDMLKVEntries(ctx, c, tableInfo, m, plr, expect)
}

func (s *kvEntrySuite) testUkWithNull(c *check.C, newRowFormat bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm, schema := setUpPullerAndSchema(ctx, c, newRowFormat,
		"create table test.test2( a int, b varchar(255), c date, unique key(a,b,c))",
	)
	m := NewTxnMounter(schema)

	tableInfo := pm.GetTableInfo("test", "test2")
	tableID := tableInfo.ID

	plr := pm.CreatePuller(0, []regionspan.Span{regionspan.GetTableSpan(tableID, false)})

	pm.MustExec("insert into test.test2 values(null, 'aa', '1996-11-20')")
	time, err := types.ParseDate(nil, "1996-11-20")
	c.Assert(err, check.IsNil)
	expect := []kvEntry{
		&rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 1,
			Delete:   false,
			Row:      map[int64]types.Datum{2: types.NewBytesDatum([]byte("aa")), 3: types.NewTimeDatum(time)},
		},
		&indexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   1,
			IndexID:    1,
			Delete:     false,
			IndexValue: []types.Datum{{}, types.NewBytesDatum([]byte("aa")), types.NewTimeDatum(time)},
		}}
	checkDMLKVEntries(ctx, c, tableInfo, m, plr, expect)

	pm.MustExec("insert into test.test2 values(null, null, '1996-11-20')")
	expect = []kvEntry{
		&rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 2,
			Delete:   false,
			Row:      map[int64]types.Datum{3: types.NewTimeDatum(time)},
		},
		&indexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   2,
			IndexID:    1,
			Delete:     false,
			IndexValue: []types.Datum{{}, {}, types.NewTimeDatum(time)},
		}}
	checkDMLKVEntries(ctx, c, tableInfo, m, plr, expect)

	pm.MustExec("insert into test.test2 values(null, null, null)")
	expect = []kvEntry{
		&rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 3,
			Delete:   false,
			Row:      map[int64]types.Datum{},
		},
		&indexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   3,
			IndexID:    1,
			Delete:     false,
			IndexValue: []types.Datum{{}, {}, {}},
		}}
	checkDMLKVEntries(ctx, c, tableInfo, m, plr, expect)

	pm.MustExec("delete from test.test2 where c is null")
	expect = []kvEntry{
		&rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 3,
			Delete:   true,
			Row:      map[int64]types.Datum{},
		},
		&indexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   3,
			IndexID:    1,
			Delete:     true,
			IndexValue: []types.Datum{{}, {}, {}},
		}}
	checkDMLKVEntries(ctx, c, tableInfo, m, plr, expect)

	pm.MustExec("update test.test2 set a = 1, b = null where a is null and b is not null")
	expect = []kvEntry{
		&rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 1,
			Delete:   false,
			Row:      map[int64]types.Datum{1: types.NewIntDatum(1), 3: types.NewTimeDatum(time)},
		},
		&indexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   1,
			IndexID:    1,
			Delete:     false,
			IndexValue: []types.Datum{types.NewIntDatum(1), {}, types.NewTimeDatum(time)},
		},
		&indexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   1,
			IndexID:    1,
			Delete:     true,
			IndexValue: []types.Datum{{}, types.NewBytesDatum([]byte("aa")), types.NewTimeDatum(time)},
		}}
	checkDMLKVEntries(ctx, c, tableInfo, m, plr, expect)
}

func (s *kvEntrySuite) testUkWithNoPk(c *check.C, newRowFormat bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm, schema := setUpPullerAndSchema(ctx, c, newRowFormat,
		"CREATE TABLE test.cdc_uk_with_no_pk (id INT, a1 INT, a3 INT, UNIQUE KEY dex1(a1, a3));",
	)
	m := NewTxnMounter(schema)

	tableInfo := pm.GetTableInfo("test", "cdc_uk_with_no_pk")
	tableID := tableInfo.ID

	plr := pm.CreatePuller(0, []regionspan.Span{regionspan.GetTableSpan(tableID, false)})

	pm.MustExec("INSERT INTO test.cdc_uk_with_no_pk(id, a1, a3) VALUES(5, 6, NULL);")
	expect := []kvEntry{
		&rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 1,
			Delete:   false,
			Row:      map[int64]types.Datum{1: types.NewIntDatum(5), 2: types.NewIntDatum(6)},
		},
		&indexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   1,
			IndexID:    1,
			Delete:     false,
			IndexValue: []types.Datum{types.NewIntDatum(6), {}},
		}}
	checkDMLKVEntries(ctx, c, tableInfo, m, plr, expect)

	pm.MustExec("UPDATE test.cdc_uk_with_no_pk SET id = 10 WHERE id = 5;")
	expect = []kvEntry{
		&rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 1,
			Delete:   false,
			Row:      map[int64]types.Datum{1: types.NewIntDatum(10), 2: types.NewIntDatum(6)},
		}}
	checkDMLKVEntries(ctx, c, tableInfo, m, plr, expect)
}

func (s *kvEntrySuite) TestCreateTable(c *check.C) {
		defer testleak.AfterTest(c)()
	s.testCreateTable(c, true)
	s.testCreateTable(c, false)
}
func (s *kvEntrySuite) TestPkIsNotHandleDML(c *check.C) {
		defer testleak.AfterTest(c)()
	s.testPkIsNotHandleDML(c, true)
	s.testPkIsNotHandleDML(c, false)
}
func (s *kvEntrySuite) TestPkIsHandleDML(c *check.C) {
		defer testleak.AfterTest(c)()
	s.testPkIsHandleDML(c, true)
	s.testPkIsHandleDML(c, false)
}
func (s *kvEntrySuite) TestUkWithNull(c *check.C) {
		defer testleak.AfterTest(c)()
	s.testUkWithNull(c, true)
	s.testUkWithNull(c, false)
}
func (s *kvEntrySuite) TestUkWithNoPk(c *check.C) {
		defer testleak.AfterTest(c)()
	s.testUkWithNoPk(c, true)
	s.testUkWithNoPk(c, false)
}

func assertIn(c *check.C, item kvEntry, expect []kvEntry) {
	for _, e := range expect {
		if reflect.DeepEqual(item, e) {
			return
		}
	}
	c.Fatalf("item {%#v} is not exist in expect {%#v}", item, expect)
}

func checkDMLKVEntries(ctx context.Context, c *check.C, tableInfo *TableInfo, m *Mounter, plr puller.Puller, expect []kvEntry) {
	ctx, cancel := context.WithCancel(ctx)
	err := plr.CollectRawTxns(ctx, func(ctx context.Context, rawTxn model.RawTxn) error {
		eventSum := 0
		for _, raw := range rawTxn.Entries {
			entry, err := m.unmarshal(raw)
			c.Assert(err, check.IsNil)
			switch e := entry.(type) {
			case *rowKVEntry:
				e.Ts = 0
				assertIn(c, e, expect)
				eventSum++
			case *indexKVEntry:
				c.Assert(e.unflatten(tableInfo), check.IsNil)
				e.Ts = 0
				assertIn(c, e, expect)
				eventSum++
			}
		}
		if eventSum != 0 {
			c.Assert(eventSum, check.Equals, len(expect))
			cancel()
		}
		return nil
	})
	c.Assert(errors.Cause(err), check.Equals, context.Canceled)
}
*/
