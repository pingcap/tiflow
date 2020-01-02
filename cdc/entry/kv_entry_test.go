package entry

import (
	"context"
	"reflect"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/cdc/schema"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
)

type kvEntrySuite struct {
}

var _ = check.Suite(&kvEntrySuite{})

func (s *kvEntrySuite) TestCreateTable(c *check.C) {
	pm, pmCtx, pmCancel := startPullerManager(c)
	defer pmCancel()

	pm.MustExec("create table test.test1(id varchar(255) primary key, a int, index i1 (a))")

	plr := pm.CreatePuller(0, []util.Span{util.GetDDLSpan()})
	existDDLJobHistoryKVEntry := false

	// create another context with canceled, we can close this puller but not affect puller manager
	plrCtx, plrCancel := context.WithCancel(pmCtx)
	err := plr.CollectRawTxns(plrCtx, func(ctx context.Context, rawTxn model.RawTxn) error {
		for _, raw := range rawTxn.Entries {
			entry, err := unmarshal(raw)
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

	// create another puller, pull KVs from now
	nowTso := oracle.EncodeTSO(time.Now().UnixNano() / int64(time.Millisecond))
	plr = pm.CreatePuller(nowTso, []util.Span{util.GetDDLSpan()})
	pm.MustExec("create table test.test2(id int primary key, b varchar(255) unique key)")

	existDDLJobHistoryKVEntry = false
	plrCtx, plrCancel = context.WithCancel(pmCtx)
	err = plr.CollectRawTxns(plrCtx, func(ctx context.Context, rawTxn model.RawTxn) error {
		for _, raw := range rawTxn.Entries {
			entry, err := unmarshal(raw)
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
}

func (s *kvEntrySuite) TestPkIsNotHandleDML(c *check.C) {
	pm, pmCtx, pmCancel := startPullerManager(c)
	defer pmCancel()

	pm.MustExec("create table test.test1(id varchar(255) primary key, a int, index ci (a))")
	tableInfo := pm.GetTableInfo("test", "test1")
	tableID := tableInfo.ID

	plr := pm.CreatePuller(0, []util.Span{util.GetTableSpan(tableID, false)})

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
	checkDMLKVEntries(pmCtx, c, tableInfo, plr, expect)

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
	checkDMLKVEntries(pmCtx, c, tableInfo, plr, expect)

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
	checkDMLKVEntries(pmCtx, c, tableInfo, plr, expect)
}

func (s *kvEntrySuite) TestPkIsHandleDML(c *check.C) {
	pm, pmCtx, pmCancel := startPullerManager(c)
	defer pmCancel()

	pm.MustExec("create table test.test2(id int primary key, b varchar(255) unique key)")
	tableInfo := pm.GetTableInfo("test", "test2")
	tableID := tableInfo.ID

	plr := pm.CreatePuller(0, []util.Span{util.GetTableSpan(tableID, false)})

	pm.MustExec("insert into test.test2 values(666,'aaa')")
	expect := []kvEntry{
		&rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 666,
			Delete:   false,
			Row:      map[int64]types.Datum{2: types.NewBytesDatum([]byte("aaa"))},
		}, &indexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   666,
			IndexID:    1,
			Delete:     false,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("aaa"))},
		}}
	checkDMLKVEntries(pmCtx, c, tableInfo, plr, expect)

	pm.MustExec("update test.test2 set id = 888,b = 'bbb' where id = 666")
	expect = []kvEntry{
		&rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 666,
			Delete:   true,
			Row:      map[int64]types.Datum{},
		}, &rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 888,
			Delete:   false,
			Row:      map[int64]types.Datum{2: types.NewBytesDatum([]byte("bbb"))},
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
	checkDMLKVEntries(pmCtx, c, tableInfo, plr, expect)

	pm.MustExec("delete from test.test2 where id = 888")
	expect = []kvEntry{
		&rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 888,
			Delete:   true,
			Row:      map[int64]types.Datum{},
		}, &indexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   0,
			IndexID:    1,
			Delete:     true,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("bbb"))},
		}}
	checkDMLKVEntries(pmCtx, c, tableInfo, plr, expect)
}

func (s *kvEntrySuite) TestUkWithNull(c *check.C) {
	pm, pmCtx, pmCancel := startPullerManager(c)
	defer pmCancel()

	pm.MustExec("create table test.test2( a int, b varchar(255), c date, unique key(a,b,c))")
	tableInfo := pm.GetTableInfo("test", "test2")
	tableID := tableInfo.ID

	plr := pm.CreatePuller(0, []util.Span{util.GetTableSpan(tableID, false)})

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
	checkDMLKVEntries(pmCtx, c, tableInfo, plr, expect)

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
	checkDMLKVEntries(pmCtx, c, tableInfo, plr, expect)

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
	checkDMLKVEntries(pmCtx, c, tableInfo, plr, expect)

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
	checkDMLKVEntries(pmCtx, c, tableInfo, plr, expect)

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
	checkDMLKVEntries(pmCtx, c, tableInfo, plr, expect)
}

func (s *kvEntrySuite) TestUkWithNoPk(c *check.C) {
	pm, pmCtx, pmCancel := startPullerManager(c)
	defer pmCancel()

	pm.MustExec("CREATE TABLE test.cdc_uk_with_no_pk (id INT, a1 INT, a3 INT, UNIQUE KEY dex1(a1, a3));")
	tableInfo := pm.GetTableInfo("test", "cdc_uk_with_no_pk")
	tableID := tableInfo.ID

	plr := pm.CreatePuller(0, []util.Span{util.GetTableSpan(tableID, false)})

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
	checkDMLKVEntries(pmCtx, c, tableInfo, plr, expect)

	pm.MustExec("UPDATE test.cdc_uk_with_no_pk SET id = 10 WHERE id = 5;")
	expect = []kvEntry{
		&rowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 1,
			Delete:   false,
			Row:      map[int64]types.Datum{1: types.NewIntDatum(10), 2: types.NewIntDatum(6)},
		}}
	checkDMLKVEntries(pmCtx, c, tableInfo, plr, expect)
}

func assertIn(c *check.C, item kvEntry, expect []kvEntry) {
	for _, e := range expect {
		if reflect.DeepEqual(item, e) {
			return
		}
	}
	c.Fatalf("item {%#v} is not exist in expect {%#v}", item, expect)
}

func startPullerManager(c *check.C) (*puller.MockPullerManager, context.Context, context.CancelFunc) {
	// create and run mock puller manager
	pm := puller.NewMockPullerManager(c)
	pmCtx, pmCancel := context.WithCancel(context.Background())
	go pm.Run(pmCtx)
	return pm, pmCtx, pmCancel
}

func checkDMLKVEntries(ctx context.Context, c *check.C, tableInfo *schema.TableInfo, plr puller.Puller, expect []kvEntry) {
	ctx, cancel := context.WithCancel(ctx)
	err := plr.CollectRawTxns(ctx, func(ctx context.Context, rawTxn model.RawTxn) error {
		eventSum := 0
		for _, raw := range rawTxn.Entries {
			entry, err := unmarshal(raw)
			c.Assert(err, check.IsNil)
			switch e := entry.(type) {
			case *rowKVEntry:
				c.Assert(e.unflatten(tableInfo), check.IsNil)
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
