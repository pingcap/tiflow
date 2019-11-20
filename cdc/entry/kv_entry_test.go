package entry

import (
	"reflect"
	"time"

	"github.com/pingcap/check"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/mock"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/tidb/types"
)

type kvEntrySuite struct {
}

var _ = check.Suite(&kvEntrySuite{})

func (s *kvEntrySuite) TestCreateTable(c *check.C) {
	c.Skip("DDL is undetectable now in unit test environment")
	puller, err := mock.NewMockPuller()
	c.Assert(err, check.IsNil)
	rawEntries := puller.MustExec(c, "create table test.test1(id varchar(255) primary key, a int, index i1 (a))")
	existUpdateTableKVEntry := false
	existDDLJobHistoryKVEntry := false
	for _, raw := range rawEntries {
		entry, err := Unmarshal(raw)
		c.Assert(err, check.IsNil)
		switch e := entry.(type) {
		case *UpdateTableKVEntry:
			existUpdateTableKVEntry = true
			c.Assert(e.TableID, check.Equals, e.TableInfo.ID)
			c.Assert(e.TableInfo.Name.O, check.Equals, "test1")
			c.Assert(len(e.TableInfo.Columns), check.Equals, 2)
			c.Assert(e.TableInfo.Columns[0].Name.O, check.Equals, "id")
			c.Assert(e.TableInfo.Columns[1].Name.O, check.Equals, "a")
			c.Assert(e.TableInfo.Columns[0].Tp, check.Equals, mysql.TypeVarchar)
			c.Assert(e.TableInfo.Columns[1].Tp, check.Equals, mysql.TypeLong)
			c.Assert(e.TableInfo.PKIsHandle, check.IsFalse)
			c.Assert(len(e.TableInfo.Indices), check.Equals, 2)
			// i1 index
			c.Assert(e.TableInfo.Indices[0].Name.O, check.Equals, "i1")
			c.Assert(e.TableInfo.Indices[0].Tp, check.Equals, timodel.IndexTypeBtree)
			c.Assert(e.TableInfo.Indices[0].Unique, check.IsFalse)
			c.Assert(e.TableInfo.Indices[0].Columns[0].Name.O, check.Equals, "a")
			c.Assert(e.TableInfo.Indices[0].Columns[0].Offset, check.Equals, 1)
			// primary index
			c.Assert(e.TableInfo.Indices[1].Name.O, check.Equals, "PRIMARY")
			c.Assert(e.TableInfo.Indices[1].Tp, check.Equals, timodel.IndexTypeBtree)
			c.Assert(e.TableInfo.Indices[1].Unique, check.IsTrue)
			c.Assert(e.TableInfo.Indices[1].Columns[0].Name.O, check.Equals, "id")
			c.Assert(e.TableInfo.Indices[1].Columns[0].Offset, check.Equals, 0)
		case *DDLJobKVEntry:
			existDDLJobHistoryKVEntry = true
			c.Assert(e.JobID, check.Equals, e.Job.ID)
			c.Assert(e.Job.SchemaName, check.Equals, "test")
			c.Assert(e.Job.Type, check.Equals, timodel.ActionCreateTable)
			c.Assert(e.Job.Query, check.Equals, "create table test.test1(id varchar(255) primary key, a int, index i1 (a))")
		}
	}
	c.Assert(existUpdateTableKVEntry, check.IsTrue)
	c.Assert(existDDLJobHistoryKVEntry, check.IsTrue)

	rawEntries = puller.MustExec(c, "create table test.test2(id int primary key, b varchar(255) unique key)")
	existUpdateTableKVEntry = false
	existDDLJobHistoryKVEntry = false
	for _, raw := range rawEntries {
		entry, err := Unmarshal(raw)
		c.Assert(err, check.IsNil)
		switch e := entry.(type) {
		case *UpdateTableKVEntry:
			existUpdateTableKVEntry = true
			c.Assert(e.TableID, check.Equals, e.TableInfo.ID)
			c.Assert(e.TableInfo.Name.O, check.Equals, "test2")
			c.Assert(len(e.TableInfo.Columns), check.Equals, 2)
			c.Assert(e.TableInfo.Columns[0].Name.O, check.Equals, "id")
			c.Assert(e.TableInfo.Columns[1].Name.O, check.Equals, "b")
			c.Assert(e.TableInfo.Columns[0].Tp, check.Equals, mysql.TypeLong)
			c.Assert(e.TableInfo.Columns[1].Tp, check.Equals, mysql.TypeVarchar)
			c.Assert(e.TableInfo.PKIsHandle, check.IsTrue)
			c.Assert(len(e.TableInfo.Indices), check.Equals, 1)
			c.Assert(e.TableInfo.Indices[0].Name.O, check.Equals, "b")
			c.Assert(e.TableInfo.Indices[0].Tp, check.Equals, timodel.IndexTypeBtree)
			c.Assert(e.TableInfo.Indices[0].Unique, check.IsTrue)
			c.Assert(e.TableInfo.Indices[0].Columns[0].Name.O, check.Equals, "b")
		case *DDLJobKVEntry:
			existDDLJobHistoryKVEntry = true
			c.Assert(e.JobID, check.Equals, e.Job.ID)
			c.Assert(e.Job.SchemaName, check.Equals, "test")
			c.Assert(e.Job.Type, check.Equals, timodel.ActionCreateTable)
			c.Assert(e.Job.Query, check.Equals, "create table test.test2(id int primary key, b varchar(255) unique key)")
		}
	}
	c.Assert(existUpdateTableKVEntry, check.IsTrue)
	c.Assert(existDDLJobHistoryKVEntry, check.IsTrue)
}

func (s *kvEntrySuite) TestPkIsNotHandleDML(c *check.C) {
	puller, err := mock.NewMockPuller()
	c.Assert(err, check.IsNil)
	rawEntries := puller.MustExec(c, "create table test.test1(id varchar(255) primary key, a int, index ci (a))")
	var tableInfo *timodel.TableInfo
	for _, raw := range rawEntries {
		entry, err := Unmarshal(raw)
		c.Assert(err, check.IsNil)
		switch e := entry.(type) {
		case *UpdateTableKVEntry:
			tableInfo = e.TableInfo
		}
	}
	c.Assert(tableInfo, check.NotNil)

	rawEntries = puller.MustExec(c, "insert into test.test1 values('ttt',666)")
	expect := []KVEntry{
		&RowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 1,
			Delete:   false,
			Row:      map[int64]types.Datum{1: types.NewBytesDatum([]byte("ttt")), 2: types.NewIntDatum(666)},
		}, &IndexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   1,
			IndexID:    1,
			Delete:     false,
			IndexValue: []types.Datum{types.NewIntDatum(666)},
		}, &IndexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   1,
			IndexID:    2,
			Delete:     false,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("ttt"))},
		}}
	checkDMLKVEntries(c, tableInfo, rawEntries, expect)

	rawEntries = puller.MustExec(c, "update test.test1 set id = '777' where a = 666")
	expect = []KVEntry{
		&RowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 1,
			Delete:   false,
			Row:      map[int64]types.Datum{1: types.NewBytesDatum([]byte("777")), 2: types.NewIntDatum(666)},
		}, &IndexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   1,
			IndexID:    2,
			Delete:     false,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("777"))},
		}, &IndexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   0,
			IndexID:    2,
			Delete:     true,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("ttt"))},
		}}
	checkDMLKVEntries(c, tableInfo, rawEntries, expect)

	rawEntries = puller.MustExec(c, "delete from test.test1 where id = '777'")
	expect = []KVEntry{
		&RowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 1,
			Delete:   true,
			Row:      map[int64]types.Datum{},
		}, &IndexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   0,
			IndexID:    2,
			Delete:     true,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("777"))},
		}, &IndexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   1,
			IndexID:    1,
			Delete:     true,
			IndexValue: []types.Datum{types.NewIntDatum(666)},
		}}
	checkDMLKVEntries(c, tableInfo, rawEntries, expect)
}

func (s *kvEntrySuite) TestPkIsHandleDML(c *check.C) {
	puller, err := mock.NewMockPuller()
	c.Assert(err, check.IsNil)
	rawEntries := puller.MustExec(c, "create table test.test2(id int primary key, b varchar(255) unique key)")
	var tableInfo *timodel.TableInfo
	for _, raw := range rawEntries {
		entry, err := Unmarshal(raw)
		c.Assert(err, check.IsNil)
		switch e := entry.(type) {
		case *UpdateTableKVEntry:
			tableInfo = e.TableInfo
		}
	}
	c.Assert(tableInfo, check.NotNil)

	rawEntries = puller.MustExec(c, "insert into test.test2 values(666,'aaa')")
	expect := []KVEntry{
		&RowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 666,
			Delete:   false,
			Row:      map[int64]types.Datum{2: types.NewBytesDatum([]byte("aaa"))},
		}, &IndexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   666,
			IndexID:    1,
			Delete:     false,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("aaa"))},
		}}
	checkDMLKVEntries(c, tableInfo, rawEntries, expect)

	rawEntries = puller.MustExec(c, "update test.test2 set id = 888,b = 'bbb' where id = 666")
	expect = []KVEntry{
		&RowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 666,
			Delete:   true,
			Row:      map[int64]types.Datum{},
		}, &RowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 888,
			Delete:   false,
			Row:      map[int64]types.Datum{2: types.NewBytesDatum([]byte("bbb"))},
		}, &IndexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   0,
			IndexID:    1,
			Delete:     true,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("aaa"))},
		}, &IndexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   888,
			IndexID:    1,
			Delete:     false,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("bbb"))},
		}}
	checkDMLKVEntries(c, tableInfo, rawEntries, expect)

	rawEntries = puller.MustExec(c, "delete from test.test2 where id = 888")
	expect = []KVEntry{
		&RowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 888,
			Delete:   true,
			Row:      map[int64]types.Datum{},
		}, &IndexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   0,
			IndexID:    1,
			Delete:     true,
			IndexValue: []types.Datum{types.NewBytesDatum([]byte("bbb"))},
		}}
	checkDMLKVEntries(c, tableInfo, rawEntries, expect)

}

func (s *kvEntrySuite) TestUkWithNull(c *check.C) {
	puller, err := mock.NewMockPuller()
	c.Assert(err, check.IsNil)

	rawEntries := puller.MustExec(c, "create table test.test2( a int, b varchar(255), c date, unique key(a,b,c))")
	var tableInfo *timodel.TableInfo
	for _, raw := range rawEntries {
		entry, err := Unmarshal(raw)
		c.Assert(err, check.IsNil)
		switch e := entry.(type) {
		case *UpdateTableKVEntry:
			tableInfo = e.TableInfo
		}
	}
	c.Assert(tableInfo, check.NotNil)

	rawEntries = puller.MustExec(c, "insert into test.test2 values(null, 'aa', '1996-11-20')")
	time, err := types.ParseDate(nil, "1996-11-20")
	c.Assert(err, check.IsNil)
	expect := []KVEntry{
		&RowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 1,
			Delete:   false,
			Row:      map[int64]types.Datum{2: types.NewBytesDatum([]byte("aa")), 3: types.NewTimeDatum(time)},
		},
		&IndexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   1,
			IndexID:    1,
			Delete:     false,
			IndexValue: []types.Datum{{}, types.NewBytesDatum([]byte("aa")), types.NewTimeDatum(time)},
		}}
	checkDMLKVEntries(c, tableInfo, rawEntries, expect)

	rawEntries = puller.MustExec(c, "insert into test.test2 values(null, null, '1996-11-20')")
	expect = []KVEntry{
		&RowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 2,
			Delete:   false,
			Row:      map[int64]types.Datum{3: types.NewTimeDatum(time)},
		},
		&IndexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   2,
			IndexID:    1,
			Delete:     false,
			IndexValue: []types.Datum{{}, {}, types.NewTimeDatum(time)},
		}}
	checkDMLKVEntries(c, tableInfo, rawEntries, expect)

	rawEntries = puller.MustExec(c, "insert into test.test2 values(null, null, null)")
	expect = []KVEntry{
		&RowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 3,
			Delete:   false,
			Row:      map[int64]types.Datum{},
		},
		&IndexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   3,
			IndexID:    1,
			Delete:     false,
			IndexValue: []types.Datum{{}, {}, {}},
		}}
	checkDMLKVEntries(c, tableInfo, rawEntries, expect)

	rawEntries = puller.MustExec(c, "delete from test.test2 where c is null")
	expect = []KVEntry{
		&RowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 3,
			Delete:   true,
			Row:      map[int64]types.Datum{},
		},
		&IndexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   3,
			IndexID:    1,
			Delete:     true,
			IndexValue: []types.Datum{{}, {}, {}},
		}}
	checkDMLKVEntries(c, tableInfo, rawEntries, expect)

	rawEntries = puller.MustExec(c, "update test.test2 set a = 1, b = null where a is null and b is not null")
	expect = []KVEntry{
		&RowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 1,
			Delete:   false,
			Row:      map[int64]types.Datum{1: types.NewIntDatum(1), 3: types.NewTimeDatum(time)},
		},
		&IndexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   1,
			IndexID:    1,
			Delete:     false,
			IndexValue: []types.Datum{types.NewIntDatum(1), {}, types.NewTimeDatum(time)},
		},
		&IndexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   1,
			IndexID:    1,
			Delete:     true,
			IndexValue: []types.Datum{{}, types.NewBytesDatum([]byte("aa")), types.NewTimeDatum(time)},
		}}
	checkDMLKVEntries(c, tableInfo, rawEntries, expect)
}

func (s *kvEntrySuite) TestUkWithNoPk(c *check.C) {
	puller, err := mock.NewMockPuller()
	c.Assert(err, check.IsNil)

	rawEntries := puller.MustExec(c, "CREATE TABLE test.cdc_uk_with_no_pk (id INT, a1 INT, a3 INT, UNIQUE KEY dex1(a1, a3));")
	var tableInfo *timodel.TableInfo
	for _, raw := range rawEntries {
		entry, err := Unmarshal(raw)
		c.Assert(err, check.IsNil)
		switch e := entry.(type) {
		case *UpdateTableKVEntry:
			tableInfo = e.TableInfo
		}
	}
	c.Assert(tableInfo, check.NotNil)

	rawEntries = puller.MustExec(c, "INSERT INTO test.cdc_uk_with_no_pk(id, a1, a3) VALUES(5, 6, NULL);")
	expect := []KVEntry{
		&RowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 1,
			Delete:   false,
			Row:      map[int64]types.Datum{1: types.NewIntDatum(5), 2: types.NewIntDatum(6)},
		},
		&IndexKVEntry{
			TableID:    tableInfo.ID,
			RecordID:   1,
			IndexID:    1,
			Delete:     false,
			IndexValue: []types.Datum{types.NewIntDatum(6), {}},
		}}
	checkDMLKVEntries(c, tableInfo, rawEntries, expect)

	rawEntries = puller.MustExec(c, "UPDATE test.cdc_uk_with_no_pk SET id = 10 WHERE id = 5;")
	expect = []KVEntry{
		&RowKVEntry{
			TableID:  tableInfo.ID,
			RecordID: 1,
			Delete:   false,
			Row:      map[int64]types.Datum{1: types.NewIntDatum(10), 2: types.NewIntDatum(6)},
		}}
	checkDMLKVEntries(c, tableInfo, rawEntries, expect)

}

func assertIn(c *check.C, item KVEntry, expect []KVEntry) {
	for _, e := range expect {
		if reflect.DeepEqual(item, e) {
			return
		}
	}
	c.Fatalf("item {%#v} is not exist in expect {%#v}", item, expect)
}

func checkDMLKVEntries(c *check.C, tableInfo *timodel.TableInfo, rawEntries []*model.RawKVEntry, expect []KVEntry) {
	eventSum := 0
	for _, raw := range rawEntries {
		entry, err := Unmarshal(raw)
		c.Assert(err, check.IsNil)
		switch e := entry.(type) {
		case *RowKVEntry:
			c.Assert(e.Unflatten(tableInfo, time.UTC), check.IsNil)
			e.Ts = 0
			assertIn(c, e, expect)
			eventSum++
		case *IndexKVEntry:
			c.Assert(e.Unflatten(tableInfo, time.UTC), check.IsNil)
			e.Ts = 0
			assertIn(c, e, expect)
			eventSum++
		}
	}
	c.Assert(eventSum, check.Equals, len(expect))
}

func (s *kvEntrySuite) TestAllKVS(c *check.C) {
	puller, err := mock.NewMockPuller()
	c.Assert(err, check.IsNil)
	puller.ScanAll(func(rawKVEntry *model.RawKVEntry) {
		_, err := Unmarshal(rawKVEntry)
		c.Assert(err, check.IsNil)
	})
}
