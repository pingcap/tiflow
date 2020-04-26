package entry

import (
	"context"
	"math"
	"reflect"

	"github.com/pingcap/parser/mysql"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/pkg/util"
)

type mountTxnsSuite struct{}

var _ = check.Suite(&mountTxnsSuite{})

func setUpPullerAndSchema(c *check.C, newRowFormat bool, sqls ...string) (*puller.MockPullerManager, *mounterImpl) {
	pm := puller.NewMockPullerManager(c, newRowFormat)
	for _, sql := range sqls {
		pm.MustExec(sql)
	}
	jobs := pm.GetDDLJobs()
	schemaStorage, err := NewSchemaStorage(jobs, nil)
	c.Assert(err, check.IsNil)
	schemaStorage.AdvanceResolvedTs(math.MaxUint64)
	return pm, NewMounter(schemaStorage).(*mounterImpl)
}

func nextRawKVEntry(input <-chan *model.RawKVEntry) *model.RawKVEntry {
	for raw := range input {
		if raw.OpType == model.OpTypeDelete || raw.OpType == model.OpTypePut {
			return raw
		}
	}
	return nil
}

func (cs *mountTxnsSuite) testInsertPkNotHandle(c *check.C, newRowFormat bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm, mounter := setUpPullerAndSchema(c, newRowFormat,
		"create database testDB",
		"create table testDB.test1(id varchar(255) primary key, a int, index ci (a))",
	)
	_, tableInfo := pm.GetTableInfo("testDB", "test1")
	tableID := tableInfo.ID
	plr := pm.CreatePuller(0, []util.Span{util.GetTableSpan(tableID, false)})
	go func() {
		err := plr.Run(ctx)
		if err != nil {
			c.Fatal(err)
		}
	}()

	trueBool := true
	pm.MustExec("insert into testDB.test1 values('ttt',6)")

	expecteds := []*model.RowChangedEvent{
		{
			Schema:       "testDB",
			Table:        "test1",
			IndieMarkCol: "id",
			Columns: map[string]*model.Column{
				"id": {
					Type:        mysql.TypeVarchar,
					WhereHandle: &trueBool,
					Value:       "ttt",
				},
				"a": {
					Type:  mysql.TypeLong,
					Value: int64(6),
				},
			},
		}, nil, nil,
	}
	cs.assertRowEquals(c, func() *model.RowChangedEvent {
		rawRow := nextRawKVEntry(plr.Output())
		row, err := mounter.unmarshalAndMountRowChanged(ctx, rawRow)
		c.Assert(err, check.IsNil)
		return row
	}, expecteds)

	pm.MustExec("update testDB.test1 set id = 'vvv' where a = 6")

	expecteds = []*model.RowChangedEvent{
		{
			Schema:       "testDB",
			Table:        "test1",
			IndieMarkCol: "id",
			Columns: map[string]*model.Column{
				"id": {
					Type:        mysql.TypeVarchar,
					WhereHandle: &trueBool,
					Value:       "vvv",
				},
				"a": {
					Type:  mysql.TypeLong,
					Value: int64(6),
				},
			},
		}, nil, nil, nil, nil, nil,
	}
	cs.assertRowEquals(c, func() *model.RowChangedEvent {
		rawRow := nextRawKVEntry(plr.Output())
		row, err := mounter.unmarshalAndMountRowChanged(ctx, rawRow)
		c.Assert(err, check.IsNil)
		return row
	}, expecteds)

	//rawTxn = getFirstRealTxn(ctx, c, plr)
	//t, err = mounter.Mount(rawTxn)
	//c.Assert(err, check.IsNil)
	//cs.assertTableTxnEquals(c, t, model.Txn{
	//	Ts: rawTxn.Entries[0].Ts,
	//	DMLs: []*model.DML{
	//		{
	//			Database: "testDB",
	//			Table:    "test1",
	//			Tp:       model.DeleteDMLType,
	//			Values: map[string]types.Datum{
	//				"id": types.NewBytesDatum([]byte("ttt")),
	//			},
	//		},
	//		{
	//			Database: "testDB",
	//			Table:    "test1",
	//			Tp:       model.InsertDMLType,
	//			Values: map[string]types.Datum{
	//				"id": types.NewBytesDatum([]byte("vvv")),
	//				"a":  types.NewIntDatum(6),
	//			},
	//		},
	//	},
	//})
	//
	//pm.MustExec("delete from testDB.test1 where a = 6")
	//rawTxn = getFirstRealTxn(ctx, c, plr)
	//t, err = mounter.Mount(rawTxn)
	//c.Assert(err, check.IsNil)
	//cs.assertTableTxnEquals(c, t, model.Txn{
	//	Ts: rawTxn.Entries[0].Ts,
	//	DMLs: []*model.DML{
	//		{
	//			Database: "testDB",
	//			Table:    "test1",
	//			Tp:       model.DeleteDMLType,
	//			Values: map[string]types.Datum{
	//				"id": types.NewBytesDatum([]byte("vvv")),
	//			},
	//		},
	//	},
	//})
}

/*
func (cs *mountTxnsSuite) testIncompleteRow(c *check.C, newRowFormat bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm, schema := setUpPullerAndSchema(ctx, c, newRowFormat,
		"create database testDB",
		"create table testDB.test1 (id int primary key, val int);",
	)
	tableInfo := pm.GetTableInfo("testDB", "test1")
	tableID := tableInfo.ID
	mounter := NewTxnMounter(schema)
	plr := pm.CreatePuller(0, []util.Span{util.GetTableSpan(tableID, false)})

	pm.MustExec("insert into testDB.test1(id) values (16),(32);")
	rawTxn := getFirstRealTxn(ctx, c, plr)
	t, err := mounter.Mount(rawTxn)
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, model.Txn{
		Ts: rawTxn.Entries[0].Ts,
		DMLs: []*model.DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.InsertDMLType,
				Values: map[string]types.Datum{
					"id":  types.NewIntDatum(16),
					"val": types.NewDatum(nil),
				},
			},
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.InsertDMLType,
				Values: map[string]types.Datum{
					"id":  types.NewIntDatum(32),
					"val": types.NewDatum(nil),
				},
			},
		},
	})

	pm.MustExec("insert into testDB.test1(id,val) values (18, 6);")
	rawTxn = getFirstRealTxn(ctx, c, plr)
	t, err = mounter.Mount(rawTxn)
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, model.Txn{
		Ts: rawTxn.Entries[0].Ts,
		DMLs: []*model.DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.InsertDMLType,
				Values: map[string]types.Datum{
					"id":  types.NewIntDatum(18),
					"val": types.NewIntDatum(6),
				},
			},
		},
	})

}

func (cs *mountTxnsSuite) testInsertPkIsHandle(c *check.C, newRowFormat bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm, schema := setUpPullerAndSchema(ctx, c, newRowFormat,
		"create database testDB",
		"create table testDB.test1(id int primary key, a int unique key not null)",
	)
	tableInfo := pm.GetTableInfo("testDB", "test1")
	tableID := tableInfo.ID
	mounter := NewTxnMounter(schema)
	plr := pm.CreatePuller(0, []util.Span{util.GetTableSpan(tableID, false)})

	pm.MustExec("insert into testDB.test1 values(777,888)")
	rawTxn := getFirstRealTxn(ctx, c, plr)
	t, err := mounter.Mount(rawTxn)
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, model.Txn{
		Ts: rawTxn.Entries[0].Ts,
		DMLs: []*model.DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.InsertDMLType,
				Values: map[string]types.Datum{
					"id": types.NewIntDatum(777),
					"a":  types.NewIntDatum(888),
				},
			},
		},
	})

	pm.MustExec("update testDB.test1 set id = 999 where a = 888")
	rawTxn = getFirstRealTxn(ctx, c, plr)
	t, err = mounter.Mount(rawTxn)
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, model.Txn{
		Ts: rawTxn.Entries[0].Ts,
		DMLs: []*model.DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.DeleteDMLType,
				Values: map[string]types.Datum{
					"id": types.NewIntDatum(777),
				},
			},
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.InsertDMLType,
				Values: map[string]types.Datum{
					"id": types.NewIntDatum(999),
					"a":  types.NewIntDatum(888),
				},
			},
		},
	})

	pm.MustExec("delete from testDB.test1 where id = 999")
	rawTxn = getFirstRealTxn(ctx, c, plr)
	t, err = mounter.Mount(rawTxn)
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, model.Txn{
		Ts: rawTxn.Entries[0].Ts,
		DMLs: []*model.DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.DeleteDMLType,
				Values: map[string]types.Datum{
					"id": types.NewIntDatum(999),
				},
			},
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.DeleteDMLType,
				Values: map[string]types.Datum{
					"a": types.NewIntDatum(888),
				},
			},
		},
	})
}

func (cs *mountTxnsSuite) testUk(c *check.C, newRowFormat bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm, schema := setUpPullerAndSchema(ctx, c, newRowFormat,
		"create database testDB",
		`create table testDB.test1(
			a int unique key not null,
			b int unique key,
			c int not null,
			d int not null,
			e int not null,
			f int,
			UNIQUE (c, d),
			UNIQUE (e, f))`,
	)
	tableInfo := pm.GetTableInfo("testDB", "test1")
	tableID := tableInfo.ID
	mounter := NewTxnMounter(schema)
	plr := pm.CreatePuller(0, []util.Span{util.GetTableSpan(tableID, false)})

	pm.MustExec("insert into testDB.test1 values(1, 2, 3, 4, 5, 6)")
	rawTxn := getFirstRealTxn(ctx, c, plr)
	t, err := mounter.Mount(rawTxn)
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, model.Txn{
		Ts: rawTxn.Entries[0].Ts,
		DMLs: []*model.DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.InsertDMLType,
				Values: map[string]types.Datum{
					"a": types.NewIntDatum(1),
					"b": types.NewIntDatum(2),
					"c": types.NewIntDatum(3),
					"d": types.NewIntDatum(4),
					"e": types.NewIntDatum(5),
					"f": types.NewIntDatum(6),
				},
			},
		},
	})

	pm.MustExec("update testDB.test1 set a = 11, b = 22, c = 33, d = 44, e = 55, f = 66 where f = 6")
	rawTxn = getFirstRealTxn(ctx, c, plr)
	t, err = mounter.Mount(rawTxn)
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, model.Txn{
		Ts: rawTxn.Entries[0].Ts,
		DMLs: []*model.DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.DeleteDMLType,
				Values: map[string]types.Datum{
					"a": types.NewIntDatum(1),
				},
			},
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.DeleteDMLType,
				Values: map[string]types.Datum{
					"c": types.NewIntDatum(3),
					"d": types.NewIntDatum(4),
				},
			},
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.InsertDMLType,
				Values: map[string]types.Datum{
					"a": types.NewIntDatum(11),
					"b": types.NewIntDatum(22),
					"c": types.NewIntDatum(33),
					"d": types.NewIntDatum(44),
					"e": types.NewIntDatum(55),
					"f": types.NewIntDatum(66),
				},
			},
		},
	})

	pm.MustExec("delete from testDB.test1 where a = 11")
	rawTxn = getFirstRealTxn(ctx, c, plr)
	t, err = mounter.Mount(rawTxn)
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, model.Txn{
		Ts: rawTxn.Entries[0].Ts,
		DMLs: []*model.DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.DeleteDMLType,
				Values: map[string]types.Datum{
					"a": types.NewIntDatum(11),
				},
			},
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.DeleteDMLType,
				Values: map[string]types.Datum{
					"c": types.NewIntDatum(33),
					"d": types.NewIntDatum(44),
				},
			},
		},
	})
}

func (cs *mountTxnsSuite) testLargeInteger(c *check.C, newRowFormat bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm, schema := setUpPullerAndSchema(ctx, c, newRowFormat,
		"create database testDB",
		"CREATE TABLE testDB.large_int(id BIGINT UNSIGNED PRIMARY KEY, a int)",
	)
	tableInfo := pm.GetTableInfo("testDB", "large_int")
	tableID := tableInfo.ID
	mounter := NewTxnMounter(schema)
	plr := pm.CreatePuller(0, []util.Span{util.GetTableSpan(tableID, false)})

	pm.MustExec("insert into testDB.large_int values(?, ?)", uint64(math.MaxUint64), 123)
	rawTxn := getFirstRealTxn(ctx, c, plr)
	t, err := mounter.Mount(rawTxn)
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, model.Txn{
		Ts: rawTxn.Entries[0].Ts,
		DMLs: []*model.DML{
			{
				Database: "testDB",
				Table:    "large_int",
				Tp:       model.InsertDMLType,
				Values: map[string]types.Datum{
					"id": types.NewUintDatum(uint64(math.MaxUint64)),
					"a":  types.NewIntDatum(123),
				},
			},
		},
	})

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	pm, schema = setUpPullerAndSchema(ctx, c, newRowFormat,
		"create database testDB",
		"CREATE TABLE testDB.large_int(id BIGINT PRIMARY KEY, a int)",
	)
	tableInfo = pm.GetTableInfo("testDB", "large_int")
	tableID = tableInfo.ID
	mounter = NewTxnMounter(schema)
	plr = pm.CreatePuller(0, []util.Span{util.GetTableSpan(tableID, false)})

	pm.MustExec("insert into testDB.large_int values(?, ?)", int64(math.MinInt64), 123)
	rawTxn = getFirstRealTxn(ctx, c, plr)
	t, err = mounter.Mount(rawTxn)
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, model.Txn{
		Ts: rawTxn.Entries[0].Ts,
		DMLs: []*model.DML{
			{
				Database: "testDB",
				Table:    "large_int",
				Tp:       model.InsertDMLType,
				Values: map[string]types.Datum{
					"id": types.NewIntDatum(int64(math.MinInt64)),
					"a":  types.NewIntDatum(123),
				},
			},
		},
	})

}

func (cs *mountTxnsSuite) TestIncompleteRow(c *check.C) {
	cs.testIncompleteRow(c, true)
	cs.testIncompleteRow(c, false)
}
func (cs *mountTxnsSuite) TestInsertPkIsHandle(c *check.C) {
	cs.testInsertPkIsHandle(c, true)
	cs.testInsertPkIsHandle(c, false)
}
func (cs *mountTxnsSuite) TestUk(c *check.C) {
	cs.testUk(c, true)
	cs.testUk(c, false)
}
func (cs *mountTxnsSuite) TestLargeInteger(c *check.C) {
	cs.testLargeInteger(c, true)
	cs.testLargeInteger(c, false)
}


*/

func (cs *mountTxnsSuite) TestInsertPkNotHandle(c *check.C) {
	//cs.testInsertPkNotHandle(c, true)
	cs.testInsertPkNotHandle(c, false)
}

func (cs *mountTxnsSuite) assertRowEquals(c *check.C, obtainedFunc func() *model.RowChangedEvent, expectedRows []*model.RowChangedEvent) {
	for range expectedRows {
		obtained := obtainedFunc()
		if obtained != nil {
			obtained.Ts = 0
		}
		match := false
		for _, row := range expectedRows {
			if reflect.DeepEqual(obtained, row) {
				match = true
				break
			}
		}
		if !match {
			c.Fatalf("obtained Row %s isn't contained by expected Row", obtained)
		}
	}
}
