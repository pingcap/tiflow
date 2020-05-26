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
	"math"
	"reflect"
	"sync"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/types"
)

type mountTxnsSuite struct{}

var _ = check.Suite(&mountTxnsSuite{})

func setUpPullerAndSchema(ctx context.Context, c *check.C, newRowFormat bool, sqls ...string) (*puller.MockPullerManager, *Storage) {
	pm := puller.NewMockPullerManager(c, newRowFormat)
	for _, sql := range sqls {
		pm.MustExec(sql)
	}
	ddlPlr := pm.CreatePuller(0, []regionspan.Span{regionspan.GetDDLSpan()})
	go func() {
		err := ddlPlr.Run(ctx)
		if err != nil && errors.Cause(err) != context.Canceled {
			c.Fail()
		}
	}()

	jobs := pm.GetDDLJobs()
	schemaBuilder,err := NewStorageBuilder(jobs, ddlPlr.SortedOutput(ctx))
	c.Assert(err, check.IsNil)
	schemaStorage := schemaBuilder.Build(jobs[len(jobs)-1].BinlogInfo.FinishedTS)
	err = schemaStorage.HandlePreviousDDLJobIfNeed(jobs[len(jobs)-1].BinlogInfo.FinishedTS)
	c.Assert(err, check.IsNil)
	return pm, schemaStorage
}

func getFirstRealTxn(ctx context.Context, c *check.C, plr puller.Puller) (result model.RawTxn) {
	ctx, cancel := context.WithCancel(ctx)
	var once sync.Once
	err := plr.CollectRawTxns(ctx, func(ctx context.Context, rawTxn model.RawTxn) error {
		if rawTxn.IsFake() {
			return nil
		}
		once.Do(func() {
			result = rawTxn
		})
		cancel()
		return nil
	})
	c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	return
}

func (cs *mountTxnsSuite) testInsertPkNotHandle(c *check.C, newRowFormat bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm, schema := setUpPullerAndSchema(ctx, c, newRowFormat,
		"create database testDB",
		"create table testDB.test1(id varchar(255) primary key, a int, index ci (a))",
	)
	tableInfo := pm.GetTableInfo("testDB", "test1")
	tableID := tableInfo.ID
	mounter := NewTxnMounter(schema)
	plr := pm.CreatePuller(0, []regionspan.Span{regionspan.GetTableSpan(tableID, false)})

	pm.MustExec("insert into testDB.test1 values('ttt',6)")
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
					"id": types.NewBytesDatum([]byte("ttt")),
					"a":  types.NewIntDatum(6),
				},
			},
		},
	})

	pm.MustExec("update testDB.test1 set id = 'vvv' where a = 6")
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
					"id": types.NewBytesDatum([]byte("ttt")),
				},
			},
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.InsertDMLType,
				Values: map[string]types.Datum{
					"id": types.NewBytesDatum([]byte("vvv")),
					"a":  types.NewIntDatum(6),
				},
			},
		},
	})

	pm.MustExec("delete from testDB.test1 where a = 6")
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
					"id": types.NewBytesDatum([]byte("vvv")),
				},
			},
		},
	})
}

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
	plr := pm.CreatePuller(0, []regionspan.Span{regionspan.GetTableSpan(tableID, false)})

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
	plr := pm.CreatePuller(0, []regionspan.Span{regionspan.GetTableSpan(tableID, false)})

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
	plr := pm.CreatePuller(0, []regionspan.Span{regionspan.GetTableSpan(tableID, false)})

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
	plr := pm.CreatePuller(0, []regionspan.Span{regionspan.GetTableSpan(tableID, false)})

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
	plr = pm.CreatePuller(0, []regionspan.Span{regionspan.GetTableSpan(tableID, false)})

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

func (cs *mountTxnsSuite) TestInsertPkNotHandle(c *check.C) {
	cs.testInsertPkNotHandle(c, true)
	cs.testInsertPkNotHandle(c, false)
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

func (cs *mountTxnsSuite) assertTableTxnEquals(c *check.C,
	obtained, expected model.Txn) {
	obtainedDMLs := obtained.DMLs
	expectedDMLs := expected.DMLs
	obtained.DMLs = nil
	expected.DMLs = nil
	c.Assert(obtained, check.DeepEquals, expected)
	assertContain := func(obtained []*model.DML, expected []*model.DML) {
		c.Assert(len(obtained), check.Equals, len(expected))
		for _, oDML := range obtained {
			match := false
			for _, eDML := range expected {
				if reflect.DeepEqual(oDML, eDML) {
					match = true
					break
				}
			}
			if !match {
				c.Errorf("obtained DML %#v isn't contained by expected DML", oDML)
			}
		}
	}
	assertContain(obtainedDMLs, expectedDMLs)
}
*/
