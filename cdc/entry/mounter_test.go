package entry

import (
	"context"
	"math"
	"reflect"
	"sync"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/cdc/schema"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/types"
)

type mountTxnsSuite struct{}

var _ = check.Suite(&mountTxnsSuite{})

func setUpPullerAndSchema(ctx context.Context, c *check.C, sqls ...string) (*puller.MockPullerManager, *schema.Storage) {
	pm := puller.NewMockPullerManager(c)
	go pm.Run(ctx)
	for _, sql := range sqls {
		pm.MustExec(sql)
	}

	jobs := pm.GetDDLJobs()
	schemaStorage, err := schema.NewStorage(jobs, false)
	c.Assert(err, check.IsNil)
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

func (cs *mountTxnsSuite) TestInsertPkNotHandle(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm, schema := setUpPullerAndSchema(ctx, c,
		"create database testDB",
		"create table testDB.test1(id varchar(255) primary key, a int, index ci (a))",
	)
	tableInfo := pm.GetTableInfo("testDB", "test1")
	tableID := tableInfo.ID
	mounter := NewTxnMounter(schema)
	plr := pm.CreatePuller(0, []util.Span{util.GetTableSpan(tableID, false)})

	pm.MustExec("insert into testDB.test1 values('ttt',6)")
	rawTxn := getFirstRealTxn(ctx, c, plr)
	t, err := mounter.Mount(rawTxn)
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, &model.Txn{
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
	cs.assertTableTxnEquals(c, t, &model.Txn{
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
	cs.assertTableTxnEquals(c, t, &model.Txn{
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

func (cs *mountTxnsSuite) TestInsertPkIsHandle(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm, schema := setUpPullerAndSchema(ctx, c,
		"create database testDB",
		"create table testDB.test1(id int primary key, a int unique key)",
	)
	tableInfo := pm.GetTableInfo("testDB", "test1")
	tableID := tableInfo.ID
	mounter := NewTxnMounter(schema)
	plr := pm.CreatePuller(0, []util.Span{util.GetTableSpan(tableID, false)})

	pm.MustExec("insert into testDB.test1 values(777,888)")
	rawTxn := getFirstRealTxn(ctx, c, plr)
	t, err := mounter.Mount(rawTxn)
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, &model.Txn{
		Ts: rawTxn.Entries[0].Ts,
		DMLs: []*model.DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.DeleteDMLType,
				Values: map[string]types.Datum{
					"a": types.NewIntDatum(888),
				},
			},
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
	cs.assertTableTxnEquals(c, t, &model.Txn{
		Ts: rawTxn.Entries[0].Ts,
		DMLs: []*model.DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.DeleteDMLType,
				Values: map[string]types.Datum{
					"a": types.NewIntDatum(888),
				},
			},
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
	cs.assertTableTxnEquals(c, t, &model.Txn{
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

func (cs *mountTxnsSuite) TestLargeInteger(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pm, schema := setUpPullerAndSchema(ctx, c,
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
	cs.assertTableTxnEquals(c, t, &model.Txn{
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
	pm, schema = setUpPullerAndSchema(ctx, c,
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
	cs.assertTableTxnEquals(c, t, &model.Txn{
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

func (cs *mountTxnsSuite) assertTableTxnEquals(c *check.C,
	obtained, expected *model.Txn) {
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
