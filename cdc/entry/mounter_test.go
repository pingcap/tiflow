package entry

import (
	"reflect"
	"time"

	"github.com/pingcap/check"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/mock"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/schema"
	"github.com/pingcap/tidb/types"
)

type mountTxnsSuite struct{}

var _ = check.Suite(&mountTxnsSuite{})

func setUpPullerAndSchema(c *check.C, sqls ...string) (*mock.TiDB, *schema.Storage) {
	puller, err := mock.NewMockPuller()
	c.Assert(err, check.IsNil)
	var jobs []*timodel.Job

	for _, sql := range sqls {
		rawEntries := puller.MustExec(c, sql)
		for _, raw := range rawEntries {
			e, err := unmarshal(raw)
			c.Assert(err, check.IsNil)
			switch e := e.(type) {
			case *ddlJobKVEntry:
				jobs = append(jobs, e.Job)
			}
		}
	}
	c.Assert(len(jobs), check.Equals, len(sqls))
	schemaStorage, err := schema.NewStorage(jobs, false)
	c.Assert(err, check.IsNil)
	err = schemaStorage.HandlePreviousDDLJobIfNeed(jobs[len(jobs)-1].BinlogInfo.FinishedTS)
	c.Assert(err, check.IsNil)
	return puller, schemaStorage
}

func (cs *mountTxnsSuite) TestInsertPkNotHandle(c *check.C) {
	c.Skip("DDL is undetectable now in unit test environment")
	puller, schema := setUpPullerAndSchema(c, "create database testDB", "create table testDB.test1(id varchar(255) primary key, a int, index ci (a))")
	mounter := NewTxnMounter(schema, time.UTC)

	rawKV := puller.MustExec(c, "insert into testDB.test1 values('ttt',6)")
	t, err := mounter.Mount(model.RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, &model.Txn{
		Ts: rawKV[0].Ts,
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

	rawKV = puller.MustExec(c, "update testDB.test1 set id = 'vvv' where a = 6")
	t, err = mounter.Mount(model.RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, &model.Txn{
		Ts: rawKV[0].Ts,
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

	rawKV = puller.MustExec(c, "delete from testDB.test1 where a = 6")
	t, err = mounter.Mount(model.RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, &model.Txn{
		Ts: rawKV[0].Ts,
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
	c.Skip("DDL is undetectable now in unit test environment")
	puller, schema := setUpPullerAndSchema(c, "create database testDB", "create table testDB.test1(id int primary key, a int unique key)")
	mounter := NewTxnMounter(schema, time.UTC)

	rawKV := puller.MustExec(c, "insert into testDB.test1 values(777,888)")
	t, err := mounter.Mount(model.RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, &model.Txn{
		Ts: rawKV[0].Ts,
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

	rawKV = puller.MustExec(c, "update testDB.test1 set id = 999 where a = 888")
	t, err = mounter.Mount(model.RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, &model.Txn{
		Ts: rawKV[0].Ts,
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

	rawKV = puller.MustExec(c, "delete from testDB.test1 where id = 999")
	t, err = mounter.Mount(model.RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, &model.Txn{
		Ts: rawKV[0].Ts,
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

func (cs *mountTxnsSuite) TestDDL(c *check.C) {
	c.Skip("DDL is undetectable now in unit test environment")
	puller, schema := setUpPullerAndSchema(c, "create database testDB", "create table testDB.test1(id varchar(255) primary key, a int, index ci (a))")
	mounter := NewTxnMounter(schema, time.UTC)
	rawKV := puller.MustExec(c, "alter table testDB.test1 add b int null")
	t, err := mounter.Mount(model.RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	c.Assert(t, check.DeepEquals, &model.Txn{
		DDL: &model.DDL{
			Database: "testDB",
			Table:    "test1",
			Job:      &timodel.Job{},
		},
		Ts: rawKV[0].Ts,
	})

	// test insert null value
	rawKV = puller.MustExec(c, "insert into testDB.test1(id,a) values('ttt',6)")
	t, err = mounter.Mount(model.RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, &model.Txn{
		Ts: rawKV[0].Ts,
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

	rawKV = puller.MustExec(c, "insert into testDB.test1(id,a,b) values('kkk',6,7)")
	t, err = mounter.Mount(model.RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, t, &model.Txn{
		Ts: rawKV[0].Ts,
		DMLs: []*model.DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.DeleteDMLType,
				Values: map[string]types.Datum{
					"id": types.NewBytesDatum([]byte("kkk")),
				},
			},
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       model.InsertDMLType,
				Values: map[string]types.Datum{
					"id": types.NewBytesDatum([]byte("kkk")),
					"a":  types.NewIntDatum(6),
					"b":  types.NewIntDatum(7),
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
