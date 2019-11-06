// Copyright 2019 PingCAP, Inc.
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

package txn

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-cdc/cdc/entry"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/mock"
	"github.com/pingcap/tidb-cdc/cdc/schema"
	"github.com/pingcap/tidb-cdc/pkg/util"
	"github.com/pingcap/tidb/types"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

type CollectRawTxnsSuite struct{}

type mockTracker struct {
	forwarded []bool
	cur       int
}

func (t *mockTracker) Forward(span util.Span, ts uint64) bool {
	if len(t.forwarded) > 0 {
		r := t.forwarded[t.cur]
		t.cur++
		return r
	}
	return true
}

func (t *mockTracker) Frontier() uint64 {
	return 1
}

var _ = check.Suite(&CollectRawTxnsSuite{})

func (cs *CollectRawTxnsSuite) TestShouldOutputTxnsInOrder(c *check.C) {
	var entries []kv.KvOrResolved
	var startTs uint64 = 1024
	var i uint64
	for i = 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			e := kv.KvOrResolved{
				KV: &kv.RawKVEntry{
					OpType: kv.OpTypePut,
					Key:    []byte(fmt.Sprintf("key-%d-%d", i, j)),
					Ts:     startTs + i,
				},
			}
			entries = append(entries, e)
		}
	}
	// Only add resolved entry for the first 2 transaction
	for i = 0; i < 2; i++ {
		e := kv.KvOrResolved{
			Resolved: &kv.ResolvedSpan{Timestamp: startTs + i},
		}
		entries = append(entries, e)
	}

	nRead := 0
	input := func(ctx context.Context) (kv.KvOrResolved, error) {
		if nRead >= len(entries) {
			return kv.KvOrResolved{}, errors.New("End")
		}
		e := entries[nRead]
		nRead++
		return e, nil
	}

	var rawTxns []RawTxn
	output := func(ctx context.Context, txn RawTxn) error {
		rawTxns = append(rawTxns, txn)
		return nil
	}

	ctx := context.Background()
	err := CollectRawTxns(ctx, input, output, &mockTracker{})
	c.Assert(err, check.ErrorMatches, "End")

	c.Assert(rawTxns, check.HasLen, 2)
	c.Assert(rawTxns[0].Ts, check.Equals, startTs)
	for i, e := range rawTxns[0].Entries {
		c.Assert(e.Ts, check.Equals, startTs)
		c.Assert(string(e.Key), check.Equals, fmt.Sprintf("key-0-%d", i))
	}
	c.Assert(rawTxns[1].Ts, check.Equals, startTs+1)
	for i, e := range rawTxns[1].Entries {
		c.Assert(e.Ts, check.Equals, startTs+1)
		c.Assert(string(e.Key), check.Equals, fmt.Sprintf("key-1-%d", i))
	}
}

func (cs *CollectRawTxnsSuite) TestShouldConsiderSpanResolvedTs(c *check.C) {
	var entries []kv.KvOrResolved
	for _, v := range []struct {
		key          []byte
		ts           uint64
		isResolvedTs bool
	}{
		{key: []byte("key1-1"), ts: 1},
		{key: []byte("key2-1"), ts: 2},
		{key: []byte("key1-2"), ts: 1},
		{key: []byte("key1-3"), ts: 1},
		{ts: 1, isResolvedTs: true},
		{ts: 2, isResolvedTs: true},
		{key: []byte("key2-1"), ts: 2},
		{ts: 1, isResolvedTs: true},
	} {
		var e kv.KvOrResolved
		if v.isResolvedTs {
			e = kv.KvOrResolved{
				Resolved: &kv.ResolvedSpan{Timestamp: v.ts},
			}
		} else {
			e = kv.KvOrResolved{
				KV: &kv.RawKVEntry{
					OpType: kv.OpTypePut,
					Key:    v.key,
					Ts:     v.ts,
				},
			}
		}
		entries = append(entries, e)
	}

	cursor := 0
	input := func(ctx context.Context) (kv.KvOrResolved, error) {
		if cursor >= len(entries) {
			return kv.KvOrResolved{}, errors.New("End")
		}
		e := entries[cursor]
		cursor++
		return e, nil
	}

	var rawTxns []RawTxn
	output := func(ctx context.Context, txn RawTxn) error {
		rawTxns = append(rawTxns, txn)
		return nil
	}

	ctx := context.Background()
	// Set up the tracker so that only the last resolve event forwards the global minimum Ts
	tracker := mockTracker{forwarded: []bool{false, false, true}}
	err := CollectRawTxns(ctx, input, output, &tracker)
	c.Assert(err, check.ErrorMatches, "End")

	c.Assert(rawTxns, check.HasLen, 1)
	txn := rawTxns[0]
	c.Assert(txn.Ts, check.Equals, uint64(1))
	c.Assert(txn.Entries, check.HasLen, 3)
	c.Assert(string(txn.Entries[0].Key), check.Equals, "key1-1")
	c.Assert(string(txn.Entries[1].Key), check.Equals, "key1-2")
	c.Assert(string(txn.Entries[2].Key), check.Equals, "key1-3")
}

func (cs *CollectRawTxnsSuite) TestShouldOutputBinlogEvenWhenThereIsNoRealEvent(c *check.C) {
	entries := []kv.KvOrResolved{
		{Resolved: &kv.ResolvedSpan{Timestamp: 1024}},
		{Resolved: &kv.ResolvedSpan{Timestamp: 2000}},
	}

	cursor := 0
	input := func(ctx context.Context) (kv.KvOrResolved, error) {
		if cursor >= len(entries) {
			return kv.KvOrResolved{}, errors.New("End")
		}
		e := entries[cursor]
		cursor++
		return e, nil
	}

	var rawTxns []RawTxn
	output := func(ctx context.Context, txn RawTxn) error {
		rawTxns = append(rawTxns, txn)
		return nil
	}

	ctx := context.Background()
	tracker := mockTracker{forwarded: []bool{true, true}}
	err := CollectRawTxns(ctx, input, output, &tracker)
	c.Assert(err, check.ErrorMatches, "End")

	c.Assert(rawTxns, check.HasLen, len(entries))
	for i, t := range rawTxns {
		c.Assert(t.Entries, check.HasLen, 0)
		c.Assert(t.Ts, check.Equals, entries[i].Resolved.Timestamp)
	}
}

type mountTxnsSuite struct{}

var _ = check.Suite(&mountTxnsSuite{})

func setUpPullerAndSchema(c *check.C, sqls ...string) (*mock.MockTiDB, *schema.Storage) {
	puller, err := mock.NewMockPuller()
	c.Assert(err, check.IsNil)
	var jobs []*model.Job

	for _, sql := range sqls {
		rawEntries := puller.MustExec(c, sql)
		for _, raw := range rawEntries {
			e, err := entry.Unmarshal(raw)
			c.Assert(err, check.IsNil)
			switch e := e.(type) {
			case *entry.DDLJobKVEntry:
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
	mounter, err := NewTxnMounter(schema, time.UTC)
	c.Assert(err, check.IsNil)

	rawKV := puller.MustExec(c, "insert into testDB.test1 values('ttt',6)")
	txn, err := mounter.Mount(RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, txn, &Txn{
		Ts: rawKV[0].Ts,
		DMLs: []*DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       DeleteDMLType,
				Values: map[string]types.Datum{
					"id": types.NewBytesDatum([]byte("ttt")),
				},
			},
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       InsertDMLType,
				Values: map[string]types.Datum{
					"id": types.NewBytesDatum([]byte("ttt")),
					"a":  types.NewIntDatum(6),
				},
			},
		},
	})

	rawKV = puller.MustExec(c, "update testDB.test1 set id = 'vvv' where a = 6")
	txn, err = mounter.Mount(RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, txn, &Txn{
		Ts: rawKV[0].Ts,
		DMLs: []*DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       DeleteDMLType,
				Values: map[string]types.Datum{
					"id": types.NewBytesDatum([]byte("vvv")),
				},
			},
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       DeleteDMLType,
				Values: map[string]types.Datum{
					"id": types.NewBytesDatum([]byte("ttt")),
				},
			},
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       InsertDMLType,
				Values: map[string]types.Datum{
					"id": types.NewBytesDatum([]byte("vvv")),
					"a":  types.NewIntDatum(6),
				},
			},
		},
	})

	rawKV = puller.MustExec(c, "delete from testDB.test1 where a = 6")
	txn, err = mounter.Mount(RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, txn, &Txn{
		Ts: rawKV[0].Ts,
		DMLs: []*DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       DeleteDMLType,
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
	mounter, err := NewTxnMounter(schema, time.UTC)
	c.Assert(err, check.IsNil)

	rawKV := puller.MustExec(c, "insert into testDB.test1 values(777,888)")
	txn, err := mounter.Mount(RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, txn, &Txn{
		Ts: rawKV[0].Ts,
		DMLs: []*DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       DeleteDMLType,
				Values: map[string]types.Datum{
					"a": types.NewIntDatum(888),
				},
			},
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       InsertDMLType,
				Values: map[string]types.Datum{
					"id": types.NewIntDatum(777),
					"a":  types.NewIntDatum(888),
				},
			},
		},
	})

	rawKV = puller.MustExec(c, "update testDB.test1 set id = 999 where a = 888")
	txn, err = mounter.Mount(RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, txn, &Txn{
		Ts: rawKV[0].Ts,
		DMLs: []*DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       DeleteDMLType,
				Values: map[string]types.Datum{
					"a": types.NewIntDatum(888),
				},
			},
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       DeleteDMLType,
				Values: map[string]types.Datum{
					"id": types.NewIntDatum(777),
				},
			},
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       InsertDMLType,
				Values: map[string]types.Datum{
					"id": types.NewIntDatum(999),
					"a":  types.NewIntDatum(888),
				},
			},
		},
	})

	rawKV = puller.MustExec(c, "delete from testDB.test1 where id = 999")
	txn, err = mounter.Mount(RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, txn, &Txn{
		Ts: rawKV[0].Ts,
		DMLs: []*DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       DeleteDMLType,
				Values: map[string]types.Datum{
					"id": types.NewIntDatum(999),
				},
			},
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       DeleteDMLType,
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
	mounter, err := NewTxnMounter(schema, time.UTC)
	c.Assert(err, check.IsNil)
	rawKV := puller.MustExec(c, "alter table testDB.test1 add b int null")
	txn, err := mounter.Mount(RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	c.Assert(txn, check.DeepEquals, &Txn{
		DDL: &DDL{
			Database: "testDB",
			Table:    "test1",
			Job:      &model.Job{},
		},
		Ts: rawKV[0].Ts,
	})

	// test insert null value
	rawKV = puller.MustExec(c, "insert into testDB.test1(id,a) values('ttt',6)")
	txn, err = mounter.Mount(RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, txn, &Txn{
		Ts: rawKV[0].Ts,
		DMLs: []*DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       DeleteDMLType,
				Values: map[string]types.Datum{
					"id": types.NewBytesDatum([]byte("ttt")),
				},
			},
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       InsertDMLType,
				Values: map[string]types.Datum{
					"id": types.NewBytesDatum([]byte("ttt")),
					"a":  types.NewIntDatum(6),
				},
			},
		},
	})

	rawKV = puller.MustExec(c, "insert into testDB.test1(id,a,b) values('kkk',6,7)")
	txn, err = mounter.Mount(RawTxn{
		Ts:      rawKV[0].Ts,
		Entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	cs.assertTableTxnEquals(c, txn, &Txn{
		Ts: rawKV[0].Ts,
		DMLs: []*DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       DeleteDMLType,
				Values: map[string]types.Datum{
					"id": types.NewBytesDatum([]byte("kkk")),
				},
			},
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       InsertDMLType,
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
	obtained, expected *Txn) {
	obtainedDMLs := obtained.DMLs
	expectedDMLs := expected.DMLs
	obtained.DMLs = nil
	expected.DMLs = nil
	c.Assert(obtained, check.DeepEquals, expected)
	assertContain := func(obtained []*DML, expected []*DML) {
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
