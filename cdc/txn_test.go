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

package cdc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-cdc/cdc/entry"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/mock"
	"github.com/pingcap/tidb/types"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

type CollectRawTxnsSuite struct{}

var _ = check.Suite(&CollectRawTxnsSuite{})

func (cs *CollectRawTxnsSuite) TestShouldOutputTxnsInOrder(c *check.C) {
	var entries []BufferEntry
	var startTs uint64 = 1024
	var i uint64
	for i = 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			e := BufferEntry{
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
		e := BufferEntry{
			Resolved: &ResolvedSpan{Timestamp: startTs + i},
		}
		entries = append(entries, e)
	}

	cursor := 0
	input := func(ctx context.Context) (BufferEntry, error) {
		if cursor >= len(entries) {
			return BufferEntry{}, errors.New("End")
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
	err := collectRawTxns(ctx, input, output)
	c.Assert(err, check.ErrorMatches, "End")

	c.Assert(rawTxns, check.HasLen, 2)
	c.Assert(rawTxns[0].ts, check.Equals, startTs)
	for i, e := range rawTxns[0].entries {
		c.Assert(e.Ts, check.Equals, startTs)
		c.Assert(string(e.Key), check.Equals, fmt.Sprintf("key-0-%d", i))
	}
	c.Assert(rawTxns[1].ts, check.Equals, startTs+1)
	for i, e := range rawTxns[1].entries {
		c.Assert(e.Ts, check.Equals, startTs+1)
		c.Assert(string(e.Key), check.Equals, fmt.Sprintf("key-1-%d", i))
	}
}

type mountTxnsSuite struct{}

var _ = check.Suite(&mountTxnsSuite{})

func setUpPullerAndSchema(c *check.C, sqls ...string) (*mock.MockTiDB, *Schema) {
	puller, err := mock.NewMockPuller(c)
	c.Assert(err, check.IsNil)
	var jobs []*model.Job

	for _, sql := range sqls {
		rawEntries := puller.MustExec(sql)
		for _, raw := range rawEntries {
			e, err := entry.Unmarshal(raw)
			c.Assert(err, check.IsNil)
			switch e := e.(type) {
			case *entry.DDLJobHistoryKVEntry:
				jobs = append(jobs, e.Job)
			}
		}
	}
	c.Assert(len(jobs), check.Equals, len(sqls))
	schema, err := NewSchema(jobs, false)
	c.Assert(err, check.IsNil)
	err = schema.handlePreviousDDLJobIfNeed(jobs[len(jobs)-1].BinlogInfo.SchemaVersion)
	c.Assert(err, check.IsNil)
	return puller, schema
}

func (cs *mountTxnsSuite) TestInsertPkNotHandle(c *check.C) {
	puller, schema := setUpPullerAndSchema(c, "create database testDB", "create table testDB.test1(id varchar(255) primary key, a int, index ci (a))")
	tableId, exist := schema.GetTableIDByName("testDB", "test1")
	c.Assert(exist, check.IsTrue)
	mounter, err := NewTxnMounter(schema, tableId, time.UTC)
	c.Assert(err, check.IsNil)

	rawKV := puller.MustExec("insert into testDB.test1 values('ttt',6)")
	txn, err := mounter.Mount(&RawTxn{
		ts:      rawKV[0].Ts,
		entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	c.Assert(txn, check.DeepEquals, &TableTxn{
		Ts: rawKV[0].Ts,
		replaceDMLs: []*DML{
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
		deleteDMLs: []*DML{
			{
				Database: "testDB",
				Table:    "test1",
				Tp:       DeleteDMLType,
				Values: map[string]types.Datum{
					"id": types.NewBytesDatum([]byte("ttt")),
				},
			},
		},
	})

	rawKV = puller.MustExec("update testDB.test1 set id = 'vvv' where a = 6")
	txn, err = mounter.Mount(&RawTxn{
		ts:      rawKV[0].Ts,
		entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	c.Assert(txn, check.DeepEquals, &TableTxn{
		Ts: rawKV[0].Ts,
		replaceDMLs: []*DML{
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
		deleteDMLs: []*DML{
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
		},
	})

	rawKV = puller.MustExec("delete from testDB.test1 where a = 6")
	txn, err = mounter.Mount(&RawTxn{
		ts:      rawKV[0].Ts,
		entries: rawKV,
	})
	c.Assert(err, check.IsNil)
	c.Assert(txn, check.DeepEquals, &TableTxn{
		Ts: rawKV[0].Ts,
		deleteDMLs: []*DML{
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
