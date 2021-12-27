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

package syncer

import (
	"math"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/util/mock"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

func (s *testSyncerSuite) TestDetectConflict(c *C) {
	ca := &causality{
		relation: newCausalityRelation(),
	}
	caseData := []string{"test_1", "test_2", "test_3"}
	excepted := map[string]string{
		"test_1": "test_1",
		"test_2": "test_1",
		"test_3": "test_1",
	}

	assertRelationsEq := func(expectMap map[string]string) {
		c.Assert(ca.relation.len(), Equals, len(expectMap))
		for k, expV := range expectMap {
			v, ok := ca.relation.get(k)
			c.Assert(ok, IsTrue)
			c.Assert(v, Equals, expV)
		}
	}

	c.Assert(ca.detectConflict(caseData), IsFalse)
	ca.add(caseData)
	assertRelationsEq(excepted)
	c.Assert(ca.detectConflict([]string{"test_4"}), IsFalse)
	ca.add([]string{"test_4"})
	excepted["test_4"] = "test_4"
	assertRelationsEq(excepted)
	conflictData := []string{"test_4", "test_3"}
	c.Assert(ca.detectConflict(conflictData), IsTrue)
	ca.relation.clear()
	c.Assert(ca.relation.len(), Equals, 0)
}

func (s *testSyncerSuite) TestCasuality(c *C) {
	p := parser.New()
	se := mock.NewContext()
	schemaStr := "create table tb(a int primary key, b int unique);"
	ti, err := createTableInfo(p, se, int64(0), schemaStr)
	c.Assert(err, IsNil)
	tiIndex := &model.IndexInfo{
		Table:   ti.Name,
		Unique:  true,
		Primary: true,
		State:   model.StatePublic,
		Tp:      model.IndexTypeBtree,
		Columns: []*model.IndexColumn{{
			Name:   ti.Columns[0].Name,
			Offset: ti.Columns[0].Offset,
			Length: types.UnspecifiedLength,
		}},
	}
	downTi := schema.GetDownStreamTi(ti, ti)
	c.Assert(downTi, NotNil)

	jobCh := make(chan *job, 10)
	syncer := &Syncer{
		cfg: &config.SubTaskConfig{
			SyncerConfig: config.SyncerConfig{
				QueueSize: 1024,
			},
			Name:     "task",
			SourceID: "source",
		},
		tctx:    tcontext.Background().WithLogger(log.L()),
		sessCtx: utils.NewSessionCtx(map[string]string{"time_zone": "UTC"}),
	}
	causalityCh := causalityWrap(jobCh, syncer)
	testCases := []struct {
		op      opType
		oldVals []interface{}
		vals    []interface{}
	}{
		{
			op:   insert,
			vals: []interface{}{1, 2},
		},
		{
			op:   insert,
			vals: []interface{}{2, 3},
		},
		{
			op:      update,
			oldVals: []interface{}{2, 3},
			vals:    []interface{}{3, 4},
		},
		{
			op:   del,
			vals: []interface{}{1, 2},
		},
		{
			op:   insert,
			vals: []interface{}{1, 3},
		},
	}
	results := []opType{insert, insert, update, del, conflict, insert}
	table := &filter.Table{Schema: "test", Name: "t1"}
	location := binlog.NewLocation("")
	ec := &eventContext{startLocation: &location, currentLocation: &location, lastLocation: &location}

	for _, tc := range testCases {
		job := newDMLJob(tc.op, table, table, newDML(tc.op, false, "", table, tc.oldVals, tc.vals, tc.oldVals, tc.vals, ti.Columns, ti, tiIndex, downTi), ec)
		jobCh <- job
	}

	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return len(causalityCh) == len(results)
	}), IsTrue)

	for _, op := range results {
		job := <-causalityCh
		c.Assert(job.tp, Equals, op)
	}
}

func (s *testSyncerSuite) TestCasualityWithPrefixIndex(c *C) {
	p := parser.New()
	se := mock.NewContext()
	schemaStr := "create table t (c1 text, c2 int unique, unique key c1(c1(3)));"
	ti, err := createTableInfo(p, se, int64(0), schemaStr)
	c.Assert(err, IsNil)
	downTi := schema.GetDownStreamTi(ti, ti)
	c.Assert(downTi, NotNil)
	c.Assert(len(downTi.AvailableUKIndexList) == 2, IsTrue)
	tiIndex := downTi.AvailableUKIndexList[0]

	jobCh := make(chan *job, 10)
	syncer := &Syncer{
		cfg: &config.SubTaskConfig{
			SyncerConfig: config.SyncerConfig{
				QueueSize: 1024,
			},
			Name:     "task",
			SourceID: "source",
		},
		tctx:    tcontext.Background().WithLogger(log.L()),
		sessCtx: utils.NewSessionCtx(map[string]string{"time_zone": "UTC"}),
	}
	causalityCh := causalityWrap(jobCh, syncer)
	testCases := []struct {
		op      opType
		oldVals []interface{}
		vals    []interface{}
	}{
		{
			op:   insert,
			vals: []interface{}{"1234", 1},
		},
		{
			op:   insert,
			vals: []interface{}{"2345", 2},
		},
		{
			op:      update,
			oldVals: []interface{}{"2345", 2},
			vals:    []interface{}{"2345", 3},
		},
		{
			op:   del,
			vals: []interface{}{"1234", 1},
		},
		{
			op:   insert,
			vals: []interface{}{"2345", 1},
		},
	}
	results := []opType{insert, insert, update, del, conflict, insert}
	resultKeys := []string{"123.c1.", "234.c1.", "234.c1.", "123.c1.", "conflict", "234.c1."}
	table := &filter.Table{Schema: "test", Name: "t1"}
	location := binlog.NewLocation("")
	ec := &eventContext{startLocation: &location, currentLocation: &location, lastLocation: &location}

	for _, tc := range testCases {
		job := newDMLJob(tc.op, table, table, newDML(tc.op, false, "", table, tc.oldVals, tc.vals, tc.oldVals, tc.vals, ti.Columns, ti, tiIndex, downTi), ec)
		jobCh <- job
	}

	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return len(causalityCh) == len(results)
	}), IsTrue)

	for i, op := range results {
		job := <-causalityCh
		if job.tp != conflict {
			c.Assert(job.dml.key, Equals, resultKeys[i])
		}
		c.Assert(job.tp, Equals, op)
	}
}

func (s *testSyncerSuite) TestCasualityRelation(c *C) {
	rm := newCausalityRelation()
	c.Assert(rm.len(), Equals, 0)
	c.Assert(len(rm.groups), Equals, 1)

	testCases := []struct {
		key string
		val string
	}{
		{key: "1.key", val: "1.val"},
		{key: "2.key", val: "2.val"},
		{key: "3.key", val: "3.val"},
		{key: "4.key", val: "4.val"},
		{key: "5.key", val: "5.val"},
		{key: "6.key", val: "6.val"},
		{key: "7.key", val: "7.val"},
		{key: "8.key", val: "8.val"},
		{key: "9.key", val: "9.val"},
		{key: "10.key", val: "10.val"},
		{key: "11.key", val: "11.val"},
	}

	// test without rotate
	for _, testcase := range testCases {
		rm.set(testcase.key, testcase.val)
	}

	c.Assert(rm.len(), Equals, len(testCases))

	for _, testcase := range testCases {
		val, ok := rm.get(testcase.key)
		c.Assert(ok, Equals, true)
		c.Assert(val, Equals, testcase.val)
	}

	rm.rotate(1)
	rm.gc(1)
	c.Assert(rm.len(), Equals, 0)

	// test gc max
	for _, testcase := range testCases {
		rm.set(testcase.key, testcase.val)
	}

	rm.gc(math.MaxInt64)
	c.Assert(rm.len(), Equals, 0)

	// test with rotate
	for index, testcase := range testCases {
		rm.set(testcase.key, testcase.val)
		rm.rotate(int64(index))
	}

	c.Assert(rm.len(), Equals, len(testCases))

	for _, testcase := range testCases {
		val, ok := rm.get(testcase.key)
		c.Assert(ok, Equals, true)
		c.Assert(val, Equals, testcase.val)
	}

	for index := range testCases {
		rm.gc(int64(index))

		for _, rmMap := range rm.groups[1:] {
			c.Assert(rmMap.prevFlushJobSeq, Not(Equals), int64(index))
		}

		for ti := 0; ti < index; ti++ {
			_, ok := rm.get(testCases[ti].key)
			c.Assert(ok, Equals, false)
		}
	}

	rm.clear()
	c.Assert(rm.len(), Equals, 0)
}
