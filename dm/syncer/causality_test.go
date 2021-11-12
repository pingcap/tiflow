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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/util/mock"

	"github.com/pingcap/ticdc/dm/dm/config"
	"github.com/pingcap/ticdc/dm/pkg/binlog"
	tcontext "github.com/pingcap/ticdc/dm/pkg/context"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"github.com/pingcap/ticdc/dm/pkg/schema"
	"github.com/pingcap/ticdc/dm/pkg/utils"
)

func (s *testSyncerSuite) TestDetectConflict(c *C) {
	ca := &causality{
		relations: make(map[string]string),
	}
	caseData := []string{"test_1", "test_2", "test_3"}
	excepted := map[string]string{
		"test_1": "test_1",
		"test_2": "test_1",
		"test_3": "test_1",
	}
	c.Assert(ca.detectConflict(caseData), IsFalse)
	ca.add(caseData)
	c.Assert(ca.relations, DeepEquals, excepted)
	c.Assert(ca.detectConflict([]string{"test_4"}), IsFalse)
	ca.add([]string{"test_4"})
	excepted["test_4"] = "test_4"
	c.Assert(ca.relations, DeepEquals, excepted)
	conflictData := []string{"test_4", "test_3"}
	c.Assert(ca.detectConflict(conflictData), IsTrue)
	ca.reset()
	c.Assert(ca.relations, HasLen, 0)
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
		tctx: tcontext.Background().WithLogger(log.L()),
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
	ec := &eventContext{curStartLocation: location, curEndLocation: location, txnEndLocation: location}

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
