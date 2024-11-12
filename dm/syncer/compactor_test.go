// Copyright 2021 PingCAP, Inc.
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
	"context"
	"math/rand"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/mock"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
)

// mockExecute mock a kv store.
func mockExecute(kv map[interface{}][]interface{}, dmls []*sqlmodel.RowChange) map[interface{}][]interface{} {
	for _, dml := range dmls {
		switch dml.Type() {
		case sqlmodel.RowChangeInsert:
			kv[dml.GetPostValues()[0]] = dml.GetPostValues()
		case sqlmodel.RowChangeUpdate:
			delete(kv, dml.GetPreValues()[0])
			kv[dml.GetPostValues()[0]] = dml.GetPostValues()
		case sqlmodel.RowChangeDelete:
			delete(kv, dml.GetPreValues()[0])
		}
	}

	return kv
}

func randString(n int) string {
	letter := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letter[rand.Intn(len(letter))]
	}
	return string(b)
}

func (s *testSyncerSuite) TestCompactJob(c *check.C) {
	compactor := &compactor{
		bufferSize:         10000,
		logger:             log.L(),
		keyMap:             make(map[string]map[string]int),
		buffer:             make([]*job, 0, 10000),
		updateJobMetricsFn: func(bool, string, *job) {},
	}

	location := binlog.MustZeroLocation(mysql.MySQLFlavor)
	ec := &eventContext{startLocation: location, endLocation: location, lastLocation: location}
	p := parser.New()
	se := mock.NewContext()
	sourceTable := &cdcmodel.TableName{Schema: "test", Table: "tb1"}
	targetTable := &cdcmodel.TableName{Schema: "test", Table: "tb"}
	schemaStr := "create table test.tb(id int primary key, col1 int, name varchar(24))"
	ti, err := createTableInfo(p, se, 0, schemaStr)
	c.Assert(err, check.IsNil)

	var dml *sqlmodel.RowChange
	var dmls []*sqlmodel.RowChange
	dmlNum := 1000000
	maxID := 1000
	batch := 1000
	updateIdentifyProbability := 0.1

	// generate DMLs
	kv := make(map[interface{}][]interface{})
	for i := 0; i < dmlNum; i++ {
		newID := rand.Intn(maxID)
		newCol1 := rand.Intn(maxID * 10)
		newName := randString(rand.Intn(20))
		values := []interface{}{newID, newCol1, newName}
		oldValues, ok := kv[newID]
		if !ok {
			// insert
			dml = sqlmodel.NewRowChange(sourceTable, targetTable, nil, values, ti, nil, nil)
		} else {
			if rand.Int()%2 > 0 {
				// update
				// check whether to update ID
				if rand.Float64() < updateIdentifyProbability {
					for try := 0; try < 10; try++ {
						newID := rand.Intn(maxID)
						if _, ok := kv[newID]; !ok {
							values[0] = newID
							break
						}
					}
				}
				dml = sqlmodel.NewRowChange(sourceTable, targetTable, oldValues, values, ti, nil, nil)
			} else {
				// delete
				dml = sqlmodel.NewRowChange(sourceTable, targetTable, oldValues, nil, ti, nil, nil)
			}
		}

		kv = mockExecute(kv, []*sqlmodel.RowChange{dml})
		dmls = append(dmls, dml)
	}

	kv = make(map[interface{}][]interface{})
	compactKV := make(map[interface{}][]interface{})

	// mock compactJob
	for i := 0; i < len(dmls); i += batch {
		end := i + batch
		if end > len(dmls) {
			end = len(dmls)
		}
		kv = mockExecute(kv, dmls[i:end])

		for _, dml := range dmls[i:end] {
			j := newDMLJob(dml, ec)
			// if update job update its identify keys, turn it into delete + insert
			if j.dml.IsIdentityUpdated() {
				delDML, insertDML := j.dml.SplitUpdate()
				delJob := j.clone()
				delJob.dml = delDML

				insertJob := j.clone()
				insertJob.dml = insertDML

				compactor.compactJob(delJob)
				compactor.compactJob(insertJob)
			} else {
				compactor.compactJob(j)
			}
		}

		noCompactNumber := end - i
		compactNumber := 0
		for _, dml := range dmls[i:end] {
			c.Logf("before compact, dml: %s", dml.String())
		}
		for _, j := range compactor.buffer {
			if j != nil {
				compactKV = mockExecute(compactKV, []*sqlmodel.RowChange{j.dml})
				compactNumber++
				c.Logf("after compact, dml: %s", j.dml.String())
			}
		}
		c.Logf("before compact: %d, after compact: %d", noCompactNumber, compactNumber)
		c.Assert(compactKV, check.DeepEquals, kv)
		compactor.keyMap = make(map[string]map[string]int)
		compactor.buffer = compactor.buffer[0:0]
	}
}

func (s *testSyncerSuite) TestCompactorSafeMode(c *check.C) {
	p := parser.New()
	se := mock.NewContext()
	sourceTable := &cdcmodel.TableName{Schema: "test", Table: "tb"}
	schemaStr := "create table test.tb(id int primary key, col1 int, name varchar(24))"
	ti, err := createTableInfo(p, se, 0, schemaStr)
	c.Assert(err, check.IsNil)

	testCases := []struct {
		input  []*job
		output []*job
	}{
		// nolint:dupl
		{
			input: []*job{
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, nil, []interface{}{1, 1, "a"}, ti, nil, nil),
					ecWithSafeMode,
				),
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, nil, []interface{}{2, 2, "b"}, ti, nil, nil),
					ecWithSafeMode,
				),
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, []interface{}{1, 1, "a"}, []interface{}{3, 3, "c"}, ti, nil, nil),
					ecWithSafeMode,
				),
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, []interface{}{2, 2, "b"}, nil, ti, nil, nil),
					ec,
				),
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, nil, []interface{}{1, 1, "a"}, ti, nil, nil),
					ec,
				),
			},
			output: []*job{
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, nil, []interface{}{3, 3, "c"}, ti, nil, nil),
					ecWithSafeMode,
				),
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, []interface{}{2, 2, "b"}, nil, ti, nil, nil),
					ecWithSafeMode,
				),
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, nil, []interface{}{1, 1, "a"}, ti, nil, nil),
					ecWithSafeMode,
				),
			},
		},
		// nolint:dupl
		{
			input: []*job{
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, nil, []interface{}{1, 1, "a"}, ti, nil, nil),
					ec,
				),
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, nil, []interface{}{2, 2, "b"}, ti, nil, nil),
					ec,
				),
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, []interface{}{1, 1, "a"}, []interface{}{3, 3, "c"}, ti, nil, nil),
					ec,
				),
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, []interface{}{2, 2, "b"}, nil, ti, nil, nil),
					ec,
				),
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, nil, []interface{}{1, 1, "a"}, ti, nil, nil),
					ec,
				),
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, nil, []interface{}{2, 2, "b"}, ti, nil, nil),
					ec,
				),
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, []interface{}{2, 2, "b"}, []interface{}{2, 2, "c"}, ti, nil, nil),
					ec,
				),
			},
			output: []*job{
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, nil, []interface{}{3, 3, "c"}, ti, nil, nil),
					ec,
				),
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, nil, []interface{}{1, 1, "a"}, ti, nil, nil),
					ecWithSafeMode,
				),
				newDMLJob(
					sqlmodel.NewRowChange(sourceTable, nil, nil, []interface{}{2, 2, "c"}, ti, nil, nil),
					ecWithSafeMode,
				),
			},
		},
	}

	inCh := make(chan *job, 100)
	syncer := &Syncer{
		tctx: tcontext.NewContext(context.Background(), log.L()),
		cfg: &config.SubTaskConfig{
			Name:     "task",
			SourceID: "source",
			SyncerConfig: config.SyncerConfig{
				QueueSize:   100,
				WorkerCount: 100,
			},
		},
		metricsProxies: metrics.DefaultMetricsProxies.CacheForOneTask("task", "worker", "source"),
	}

	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/syncer/SkipFlushCompactor", `return()`), check.IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/syncer/SkipFlushCompactor")

	outCh := compactorWrap(inCh, syncer)

	for _, tc := range testCases {
		for _, j := range tc.input {
			inCh <- j
		}
		inCh <- newFlushJob(syncer.cfg.WorkerCount, 1)
		c.Assert(
			utils.WaitSomething(10, time.Millisecond, func() bool {
				return len(outCh) == len(tc.output)+1
			}), check.Equals, true)
		for i := 0; i <= len(tc.output); i++ {
			j := <-outCh
			if i < len(tc.output) {
				c.Assert(j.String(), check.Equals, tc.output[i].String())
			} else {
				c.Assert(j.tp, check.Equals, flush)
			}
		}
	}
}
