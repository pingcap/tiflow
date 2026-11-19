// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package syncer

import (
	"strings"
	"testing"

	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/config"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
	"github.com/stretchr/testify/require"
)

func TestCompactorPreservesUniqueKeyCausality(t *testing.T) {
	type change struct {
		pre  []interface{}
		post []interface{}
	}
	cases := []struct {
		name       string
		changes    []change
		reusedKeys []int
		final      []interface{}
	}{
		{
			name: "delete and insert release a unique key",
			changes: []change{
				{pre: []interface{}{91, 92, 2}},
				{post: []interface{}{91, 91, nil}},
			},
			reusedKeys: []int{92},
			final:      []interface{}{91, 91, nil},
		},
		{
			name: "keys survive repeated compaction",
			changes: []change{
				{pre: []interface{}{91, 92, 2}},
				{post: []interface{}{91, 91, nil}},
				{pre: []interface{}{91, 91, nil}, post: []interface{}{91, 90, nil}},
				{pre: []interface{}{91, 90, nil}},
				{post: []interface{}{91, 89, nil}},
			},
			reusedKeys: []int{92, 91, 90},
			final:      []interface{}{91, 89, nil},
		},
		{
			name: "keys survive replacement of pre values by reduce",
			changes: []change{
				{pre: []interface{}{91, 92, nil}, post: []interface{}{91, 93, nil}},
				{pre: []interface{}{91, 93, nil}, post: []interface{}{91, 94, nil}},
				{pre: []interface{}{91, 94, nil}},
				{post: []interface{}{91, 91, nil}},
			},
			reusedKeys: []int{92, 93, 94},
			final:      []interface{}{91, 91, nil},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ti := mockTableInfo(t, "CREATE TABLE tb(a INT PRIMARY KEY, b INT UNIQUE, c INT)")
			source := &cdcmodel.TableName{Schema: "shardddl1", Table: "tb1"}
			target := &cdcmodel.TableName{Schema: "shardddl", Table: "tb"}
			c := &compactor{
				logger:             log.L(),
				keyMap:             make(map[string]map[string]int),
				outCh:              make(chan *job, len(tc.changes)),
				updateJobMetricsFn: func(bool, string, *job) {},
			}
			for _, row := range tc.changes {
				dml := sqlmodel.NewRowChange(source, target, row.pre, row.post, ti, nil, nil)
				c.compactJob(newDMLJob(dml, ec))
			}
			c.flushBuffer()
			require.Len(t, c.outCh, 1, "the fix must preserve compaction")
			compacted := <-c.outCh
			require.Equal(t, sqlmodel.RowChangeInsert, compacted.dml.Type())
			queries, args := (&DMLWorker{}).genSQLs([]*job{compacted})
			require.Equal(t, []string{"REPLACE INTO `shardddl`.`tb` (`a`,`b`,`c`) VALUES (?,?,?)"}, queries)
			require.Equal(t, [][]interface{}{tc.final}, args)

			in := make(chan *job, 1+len(tc.reusedKeys))
			syncer := &Syncer{
				cfg: &config.SubTaskConfig{
					Name:         t.Name(),
					SourceID:     "source",
					SyncerConfig: config.SyncerConfig{QueueSize: 16, WorkerCount: 16},
				},
				tctx:           tcontext.Background(),
				metricsProxies: metrics.DefaultMetricsProxies.CacheForOneTask(t.Name(), "worker", "source"),
			}
			out := causalityWrap(in, syncer)
			in <- compacted
			for i, key := range tc.reusedKeys {
				dml := sqlmodel.NewRowChange(source, target, nil, []interface{}{200 + i, key, nil}, ti, nil, nil)
				in <- newDMLJob(dml, ec)
			}
			close(in)

			var jobs []*job
			for j := range out {
				require.Equal(t, dml, j.tp)
				jobs = append(jobs, j)
			}
			require.Len(t, jobs, 1+len(tc.reusedKeys))
			for _, j := range jobs[1:] {
				require.Equal(t, jobs[0].dmlQueueKey, j.dmlQueueKey,
					"reusing a unique key must wait for the compacted job to release it")
			}
		})
	}
}

func TestCompactorDeduplicatesCausalityKeys(t *testing.T) {
	ti := mockTableInfo(t, "CREATE TABLE tb(a INT PRIMARY KEY, b INT UNIQUE, c INT)")
	source := &cdcmodel.TableName{Schema: "shardddl1", Table: "tb1"}
	c := &compactor{
		logger:             log.L(),
		keyMap:             make(map[string]map[string]int),
		updateJobMetricsFn: func(bool, string, *job) {},
	}
	for i := range 1000 {
		dml := sqlmodel.NewRowChange(source, nil, []interface{}{91, 92, i}, []interface{}{91, 92, i + 1}, ti, nil, nil)
		c.compactJob(newDMLJob(dml, ec))
	}
	compacted := c.buffer[len(c.buffer)-1]
	require.ElementsMatch(t, []string{"91.a.shardddl1.tb1", "92.b.shardddl1.tb1"}, compacted.causalityKeys())
}

func TestCompactorPreservesInterleavedUniqueKeyReuse(t *testing.T) {
	ti := mockTableInfo(t, "CREATE TABLE tb(a INT PRIMARY KEY, b INT UNIQUE)")
	source := &cdcmodel.TableName{Schema: "test", Table: "tb"}
	c := &compactor{
		logger:             log.L(),
		keyMap:             make(map[string]map[string]int),
		outCh:              make(chan *job, 3),
		updateJobMetricsFn: func(bool, string, *job) {},
	}
	// The INSERT reuses b=92 before the original primary key is inserted again.
	changes := []*sqlmodel.RowChange{
		sqlmodel.NewRowChange(source, nil, []interface{}{91, 92}, nil, ti, nil, nil),
		sqlmodel.NewRowChange(source, nil, nil, []interface{}{92, 92}, ti, nil, nil),
		sqlmodel.NewRowChange(source, nil, nil, []interface{}{91, 91}, ti, nil, nil),
	}
	for _, change := range changes {
		c.compactJob(newDMLJob(change, ec))
	}
	c.flushBuffer()
	close(c.outCh)

	// Apply the generated SQL's INSERT/REPLACE semantics to a table with a
	// primary key and a unique key. Final row counts alone miss this conflict.
	rows := map[int]int{91: 92}
	for j := range c.outCh {
		queries, _ := (&DMLWorker{}).genSQLs([]*job{j})
		require.Len(t, queries, 1)
		switch j.dml.Type() {
		case sqlmodel.RowChangeDelete:
			delete(rows, j.dml.GetPreValues()[0].(int))
		case sqlmodel.RowChangeInsert:
			values := j.dml.GetPostValues()
			a, b := values[0].(int), values[1].(int)
			if strings.HasPrefix(queries[0], "REPLACE INTO") {
				for pk, uk := range rows {
					if pk == a || uk == b {
						delete(rows, pk)
					}
				}
			} else {
				require.NotContains(t, rows, a, "duplicate primary key executing %s", queries[0])
				for _, uk := range rows {
					require.NotEqual(t, b, uk, "unique key must be released before executing %s", queries[0])
				}
			}
			rows[a] = b
		default:
			t.Fatalf("unexpected compacted change: %s", j.dml)
		}
	}
	require.Equal(t, map[int]int{91: 91, 92: 92}, rows)
}
