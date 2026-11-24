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
	"fmt"
	"testing"

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/config"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
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

	var outputs []*job
	for j := range c.outCh {
		outputs = append(outputs, j)
	}
	// The DELETE must remain ahead of the INSERT reusing b=92. Merging it
	// into the final INSERT would move the release past that reuse.
	require.Len(t, outputs, 3)
	require.Equal(t, sqlmodel.RowChangeDelete, outputs[0].dml.Type())
	require.Equal(t, []interface{}{91, 92}, outputs[0].dml.GetPreValues())
	require.Equal(t, sqlmodel.RowChangeInsert, outputs[1].dml.Type())
	require.Equal(t, []interface{}{92, 92}, outputs[1].dml.GetPostValues())
	require.Equal(t, sqlmodel.RowChangeInsert, outputs[2].dml.Type())
	require.Equal(t, []interface{}{91, 91}, outputs[2].dml.GetPostValues())
}

func TestCompactorPreservesInterleavedHistoricalKeyReuse(t *testing.T) {
	ti := mockTableInfo(t, "CREATE TABLE tb(a INT PRIMARY KEY, b INT UNIQUE)")
	source := &cdcmodel.TableName{Schema: "test", Table: "tb"}
	c := &compactor{
		logger:             log.L(),
		keyMap:             make(map[string]map[string]int),
		outCh:              make(chan *job, 4),
		updateJobMetricsFn: func(bool, string, *job) {},
	}
	changes := []*sqlmodel.RowChange{
		sqlmodel.NewRowChange(source, nil, []interface{}{91, 92}, nil, ti, nil, nil),
		sqlmodel.NewRowChange(source, nil, nil, []interface{}{91, 91}, ti, nil, nil),
		// b=92 exists only in the merged job's history at this point.
		sqlmodel.NewRowChange(source, nil, nil, []interface{}{92, 92}, ti, nil, nil),
		// Move the job reusing b=92 through another merge as well.
		sqlmodel.NewRowChange(source, nil, []interface{}{92, 92}, []interface{}{92, 93}, ti, nil, nil),
		sqlmodel.NewRowChange(source, nil, []interface{}{91, 91}, []interface{}{91, 90}, ti, nil, nil),
	}
	for _, change := range changes {
		c.compactJob(newDMLJob(change, ec))
	}
	// The final UPDATE must flush the two preceding merged jobs first.
	require.Len(t, c.outCh, 2)
	released := <-c.outCh
	reused := <-c.outCh
	require.Equal(t, []interface{}{91, 91}, released.dml.GetPostValues())
	require.Contains(t, released.causalityKeys(), "92.b.test.tb")
	require.Equal(t, []interface{}{92, 93}, reused.dml.GetPostValues())
	require.Contains(t, reused.causalityKeys(), "92.b.test.tb")
	c.flushBuffer()
	require.Len(t, c.outCh, 1)
	require.Equal(t, []interface{}{91, 90}, (<-c.outCh).dml.GetPostValues())
}

// runCompactorBuffered feeds jobs through compactor.run() while keeping a
// sentinel entry in outCh so len(outCh) > 0 prevents per-job flushes without
// requiring failpoints.
func runCompactorBuffered(t *testing.T, bufferSize int, inputs []*job) []*job {
	t.Helper()
	outCap := len(inputs)*2 + 4
	c := &compactor{
		inCh:               make(chan *job, len(inputs)+1),
		outCh:              make(chan *job, outCap),
		bufferSize:         bufferSize,
		logger:             log.L(),
		keyMap:             make(map[string]map[string]int),
		buffer:             make([]*job, 0, bufferSize),
		task:               t.Name(),
		source:             "source",
		metricProxies:      metrics.DefaultMetricsProxies.CacheForOneTask(t.Name(), "worker", "source"),
		updateJobMetricsFn: func(bool, string, *job) {},
	}
	sentinel := newGCJob(0)
	c.outCh <- sentinel
	for _, j := range inputs {
		c.inCh <- j
	}
	c.inCh <- newFlushJob(1, 1)
	close(c.inCh)

	c.run()
	c.close()

	var outputs []*job
	for j := range c.outCh {
		if j == sentinel || j.tp == flush {
			continue
		}
		outputs = append(outputs, j)
	}
	return outputs
}

func TestCompactorRunPreservesSafeModeAcrossEarlyFlush(t *testing.T) {
	ti := mockTableInfo(t, "CREATE TABLE tb(a INT PRIMARY KEY, b INT UNIQUE)")
	source := &cdcmodel.TableName{Schema: "test", Table: "tb"}

	inputs := []*job{
		// First job sets compactor.safeMode=true.
		newDMLJob(sqlmodel.NewRowChange(source, nil, []interface{}{91, 92}, nil, ti, nil, nil), ecWithSafeMode),
		// Reuses b=92, blocking the first job; still in the same safeMode=true buffer.
		newDMLJob(sqlmodel.NewRowChange(source, nil, nil, []interface{}{92, 92}, ti, nil, nil), ec),
		// Triggers early flush of the first two jobs and resets compactor.safeMode=false.
		newDMLJob(sqlmodel.NewRowChange(source, nil, nil, []interface{}{91, 91}, ti, nil, nil), ec),
		// Stays in the post-flush buffer and must not inherit the old safeMode=true.
		newDMLJob(sqlmodel.NewRowChange(source, nil, nil, []interface{}{93, 93}, ti, nil, nil), ec),
	}

	outputs := runCompactorBuffered(t, 16, inputs)
	require.Len(t, outputs, 4)
	require.True(t, outputs[0].safeMode)
	require.True(t, outputs[1].safeMode)
	require.False(t, outputs[2].safeMode)
	require.False(t, outputs[3].safeMode)
}

func TestCompactorRunSplitsIdentityUpdateAndBlocksCompaction(t *testing.T) {
	ti := mockTableInfo(t, "CREATE TABLE tb(a INT PRIMARY KEY, b INT UNIQUE)")
	source := &cdcmodel.TableName{Schema: "test", Table: "tb"}

	inputs := []*job{
		// Identity update splits into DELETE(91,92) + INSERT(92,92).
		// INSERT(92,92) shares b=92 and must block DELETE(91,92).
		newDMLJob(sqlmodel.NewRowChange(source, nil, []interface{}{91, 92}, []interface{}{92, 92}, ti, nil, nil), ec),
		// Must flush DELETE(91,92) and INSERT(92,92) instead of moving DELETE(91,92) past INSERT(92,92).
		newDMLJob(sqlmodel.NewRowChange(source, nil, nil, []interface{}{91, 91}, ti, nil, nil), ec),
	}

	outputs := runCompactorBuffered(t, 16, inputs)
	require.Len(t, outputs, 3)
	require.Equal(t, sqlmodel.RowChangeDelete, outputs[0].dml.Type())
	require.Equal(t, []interface{}{91, 92}, outputs[0].dml.GetPreValues())
	require.Equal(t, sqlmodel.RowChangeInsert, outputs[1].dml.Type())
	require.Equal(t, []interface{}{92, 92}, outputs[1].dml.GetPostValues())
	require.Equal(t, sqlmodel.RowChangeInsert, outputs[2].dml.Type())
	require.Equal(t, []interface{}{91, 91}, outputs[2].dml.GetPostValues())
}

func TestCompactorRunBuffersNonCompactableForeignKeyChild(t *testing.T) {
	parentTI := mockTableInfo(t, "CREATE TABLE parent(a INT PRIMARY KEY)")
	childTI := mockTableInfo(t, "CREATE TABLE child(pid INT)")
	parentTbl := &cdcmodel.TableName{Schema: "test", Table: "parent"}
	childTbl := &cdcmodel.TableName{Schema: "test", Table: "child"}

	childDML := sqlmodel.NewRowChange(childTbl, nil, nil, []interface{}{1}, childTI, nil, nil)
	require.False(t, childDML.HasNotNullUniqueIdx())
	childDML.SetForeignKeyRelations([]sqlmodel.ForeignKeyCausalityRelation{{
		ParentTable:    parentTbl.String(),
		ParentColumns:  []*timodel.ColumnInfo{parentTI.Columns[0]},
		ChildColumnIdx: []int{0},
	}})

	inputs := []*job{
		newDMLJob(sqlmodel.NewRowChange(parentTbl, nil, nil, []interface{}{1}, parentTI, nil, nil), ec),
		newDMLJob(childDML, ec),
		newDMLJob(sqlmodel.NewRowChange(parentTbl, nil, []interface{}{1}, nil, parentTI, nil, nil), ec),
	}

	outputs := runCompactorBuffered(t, 16, inputs)
	require.Len(t, outputs, 3)
	require.Equal(t, sqlmodel.RowChangeInsert, outputs[0].dml.Type())
	require.Equal(t, parentTbl, outputs[0].dml.GetSourceTable())
	require.Equal(t, childTbl, outputs[1].dml.GetSourceTable())
	require.NotNil(t, outputs[1].cachedCausalityKeys)
	require.Contains(t, outputs[1].causalityKeys(), "1.a.test.parent")
	require.Equal(t, sqlmodel.RowChangeDelete, outputs[2].dml.Type())
	require.Equal(t, parentTbl, outputs[2].dml.GetSourceTable())
}

func TestCompactorResetsOversizedCausalityKeyOwners(t *testing.T) {
	ti := mockTableInfo(t, "CREATE TABLE tb(a INT PRIMARY KEY, b INT UNIQUE)")
	source := &cdcmodel.TableName{Schema: "test", Table: "tb"}
	batchSize := maxRetainedCausalityKeyOwners + 1
	c := &compactor{
		logger:             log.L(),
		keyMap:             make(map[string]map[string]int),
		buffer:             make([]*job, 0, batchSize),
		outCh:              make(chan *job, batchSize),
		updateJobMetricsFn: func(bool, string, *job) {},
	}

	// Small batch retains and clears the owner map for reuse.
	for i := range 2 {
		c.compactJob(newDMLJob(sqlmodel.NewRowChange(source, nil, nil, []interface{}{i, i}, ti, nil, nil), ec))
	}
	c.flushBuffer()
	require.NotNil(t, c.causalityKeyOwners)
	require.Empty(t, c.causalityKeyOwners)
	for len(c.outCh) > 0 {
		<-c.outCh
	}

	// Oversized batch releases the owner map and clears retained buffer pointers.
	for i := range batchSize {
		c.compactJob(newDMLJob(sqlmodel.NewRowChange(source, nil, nil, []interface{}{i, i}, ti, nil, nil), ec))
	}
	c.flushBuffer()
	require.Nil(t, c.causalityKeyOwners)
	fullBuf := c.buffer[:cap(c.buffer)]
	for _, slot := range fullBuf {
		require.Nil(t, slot)
	}
}

func TestCompactorAndCausalityShareSessionCtxNoRace(t *testing.T) {
	prefixTI := mockTableInfo(t, "CREATE TABLE tb_prefix(a INT PRIMARY KEY, b VARCHAR(16), UNIQUE KEY uk_b(b(4)))")
	noPKTI := mockTableInfo(t, "CREATE TABLE tb_nopk(a INT, b VARCHAR(16))")
	prefixTbl := &cdcmodel.TableName{Schema: "test", Table: "tb_prefix"}
	noPKTbl := &cdcmodel.TableName{Schema: "test", Table: "tb_nopk"}
	prefixHandle := sqlmodel.GetWhereHandle(prefixTI, prefixTI)
	noPKHandle := sqlmodel.GetWhereHandle(noPKTI, noPKTI)

	sessCtx := utils.NewSessionCtx(map[string]string{"time_zone": "UTC"})
	syncer := &Syncer{
		cfg: &config.SubTaskConfig{
			Name:         t.Name(),
			SourceID:     "source",
			SyncerConfig: config.SyncerConfig{QueueSize: 64, WorkerCount: 8},
		},
		tctx:           tcontext.Background(),
		causalityCtx:   sessCtx,
		metricsProxies: metrics.DefaultMetricsProxies.CacheForOneTask(t.Name(), "worker", "source"),
	}

	inCh := make(chan *job, 64)
	compactCh := compactorWrap(inCh, syncer)
	outCh := causalityWrap(compactCh, syncer)

	type readResult struct {
		count             int
		missingCachedKeys int
		emptyQueueKeys    int
	}
	done := make(chan readResult, 1)
	go func() {
		var res readResult
		for j := range outCh {
			if j.tp == dml {
				if j.cachedCausalityKeys == nil {
					res.missingCachedKeys++
				}
				if j.dmlQueueKey == "" {
					res.emptyQueueKeys++
				}
				res.count++
			}
		}
		done <- res
	}()

	for i := range 200 {
		prefixVal := fmt.Sprintf("val_%03d_suffix", i)
		nextVal := fmt.Sprintf("val_%03d_next", i)
		del := sqlmodel.NewRowChange(prefixTbl, nil, []interface{}{i, prefixVal}, nil, prefixTI, nil, sessCtx)
		del.SetWhereHandle(prefixHandle)
		inCh <- newDMLJob(del, ec)

		ins := sqlmodel.NewRowChange(prefixTbl, nil, nil, []interface{}{i, nextVal}, prefixTI, nil, sessCtx)
		ins.SetWhereHandle(prefixHandle)
		inCh <- newDMLJob(ins, ec)

		upd := sqlmodel.NewRowChange(prefixTbl, nil, []interface{}{i, nextVal}, []interface{}{i + 1000, nextVal}, prefixTI, nil, sessCtx)
		upd.SetWhereHandle(prefixHandle)
		inCh <- newDMLJob(upd, ec)

		noPK := sqlmodel.NewRowChange(noPKTbl, nil, nil, []interface{}{i, prefixVal}, noPKTI, nil, sessCtx)
		noPK.SetWhereHandle(noPKHandle)
		inCh <- newDMLJob(noPK, ec)
	}
	inCh <- newFlushJob(syncer.cfg.WorkerCount, 1)
	close(inCh)

	res := <-done
	require.Greater(t, res.count, 0)
	require.Zero(t, res.missingCachedKeys)
	require.Zero(t, res.emptyQueueKeys)
}
