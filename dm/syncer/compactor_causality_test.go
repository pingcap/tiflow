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
	"math/rand"
	"strings"
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

	done := make(chan int, 1)
	go func() {
		count := 0
		for j := range outCh {
			if j.tp == dml {
				require.NotNil(t, j.cachedCausalityKeys)
				require.NotEmpty(t, j.dmlQueueKey)
				count++
			}
		}
		done <- count
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

	require.Greater(t, <-done, 0)
}

func TestCompactorRandomizedUniqueKeyCompactionAndCausality(t *testing.T) {
	ti := mockTableInfo(t, "CREATE TABLE tb(a INT PRIMARY KEY, b INT UNIQUE, c INT)")
	source := &cdcmodel.TableName{Schema: "test", Table: "tb"}
	whereHandle := sqlmodel.GetWhereHandle(ti, ti)

	rng := rand.New(rand.NewSource(1))
	const (
		totalOps  = 3000
		batchSize = 50
		keySpace  = 15
	)

	type rowVal struct {
		b int
		c int
	}
	expected := make(map[int]rowVal)
	expectedUK := make(map[int]int) // b -> a

	pickFreeUK := func() (int, bool) {
		for range 30 {
			candidate := rng.Intn(keySpace)
			if _, used := expectedUK[candidate]; !used {
				return candidate, true
			}
		}
		return 0, false
	}

	var allChanges []*sqlmodel.RowChange
	for step := range totalOps {
		a := rng.Intn(keySpace)
		cur, exists := expected[a]
		if !exists {
			b, ok := pickFreeUK()
			if !ok {
				continue
			}
			rc := sqlmodel.NewRowChange(source, nil, nil, []interface{}{a, b, step}, ti, nil, nil)
			rc.SetWhereHandle(whereHandle)
			allChanges = append(allChanges, rc)
			expected[a] = rowVal{b: b, c: step}
			expectedUK[b] = a
			continue
		}

		switch rng.Intn(4) {
		case 0: // DELETE
			rc := sqlmodel.NewRowChange(source, nil, []interface{}{a, cur.b, cur.c}, nil, ti, nil, nil)
			rc.SetWhereHandle(whereHandle)
			allChanges = append(allChanges, rc)
			delete(expected, a)
			delete(expectedUK, cur.b)
		case 1: // UPDATE non-key column
			rc := sqlmodel.NewRowChange(source, nil, []interface{}{a, cur.b, cur.c}, []interface{}{a, cur.b, step}, ti, nil, nil)
			rc.SetWhereHandle(whereHandle)
			allChanges = append(allChanges, rc)
			expected[a] = rowVal{b: cur.b, c: step}
		case 2: // UPDATE UK
			newB, ok := pickFreeUK()
			if !ok {
				continue
			}
			rc := sqlmodel.NewRowChange(source, nil, []interface{}{a, cur.b, cur.c}, []interface{}{a, newB, step}, ti, nil, nil)
			rc.SetWhereHandle(whereHandle)
			allChanges = append(allChanges, rc)
			delete(expectedUK, cur.b)
			expected[a] = rowVal{b: newB, c: step}
			expectedUK[newB] = a
		case 3: // UPDATE PK (identity update)
			newA := rng.Intn(keySpace)
			if _, taken := expected[newA]; taken {
				continue
			}
			newB := cur.b
			if rng.Intn(2) == 0 {
				if candidate, ok := pickFreeUK(); ok {
					newB = candidate
				}
			}
			rc := sqlmodel.NewRowChange(source, nil, []interface{}{a, cur.b, cur.c}, []interface{}{newA, newB, step}, ti, nil, nil)
			rc.SetWhereHandle(whereHandle)
			allChanges = append(allChanges, rc)
			delete(expected, a)
			delete(expectedUK, cur.b)
			expected[newA] = rowVal{b: newB, c: step}
			expectedUK[newB] = newA
		}
	}

	// Execute compacted batches strictly enforcing PK/UK constraints and causality queues.
	actual := make(map[int]rowVal)
	actualUK := make(map[int]int)
	for start := 0; start < len(allChanges); start += batchSize {
		end := min(start+batchSize, len(allChanges))
		inputs := make([]*job, 0, end-start)
		for _, rc := range allChanges[start:end] {
			inputs = append(inputs, newDMLJob(rc, ec))
		}
		compacted := runCompactorBuffered(t, batchSize*2, inputs)

		// Verify strict PK/UK SQL execution in emitted order and record the
		// actual PK/UK values touched by each emitted job (including rows
		// deleted/overwritten by REPLACE or UPDATE).
		touchedKeys := make(map[*job][]string, len(compacted))
		for _, j := range compacted {
			queries, _ := (&DMLWorker{}).genSQLs([]*job{j})
			require.Len(t, queries, 1)
			var keys []string
			switch j.dml.Type() {
			case sqlmodel.RowChangeDelete:
				pk := j.dml.GetPreValues()[0].(int)
				keys = append(keys, fmt.Sprintf("pk:%d", pk))
				if prev, ok := actual[pk]; ok {
					keys = append(keys, fmt.Sprintf("uk:%d", prev.b))
					delete(actualUK, prev.b)
					delete(actual, pk)
				}
			case sqlmodel.RowChangeInsert:
				vals := j.dml.GetPostValues()
				a, b, cVal := vals[0].(int), vals[1].(int), vals[2].(int)
				keys = append(keys, fmt.Sprintf("pk:%d", a), fmt.Sprintf("uk:%d", b))
				if strings.HasPrefix(queries[0], "REPLACE INTO") {
					if prev, ok := actual[a]; ok {
						keys = append(keys, fmt.Sprintf("uk:%d", prev.b))
						delete(actualUK, prev.b)
						delete(actual, a)
					}
					if prevPK, ok := actualUK[b]; ok {
						keys = append(keys, fmt.Sprintf("pk:%d", prevPK))
						delete(actual, prevPK)
						delete(actualUK, b)
					}
				} else {
					require.NotContains(t, actual, a, "duplicate PK on %s", queries[0])
					require.NotContains(t, actualUK, b, "duplicate UK on %s", queries[0])
				}
				actual[a] = rowVal{b: b, c: cVal}
				actualUK[b] = a
			case sqlmodel.RowChangeUpdate:
				pre := j.dml.GetPreValues()
				post := j.dml.GetPostValues()
				oldA := pre[0].(int)
				newA, newB, newC := post[0].(int), post[1].(int), post[2].(int)
				require.Equal(t, oldA, newA)
				prev, ok := actual[oldA]
				require.True(t, ok, "missing row for %s", queries[0])
				if holder, conflict := actualUK[newB]; conflict {
					require.Equal(t, oldA, holder, "duplicate UK on %s", queries[0])
				}
				keys = append(keys, fmt.Sprintf("pk:%d", oldA), fmt.Sprintf("uk:%d", prev.b), fmt.Sprintf("uk:%d", newB))
				delete(actualUK, prev.b)
				actual[newA] = rowVal{b: newB, c: newC}
				actualUK[newB] = newA
			default:
				t.Fatalf("unexpected row change type: %s", j.dml)
			}
			touchedKeys[j] = keys
		}

		// Verify causality assigns any jobs touching the same actual PK/UK
		// (including old UKs released by compacted REPLACE/UPDATE/DELETE) to
		// the same queue unless separated by a conflict barrier.
		in := make(chan *job, len(compacted))
		syncer := &Syncer{
			cfg: &config.SubTaskConfig{
				Name:         t.Name(),
				SourceID:     "source",
				SyncerConfig: config.SyncerConfig{QueueSize: 128, WorkerCount: 16},
			},
			tctx:           tcontext.Background(),
			metricsProxies: metrics.DefaultMetricsProxies.CacheForOneTask(t.Name(), "worker", "source"),
		}
		out := causalityWrap(in, syncer)
		for _, j := range compacted {
			in <- j
		}
		close(in)

		keyQueue := make(map[string]string)
		for j := range out {
			if j.tp == conflict {
				clear(keyQueue)
				continue
			}
			require.Equal(t, dml, j.tp)
			for _, k := range touchedKeys[j] {
				if prevQ, seen := keyQueue[k]; seen {
					require.Equal(t, prevQ, j.dmlQueueKey, "shared touched key %s dispatched to different queues without conflict", k)
				}
				keyQueue[k] = j.dmlQueueKey
			}
		}
	}

	require.Equal(t, expected, actual)
}
