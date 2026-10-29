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
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/config"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestCausalityPipelineOrderAndBarriers(t *testing.T) {
	ti := mockTableInfo(t, "CREATE TABLE t (id INT PRIMARY KEY, u INT UNIQUE, s VARCHAR(64), UNIQUE KEY prefix(s(4)))")
	table := &cdcmodel.TableName{Schema: "db", Table: "t"}
	ctx := utils.NewSessionCtx(map[string]string{"time_zone": "+08:00", "sql_mode": "STRICT_ALL_TABLES"})
	handle := sqlmodel.GetWhereHandle(ti, ti)
	input := make([]*job, 0, 2048)
	for i := 0; i < 1500; i++ {
		pre, post := []any(nil), []any{i, i % 127, "AbCd"}
		if i%3 == 1 {
			pre = []any{i - 1, (i - 1) % 127, "abcd"}
		}
		if i%3 == 2 {
			pre, post = post, nil
		}
		r := sqlmodel.NewRowChange(table, nil, pre, post, ti, nil, ctx)
		r.SetWhereHandle(handle)
		input = append(input, &job{tp: dml, dml: r})
		if i%97 == 0 {
			input = append(input, &job{tp: asyncFlush, flushSeq: int64(i)}, &job{tp: gc, flushSeq: int64(i)})
		}
		if i%251 == 0 {
			input = append(input, &job{tp: flush, flushSeq: int64(i)}, &job{tp: gc, flushSeq: math.MaxInt64})
		}
	}
	type result struct {
		tp  opType
		key string
		seq int64
	}
	reference := newCausalityRelation()
	want := []result{}
	for _, j := range input {
		key := ""
		switch j.tp {
		case flush, asyncFlush:
			reference.rotate(j.flushSeq)
		case gc:
			reference.gc(j.flushSeq)
			continue
		default:
			keys := j.dml.CausalityKeys()
			var conflictDetected bool
			key, conflictDetected = referenceCausalityAssign(reference, keys)
			if conflictDetected {
				want = append(want, result{tp: conflict})
			}
		}
		want = append(want, result{tp: j.tp, key: key, seq: j.flushSeq})
	}
	for _, compact := range []bool{false, true} {
		for _, workers := range []int{1, 4, 512} {
			jobs := make(chan *job, 2000)
			s := &Syncer{cfg: &config.SubTaskConfig{Name: "pipeline", SourceID: "source", SyncerConfig: config.SyncerConfig{QueueSize: 128, WorkerCount: workers, Compact: compact}}, tctx: tcontext.Background().WithLogger(log.L()), causalityCtx: ctx}
			s.metricsProxies = metrics.DefaultMetricsProxies.CacheForOneTask("pipeline", "worker", "source")
			output := causalityWrap(jobs, s)
			for _, j := range input {
				jobs <- j.clone()
			}
			close(jobs)
			got := []result{}
			timeout := time.After(10 * time.Second)
		collect:
			for {
				select {
				case j, ok := <-output:
					if !ok {
						break collect
					}
					if j.tp == conflict {
						got = append(got, result{tp: conflict})
					} else {
						got = append(got, result{tp: j.tp, key: j.dmlQueueKey, seq: j.flushSeq})
					}
				case <-timeout:
					t.Fatal("pipeline did not drain after input close")
				}
			}
			require.Equal(t, want, got, "compact=%t workers=%d", compact, workers)
		}
	}
}

// Keep key semantics covered at the pipeline boundary, rather than testing a
// separate generator. Each schema has a cold, shared handle in the pipeline;
// the serial oracle uses independent handles so it cannot warm that cache.
func TestCausalityPipelineKeysAndSchemaChange(t *testing.T) {
	parent := mockTableInfo(t, "CREATE TABLE p(id INT PRIMARY KEY)")
	source := &cdcmodel.TableName{Schema: "DB", Table: "Child"}
	canonical := &cdcmodel.TableName{Schema: "db", Table: "child"}
	ctx := utils.NewSessionCtx(map[string]string{"time_zone": "+08:00", "sql_mode": "STRICT_ALL_TABLES"})
	var jobs []*job
	var want [][]string
	for _, schema := range []string{
		"CREATE TABLE c(id INT PRIMARY KEY, p INT, s VARCHAR(64), UNIQUE KEY u(s(3))) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci",
		"CREATE TABLE c(id INT PRIMARY KEY, p INT, s VARCHAR(64), UNIQUE KEY u((lower(s))))",
		"CREATE TABLE c(id INT, p INT, s VARCHAR(64))",
	} {
		ti := mockTableInfo(t, schema)
		handle := sqlmodel.GetWhereHandle(ti, ti)
		for _, value := range []any{nil, "AbCdEF", "中文字符"} {
			for _, images := range [][2][]any{
				{nil, {1, 10, value}},
				{{1, 10, value}, {2, nil, value}},
				{{1, 10, value}, nil},
			} {
				makeRow := func() *sqlmodel.RowChange {
					row := sqlmodel.NewRowChange(source, nil, images[0], images[1], ti, nil, ctx)
					row.SetCausalityKeySourceTable(canonical)
					row.SetForeignKeyRelations([]sqlmodel.ForeignKeyCausalityRelation{{ParentTable: "db.p", ParentColumns: []*timodel.ColumnInfo{parent.Columns[0]}, ChildColumnIdx: []int{1}}})
					return row
				}
				row := makeRow()
				row.SetWhereHandle(handle)
				jobs = append(jobs, &job{tp: dml, dml: row})
				keys := makeRow().CausalityKeys()
				require.Contains(t, keys, "10.id.db.p")
				want = append(want, keys)
			}
		}
	}
	input := make(chan *job, len(jobs))
	for _, j := range jobs {
		input <- j
	}
	close(input)
	p := newCausalityKeyPipeline(input, causalityRowQueueSize)
	count := 0
	for prepared := range p.ordered {
		require.Less(t, count, len(jobs))
		require.Same(t, jobs[count], prepared.job)
		require.Equal(t, want[count], prepared.keys)
		count++
	}
	p.wg.Wait()
	require.Equal(t, len(jobs), count)
}

func TestCausalityPipelineBoundAndClose(t *testing.T) {
	input := make(chan *job)
	p := newCausalityKeyPipeline(input, causalityRowQueueSize)
	require.Equal(t, causalityRowQueueSize, cap(p.ordered))
	// Fill the result queue and leave one row held by the preparation worker.
	for i := 0; i < causalityRowQueueSize+1; i++ {
		input <- &job{tp: flush}
	}
	accepted := make(chan struct{})
	go func() { input <- &job{tp: flush}; close(accepted); close(input) }()
	select {
	case <-accepted:
		t.Fatal("unbounded read-ahead")
	case <-time.After(20 * time.Millisecond):
	}
	<-p.ordered
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("consuming a row did not resume input")
	}
	count := 1
	for range p.ordered {
		count++
	}
	p.wg.Wait()
	require.Equal(t, causalityRowQueueSize+2, count)
}

func TestCausalityPreservesCompactorBackpressure(t *testing.T) {
	input := make(chan *job)
	s := &Syncer{
		cfg: &config.SubTaskConfig{
			Name: "compact-backpressure", SourceID: "source",
			SyncerConfig: config.SyncerConfig{QueueSize: 1, WorkerCount: 1, Compact: true},
		},
		tctx: tcontext.Background().WithLogger(log.L()),
	}
	s.metricsProxies = metrics.DefaultMetricsProxies.CacheForOneTask(s.cfg.Name, "worker", s.cfg.SourceID)
	output := causalityWrap(input, s)
	accepted := make(chan struct{}, 4)
	go func() {
		defer close(input)
		for i := 0; i < 4; i++ {
			input <- &job{tp: flush, flushSeq: int64(i)}
			accepted <- struct{}{}
		}
	}()
	// Always release the blocked stages, including after an assertion failure.
	t.Cleanup(func() {
		deadline := time.After(5 * time.Second)
		for {
			select {
			case _, ok := <-output:
				if !ok {
					return
				}
			case <-deadline:
				t.Error("compact pipeline did not drain")
				return
			}
		}
	})
	// One output slot, one consumer-held job and one producer-held job. A
	// fourth must stay upstream so the compactor can observe downstream load.
	for i := 0; i < 3; i++ {
		select {
		case <-accepted:
		case <-time.After(5 * time.Second):
			t.Fatal("pipeline did not accept its bounded in-flight jobs")
		}
	}
	select {
	case <-accepted:
		t.Fatal("causality read-ahead masked downstream backpressure from compactor")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestCausalityPipelineSparseAndOversized(t *testing.T) {
	ti := mockTableInfo(t, "CREATE TABLE t(id INT PRIMARY KEY)")
	table := &cdcmodel.TableName{Schema: "db", Table: "t"}
	for _, size := range []uint32{0, 4 << 20} {
		input := make(chan *job)
		p := newCausalityKeyPipeline(input, causalityRowQueueSize)
		j := &job{tp: dml, dml: sqlmodel.NewRowChange(table, nil, nil, []any{1}, ti, nil, nil), eventHeader: &replication.EventHeader{EventSize: size}}
		input <- j
		// No second row, input close, or timer should be needed to publish this row.
		select {
		case prepared := <-p.ordered:
			require.Same(t, j, prepared.job)
			require.Equal(t, []string{"1.id.db.t"}, prepared.keys)
		case <-time.After(time.Second):
			t.Fatal("sparse input was held waiting for a full batch")
		}
		close(input)
		for range p.ordered {
		}
		p.wg.Wait()
	}
	input := make(chan *job)
	close(input)
	p := newCausalityKeyPipeline(input, causalityRowQueueSize)
	_, ok := <-p.ordered
	require.False(t, ok)
	p.wg.Wait()
}

// The pre-pipeline two-pass algorithm is retained only as a test oracle.
func referenceCausalityAssign(m *causalityRelation, keys []string) (string, bool) {
	if len(keys) == 0 {
		return "", false
	}
	existing := ""
	conflicted := false
	for _, key := range keys {
		if v, ok := m.get(key); ok {
			if existing != "" && existing != v {
				conflicted = true
				break
			}
			existing = v
		}
	}
	if conflicted {
		m.clear()
	}
	selected := keys[0]
	missing := []string{}
	for _, key := range keys {
		if v, ok := m.get(key); ok {
			selected = v
		} else {
			missing = append(missing, key)
		}
	}
	for _, key := range missing {
		m.set(key, selected)
	}
	return selected, conflicted
}

func TestCausalityAssignmentEquivalent(t *testing.T) {
	a, b := newCausalityRelation(), newCausalityRelation()
	for i := 0; i < 10000; i++ {
		if i%37 == 0 {
			a.rotate(int64(i))
			b.rotate(int64(i))
		}
		if i%101 == 0 {
			a.gc(int64(i - 50))
			b.gc(int64(i - 50))
		}
		keys := []string{}
		for j := 0; j < i%5; j++ {
			keys = append(keys, fmt.Sprint((i*7+j*13)%31))
		}
		if i%71 == 0 {
			keys = append(keys, "")
		}
		expected, ec := referenceCausalityAssign(a, keys)
		got, gc := b.assign(keys)
		require.Equal(t, expected, got)
		require.Equal(t, ec, gc)
		require.Equal(t, a.groups, b.groups)
		require.Empty(t, b.missing)
	}
}

// An apply failure must not strand the bounded preparation worker or the final
// flush. Use the real DML worker's safe-mode/FK validation error path (before
// SQL execution) and drain the same channels used by the normal pipeline.
func TestCausalityPipelineDrainAfterApplyError(t *testing.T) {
	const rows = 1025
	ti := mockTableInfo(t, "CREATE TABLE t(id INT PRIMARY KEY)")
	table := &cdcmodel.TableName{Schema: "db", Table: "t"}
	input := make(chan *job, 8)
	s := &Syncer{
		cfg:  &config.SubTaskConfig{Name: "causality-error", SourceID: "source", SyncerConfig: config.SyncerConfig{QueueSize: 8, WorkerCount: 4}},
		tctx: tcontext.Background().WithLogger(log.L()),
	}
	s.metricsProxies = metrics.DefaultMetricsProxies.CacheForOneTask("causality-error", "worker", "source")
	var successes, failures atomic.Int64
	w := &DMLWorker{
		// The validation path returns before SQL, but the worker indexes its
		// connection slot before validation. Keep slots present and connections nil.
		workerCount: 4, chanSize: 2, batch: 1,
		toDBConns: make([]*dbconn.DBConn, 4),
		logger:    log.L(), metricProxies: s.metricsProxies,
		foreignKeyChecksEnabled: true,
		successFunc:             func(int, int, []*job) { successes.Add(1) },
		fatalFunc:               func(*job, error) { failures.Add(1) },
		lagFunc:                 func(*job, int) {},
		updateJobMetricsFunc:    func(bool, string, *job) {},
		inCh:                    causalityWrap(input, s), flushCh: make(chan *job),
	}
	go func() { w.run(); w.close() }()
	go func() {
		defer close(input)
		for i := 0; i < rows; i++ {
			row := sqlmodel.NewRowChange(table, nil, []any{2 * i}, []any{2*i + 1}, ti, nil, nil)
			input <- &job{tp: dml, dml: row, safeMode: true}
		}
		input <- newFlushJob(4, 1)
	}()
	timeout := time.After(10 * time.Second)
	flushes := 0
	for {
		select {
		case j, ok := <-w.flushCh:
			if !ok {
				require.Equal(t, 1, flushes)
				require.EqualValues(t, rows, failures.Load())
				require.Zero(t, successes.Load())
				return
			}
			require.Equal(t, flush, j.tp)
			flushes++
		case <-timeout:
			t.Fatal("pipeline did not drain after apply errors")
		}
	}
}

func TestCausalityPipelineMetricsAndDebugLogs(t *testing.T) {
	for _, level := range []zap.AtomicLevel{zap.NewAtomicLevelAt(zap.InfoLevel), zap.NewAtomicLevelAt(zap.DebugLevel)} {
		t.Run(level.String(), func(t *testing.T) {
			core, observed := observer.New(level.Level())
			ti := mockTableInfo(t, "CREATE TABLE t(id INT PRIMARY KEY)")
			table := &cdcmodel.TableName{Schema: "db", Table: "t"}
			input := make(chan *job, 2)
			name := "causality-observe-" + level.String()
			s := &Syncer{
				cfg:  &config.SubTaskConfig{Name: name, SourceID: "source", SyncerConfig: config.SyncerConfig{QueueSize: 2, WorkerCount: 4}},
				tctx: tcontext.Background().WithLogger(log.Logger{Logger: zap.New(core)}),
			}
			s.metricsProxies = metrics.DefaultMetricsProxies.CacheForOneTask(name, "worker", "source")
			for i := 1; i <= 2; i++ {
				input <- &job{tp: dml, dml: sqlmodel.NewRowChange(table, nil, nil, []any{i}, ti, nil, nil)}
			}
			close(input)
			count := 0
			for j := range causalityWrap(input, s) {
				require.Equal(t, dml, j.tp)
				count++
			}
			require.Equal(t, 2, count)
			gauge := s.metricsProxies.QueueSizeGauge.WithLabelValues(name, "causality_input", "source")
			require.Zero(t, testutil.ToFloat64(gauge))
			entries := observed.FilterMessage("key for keys").All()
			if level.Level() == zap.DebugLevel {
				require.Len(t, entries, 2)
				for i, entry := range entries {
					want := fmt.Sprintf("%d.id.db.t", i+1)
					require.Equal(t, want, entry.ContextMap()["key"])
					require.Equal(t, []interface{}{want}, entry.ContextMap()["keys"])
				}
			} else {
				require.Empty(t, entries)
			}
		})
	}
}
