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
	"context"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

const (
	rowEventQueueSize        = 256
	rowEventQueueBytes int64 = 16 << 20
)

// rowEventOperation transfers ownership of one immutable event context. A nil
// rows field denotes a transaction commit. Control events stay on Run's side of
// the boundary and must drain this stage before accessing its mutable state.
type rowEventOperation struct {
	rows    *replication.RowsEvent
	context eventContext
	charge  int64
	barrier bool
}

// rowEventExecutor has one producer and one ordered consumer. Rows, table
// checkpoints and transaction commits therefore have exactly one owner, while
// event fetching and its per-event observations can overlap row processing.
// Both queued and executing events hold byte credit. This limits wire bytes,
// not decoded heap size; one oversized event occupies the whole byte budget.
// Drain and Close must be called by the producer, never by the callback.
type rowEventExecutor struct {
	ctx       context.Context
	input     chan rowEventOperation
	drained   chan struct{}
	done      chan struct{}
	budget    *semaphore.Weighted
	byteLimit int64
	err       atomic.Error
}

func newRowEventExecutor(ctx context.Context, capacity int, byteLimit int64, handle func(rowEventOperation) error, interrupt func()) *rowEventExecutor {
	e := &rowEventExecutor{
		ctx: ctx, input: make(chan rowEventOperation, capacity), drained: make(chan struct{}, 1),
		done: make(chan struct{}), budget: semaphore.NewWeighted(byteLimit), byteLimit: byteLimit,
	}
	go func() {
		defer close(e.done)
		for op := range e.input {
			if op.barrier {
				e.drained <- struct{}{}
				continue
			}
			if e.err.Load() == nil {
				if err := handle(op); err != nil {
					e.err.Store(err)
					interrupt()
				}
			}
			e.budget.Release(op.charge)
		}
	}()
	return e
}

func (e *rowEventExecutor) Submit(rows *replication.RowsEvent, ec eventContext) error {
	if err := e.err.Load(); err != nil {
		return err
	}
	charge := min(max(int64(ec.header.EventSize), 1), e.byteLimit)
	if err := e.budget.Acquire(e.ctx, charge); err != nil {
		return err
	}
	select {
	case e.input <- rowEventOperation{rows: rows, context: ec, charge: charge}:
		return nil
	case <-e.ctx.Done():
		e.budget.Release(charge)
		return e.ctx.Err()
	}
}

// Even after cancellation, join all previously admitted work. The consumer
// discards successors of the first error but still releases their byte credit.
func (e *rowEventExecutor) Drain() error {
	e.input <- rowEventOperation{barrier: true}
	<-e.drained
	return e.err.Load()
}

func (e *rowEventExecutor) Close() error {
	close(e.input)
	<-e.done
	return e.err.Load()
}

// orderedRowHandler's state is only accessed by the executor, or by Run after
// Drain for synchronous control paths (compressed transactions and re-sync).
type orderedRowHandler struct {
	syncer               *Syncer
	affectedSourceTables map[string]map[string]struct{}
}

func (h *orderedRowHandler) recordAffected(table *filter.Table) {
	if table == nil || !h.syncer.cfg.EnableGTID {
		return
	}
	tables := h.affectedSourceTables[table.Schema]
	if tables == nil {
		tables = make(map[string]struct{})
		h.affectedSourceTables[table.Schema] = tables
	}
	tables[table.Name] = struct{}{}
}

func (h *orderedRowHandler) saveAffected(location binlog.Location) {
	for schema, tables := range h.affectedSourceTables {
		for table := range tables {
			h.syncer.saveTablePoint(&filter.Table{Schema: schema, Name: table}, location)
		}
	}
	clear(h.affectedSourceTables)
}

func (h *orderedRowHandler) handle(op rowEventOperation) (err error) {
	s, ec := h.syncer, op.context
	defer func() {
		if recovered := recover(); recovered != nil {
			s.tctx.L().Error("panic in ordered row processing", zap.Any("error", recovered), zap.Stack("stack"))
			err = terror.ErrSyncerUnitPanic.Generate(recovered)
		}
		if err != nil {
			originSQL := ""
			if ec.header.EventType == replication.QUERY_EVENT {
				originSQL = "COMMIT"
			}
			err = s.handleEventError(err, ec.startLocation, ec.endLocation, ec.header.EventType == replication.QUERY_EVENT, originSQL)
		}
		if waitXIDStatus(s.waitXIDJob.Load()) == waitComplete {
			s.runCancel()
		}
	}()
	if s.runCtx.Ctx.Err() != nil || s.execError.Load() != nil || waitXIDStatus(s.waitXIDJob.Load()) == waitComplete {
		return nil
	}
	if op.rows != nil {
		var table *filter.Table
		table, err = s.handleRowsEvent(op.rows, ec)
		if err == nil {
			h.recordAffected(table)
		}
		return err
	}
	h.saveAffected(ec.endLocation)
	if ce := s.tctx.L().Check(zap.DebugLevel, ""); ce != nil {
		ce.Write(zap.String("event", ec.header.EventType.String()), zap.Stringer("last location", ec.lastLocation), log.WrapStringerField("location", ec.endLocation))
	}
	_, err = s.handleJobFunc(newXIDJob(ec.endLocation, ec.startLocation, ec.endLocation))
	return err
}
