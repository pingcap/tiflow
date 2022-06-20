// Copyright 2022 PingCAP, Inc.
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

package tablesink

import (
	"sort"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/roweventsink"
	"go.uber.org/atomic"
)

// Assert TableSink implementation
var _ TableSink = (*rowEventTableSink)(nil)

type rowEventTableSink struct {
	rowID                   uint64
	maxResolvedTs           model.ResolvedTs
	backendSink             roweventsink.RowEventSink
	rowEventProgressTracker *progressTracker
	// NOTICE: It is ordered by commitTs.
	rowBuffer   []*model.RowChangedEvent
	TableStatus *atomic.Uint32
}

func (r *rowEventTableSink) AppendRowChangedEvents(rows ...*model.RowChangedEvent) {
	r.rowBuffer = append(r.rowBuffer, rows...)
}

func (r *rowEventTableSink) UpdateResolvedTs(resolvedTs model.ResolvedTs) {
	// If resolvedTs is not greater than maxResolvedTs,
	// the flush is unnecessary.
	if !r.maxResolvedTs.Less(resolvedTs) {
		return
	}
	r.maxResolvedTs = resolvedTs

	i := sort.Search(len(r.rowBuffer), func(i int) bool {
		return r.rowBuffer[i].CommitTs > resolvedTs.Ts
	})
	if i == 0 {
		return
	}
	resolvedRows := r.rowBuffer[:i]

	resolvedRowEvents := make([]*roweventsink.RowEvent, 0, len(resolvedRows))
	for _, row := range resolvedRows {
		rowEvent := &roweventsink.RowEvent{
			Row: row,
			Callback: func() {
				r.rowEventProgressTracker.remove(r.rowID)
			},
			TableStatus: r.TableStatus,
		}
		resolvedRowEvents = append(resolvedRowEvents, rowEvent)
		r.rowEventProgressTracker.addEvent(r.rowID)
		r.rowID++
	}
	r.rowEventProgressTracker.addResolvedTs(r.rowID, resolvedTs)
	r.rowID++

	r.backendSink.WriteRowChangedEvents(resolvedRowEvents...)
}

func (r *rowEventTableSink) GetCheckpointTs() model.ResolvedTs {
	return r.rowEventProgressTracker.minTs()
}

func (r *rowEventTableSink) Close() {
	// TODO implement me
	panic("implement me")
}
