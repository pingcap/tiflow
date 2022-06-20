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
	"github.com/pingcap/tiflow/cdc/sinkv2/txneventsink"
	"go.uber.org/atomic"
)

// Assert TableSink implementation
var _ TableSink = (*txnEventTableSink)(nil)

type txnEventTableSink struct {
	txnID                   uint64
	maxResolvedTs           model.ResolvedTs
	backendSink             txneventsink.TxnEventSink
	txnEventProgressTracker *progressTracker
	// NOTICE: It is ordered by commitTs.
	txnBuffer   []*model.SingleTableTxn
	TableStatus *atomic.Uint32
}

func (t *txnEventTableSink) AppendRowChangedEvents(rows ...*model.RowChangedEvent) {
	// TODO implement me
	// Assemble each txn with the same startTs and the same commitTs in the order of commitTs.
	panic("implement me")
}

func (t *txnEventTableSink) UpdateResolvedTs(resolvedTs model.ResolvedTs) {
	// If resolvedTs is not greater than maxResolvedTs,
	// the flush is unnecessary.
	if !t.maxResolvedTs.Less(resolvedTs) {
		return
	}
	t.maxResolvedTs = resolvedTs

	i := sort.Search(len(t.txnBuffer), func(i int) bool {
		return t.txnBuffer[i].CommitTs > resolvedTs.Ts
	})
	if i == 0 {
		return
	}
	resolvedTxns := t.txnBuffer[:i]

	resolvedTxnEvents := make([]*txneventsink.TxnEvent, 0, len(resolvedTxns))
	for _, txn := range resolvedTxns {
		txnEvent := &txneventsink.TxnEvent{
			Txn: txn,
			Callback: func() {
				t.txnEventProgressTracker.remove(t.txnID)
			},
			TableStatus: t.TableStatus,
		}
		resolvedTxnEvents = append(resolvedTxnEvents, txnEvent)
		t.txnEventProgressTracker.addEvent(t.txnID)
		t.txnID++
	}
	t.txnEventProgressTracker.addResolvedTs(t.txnID, resolvedTs)
	t.txnID++
	t.backendSink.WriteTxnEvents(resolvedTxnEvents...)
}

func (t *txnEventTableSink) GetCheckpointTs() model.ResolvedTs {
	return t.txnEventProgressTracker.minTs()
}

func (t *txnEventTableSink) Close() {
	// TODO implement me
	panic("implement me")
}
