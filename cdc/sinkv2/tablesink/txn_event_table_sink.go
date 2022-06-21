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
	backendSink       txneventsink.TxnEventSink
	txnEventTsTracker *progressTracker
	txnBuffer         []*model.SingleTableTxn
	TableStopped      *atomic.Bool
}

func (t *txnEventTableSink) AppendRowChangedEvents(rows ...*model.RowChangedEvent) {
	// TODO implement me
	// Assemble each txn with the same startTs and the same commitTs in the order of commitTs.
	panic("implement me")
}

func (t *txnEventTableSink) UpdateResolvedTs(resolvedTs model.ResolvedTs) {
	// TODO: use real txn ID.
	var fakeTxnID uint64 = 0
	i := sort.Search(len(t.txnBuffer), func(i int) bool {
		return t.txnBuffer[i].CommitTs > resolvedTs.Ts
	})
	if i == 0 {
		return
	}
	resolvedTxns := t.txnBuffer[:i]

	for _, txn := range resolvedTxns {
		txnEvent := &txneventsink.TxnEvent{
			Txn: txn,
			Callback: func() {
				t.txnEventTsTracker.remove(fakeTxnID)
			},
			TableStopped: t.TableStopped,
		}
		t.backendSink.WriteTxnEvents(txnEvent)
		t.txnEventTsTracker.add(fakeTxnID, resolvedTs)
	}
}

func (t *txnEventTableSink) GetCheckpointTs() model.ResolvedTs {
	return t.txnEventTsTracker.minTs()
}

func (t *txnEventTableSink) Close() {
	// TODO implement me
	panic("implement me")
}
