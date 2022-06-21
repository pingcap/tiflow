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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/txneventsink"
	"go.uber.org/atomic"
)

// Assert TableSink implementation
var _ TableSink = (*txnEventTableSink)(nil)

type txnEventTableSink struct {
	backendSink       txneventsink.TxnEventSink
	txnEventTsTracker *progressTracker
	txnBuffer         map[uint64][]*model.SingleTableTxn
	TableStopped      *atomic.Bool
}

func (t *txnEventTableSink) AppendRowChangedEvents(rows ...*model.RowChangedEvent) {
	// TODO implement me
	// Append into txnBuffer by commitTs.
	panic("implement me")
}

func (t *txnEventTableSink) UpdateResolvedTs(resolvedTs model.ResolvedTs) error {
	// TODO: use real txn ID.
	var fakeTxnID uint64 = 0
	var resolvedTxnEvents []*txneventsink.TxnEvent
	for commitTs, txns := range t.txnBuffer {
		if commitTs <= resolvedTs.Ts {
			for _, txn := range txns {
				resolvedTxnEvents = append(resolvedTxnEvents, &txneventsink.TxnEvent{
					Txn: txn,
					Callback: func() {
						t.txnEventTsTracker.remove(fakeTxnID)
					},
					TableStopped: t.TableStopped,
				})
				t.txnEventTsTracker.add(fakeTxnID, resolvedTs)
			}
			delete(t.txnBuffer, commitTs)
		}
	}
	if len(resolvedTxnEvents) == 0 {
		return nil
	}

	return t.backendSink.WriteTxnEvents(resolvedTxnEvents...)
}

func (t *txnEventTableSink) GetCheckpointTs() model.ResolvedTs {
	return t.txnEventTsTracker.minTs()
}

func (t *txnEventTableSink) Close() {
	// TODO implement me
	panic("implement me")
}
