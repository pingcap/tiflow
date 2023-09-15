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

package dmlsink

import (
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/tablesink/state"
)

// TableEvent is the interface for events which can be written to sink by TableSink.
type TableEvent interface {
	// GetCommitTs returns the commit timestamp of the event.
	GetCommitTs() uint64
	// TrySplitAndSortUpdateEvent split the update to delete and insert if the unique key is updated
	// Note that sinkScheme is used to control the split behavior.
	TrySplitAndSortUpdateEvent(scheme string) error
}

// CallbackFunc is the callback function for callbackable event.
type CallbackFunc func()

// CallbackableEvent means the event can be callbacked.
// It also contains the table status.
type CallbackableEvent[E TableEvent] struct {
	Event     E
	Callback  CallbackFunc
	SinkState *state.TableSinkState
}

// GetTableSinkState returns the table sink state.
func (ce *CallbackableEvent[E]) GetTableSinkState() state.TableSinkState {
	return ce.SinkState.Load()
}

// RowChangeCallbackableEvent is the row change event which can be callbacked.
type RowChangeCallbackableEvent = CallbackableEvent[*model.RowChangedEvent]

// TxnCallbackableEvent is the txn event which can be callbacked.
type TxnCallbackableEvent = CallbackableEvent[*model.SingleTableTxn]
