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

package tableevent

import (
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/atomic"
)

// TableEvent is the interface for events which can be written to sink by TableSink.
type TableEvent interface {
	// GetCommitTs returns the commit timestamp of the event.
	GetCommitTs() uint64
}

// CallbackFunc is the callback function for callbackable event.
type CallbackFunc func()

// CallbackableEvent means the event can be callbacked.
// It also contains the table status.
type CallbackableEvent[E TableEvent] struct {
	Event       E
	Callback    CallbackFunc
	TableStatus *atomic.Uint32
}

// RowChangeCallbackableEvent is the row change event which can be callbacked.
type RowChangeCallbackableEvent = CallbackableEvent[*model.RowChangedEvent]

// TxnCallbackableEvent is the txn event which can be callbacked.
type TxnCallbackableEvent = CallbackableEvent[*model.SingleTableTxn]

// Appender is the interface for appending events to buffer.
type Appender[E TableEvent] interface {
	// Append appends the event to buffer.
	Append(buffer []E, rows ...*model.RowChangedEvent) []E
}
