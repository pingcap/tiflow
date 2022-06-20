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

package txneventsink

import (
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/atomic"
)

// TxnEvent represents a transaction event with callbacks.
// In addition, it contains the state of the table.
// When we process row events, TableStopped is used to
// determine if we really need to process the event.
type TxnEvent struct {
	Txn          *model.SingleTableTxn
	Callback     func()
	TableStopped *atomic.Bool
}

// TxnEventSink is a sink that processes transaction events.
// Usually, it is a MySQL sink.
type TxnEventSink interface {
	// WriteTxnEvents writes transaction events to the sink.
	// Note: This is an asynchronous and thread-safe method.
	WriteTxnEvents(txns ...*TxnEvent) error
	// Close closes the sink.
	Close() error
}
