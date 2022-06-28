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

package txn

import (
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
)

// Assert EventSink[E event.TableEvent] implementation
var _ eventsink.EventSink[*model.SingleTableTxn] = (*Sink)(nil)

// Sink is the sink for SingleTableTxn.
type Sink struct{}

// WriteEvents writes events to the sink.
func (s *Sink) WriteEvents(rows ...*eventsink.TxnCallbackableEvent) {
	// TODO implement me
	panic("implement me")
}

// Close closes the sink.
func (s *Sink) Close() error {
	// TODO implement me
	panic("implement me")
}
