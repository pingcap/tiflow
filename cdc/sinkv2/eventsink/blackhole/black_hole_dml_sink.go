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

package blackhole

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"go.uber.org/zap"
)

// Assert EventSink[E event.TableEvent] implementation
var _ eventsink.EventSink[*model.RowChangedEvent] = (*Sink)(nil)

// Sink is a black hole sink.
type Sink struct{}

// New create a black hole DML sink.
func New() *Sink {
	return &Sink{}
}

// WriteEvents log the events.
func (s *Sink) WriteEvents(rows ...*eventsink.CallbackableEvent[*model.RowChangedEvent]) error {
	for _, row := range rows {
		// NOTE: don't change the log, some tests depend on it.
		log.Debug("BlackHoleSink: WriteEvents", zap.Any("row", row.Event))
		row.Callback()
	}

	return nil
}

// Close do nothing.
func (s *Sink) Close() error {
	return nil
}
