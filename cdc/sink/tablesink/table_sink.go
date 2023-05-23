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
)

// TableSink is the interface for table sink.
// It is used to sink data in table units.
type TableSink interface {
	// AppendRowChangedEvents appends row changed events to the table sink.
	// Usually, it is used to cache the row changed events into table sink.
	// This is a not thread-safe method. Please do not call it concurrently.
	AppendRowChangedEvents(rows ...*model.RowChangedEvent)
	// UpdateResolvedTs writes the buffered row changed events to the eventTableSink.
	// Note: This is an asynchronous and not thread-safe method.
	// Please do not call it concurrently.
	UpdateResolvedTs(resolvedTs model.ResolvedTs) error
	// GetCheckpointTs returns the current checkpoint ts of table sink.
	// For example, calculating the current progress from the statistics of the table sink.
	// This is a thread-safe method.
	GetCheckpointTs() model.ResolvedTs
	// Close closes the table sink.
	// After it returns, no more events will be sent out from this capture.
	Close()
	// AsyncClose closes the table sink asynchronously. Returns true if it's closed.
	AsyncClose() bool
}

// SinkInternalError means the error comes from sink internal.
type SinkInternalError struct {
	err error
}

// Error implements builtin `error` interface.
func (e SinkInternalError) Error() string {
	return e.err.Error()
}

// NewSinkInternalError creates a SinkInternalError.
func NewSinkInternalError(err error) SinkInternalError {
	return SinkInternalError{err}
}
