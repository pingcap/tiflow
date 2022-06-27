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

package eventsink

import "github.com/pingcap/tiflow/cdc/model"

// Assert Appender[E TableEvent] implementation
var _ Appender[*model.RowChangedEvent] = (*RowChangeEventAppender)(nil)

// RowChangeEventAppender is the builder for RowChangedEvent.
type RowChangeEventAppender struct{}

// Append appends the given rows to the given buffer.
func (r *RowChangeEventAppender) Append(
	buffer []*model.RowChangedEvent,
	rows ...*model.RowChangedEvent,
) []*model.RowChangedEvent {
	return append(buffer, rows...)
}
