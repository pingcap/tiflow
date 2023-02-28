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

package partition

import (
	"sync"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/hash"
)

// IndexValueDispatcher is a partition dispatcher which dispatches events based on the index value.
type IndexValueDispatcher struct {
	hasher *hash.PositionInertia
	lock   sync.Mutex
}

// NewIndexValueDispatcher creates a IndexValueDispatcher.
func NewIndexValueDispatcher() *IndexValueDispatcher {
	return &IndexValueDispatcher{
		hasher: hash.NewPositionInertia(),
	}
}

// DispatchRowChangedEvent returns the target partition to which
// a row changed event should be dispatched.
func (r *IndexValueDispatcher) DispatchRowChangedEvent(row *model.RowChangedEvent, partitionNum int32) int32 {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.hasher.Reset()
	r.hasher.Write([]byte(row.Table.Schema), []byte(row.Table.Table))
	// FIXME(leoppro): if the row events includes both pre-cols and cols
	// the dispatch logic here is wrong

	// distribute partition by rowid or unique column value
	dispatchCols := row.Columns
	if len(row.Columns) == 0 {
		dispatchCols = row.PreColumns
	}
	for _, col := range dispatchCols {
		if col == nil {
			continue
		}
		if col.Flag.IsHandleKey() {
			r.hasher.Write([]byte(col.Name), []byte(model.ColumnValueString(col.Value)))
		}
	}
	return int32(r.hasher.Sum32() % uint32(partitionNum))
}
