// Copyright 2020 PingCAP, Inc.
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

package dispatcher

import (
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/hash"
)

type indexValueDispatcher struct {
	partitionNum int32
	hasher       *hash.PositionInertia
}

func newIndexValueDispatcher(partitionNum int32) *indexValueDispatcher {
	return &indexValueDispatcher{
		partitionNum: partitionNum,
		hasher:       hash.NewPositionInertia(),
	}
}

func (r *indexValueDispatcher) Dispatch(row *model.RowChangedEvent) int32 {
	r.hasher.Reset()
	r.hasher.Write([]byte(row.Table.Schema), []byte(row.Table.Table))
	// FIXME(leoppro): if the row events includes both pre-cols and cols
	// the dispatch logic here is wrong

	// distribute partition by rowid or unique column value
	dispatchCols := row.Columns
	if len(row.Columns) == 0 {
		dispatchCols = row.PreColumns
	}
	for name, col := range dispatchCols {
		if col.Flag.IsHandleKey() {
			r.hasher.Write([]byte(name), []byte(model.ColumnValueString(col.Value)))
		}
	}
	return int32(r.hasher.Sum32() % uint32(r.partitionNum))
}
