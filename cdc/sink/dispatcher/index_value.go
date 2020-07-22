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
}

func (r *indexValueDispatcher) Dispatch(row *model.RowChangedEvent) int32 {
	var h hash.PositionInertia
	h.Write([]byte(row.Table.Schema), []byte(row.Table.Table))
	for name, col := range row.Columns {
		if col.Flag.IsHandleKey() {
			h.Write([]byte(name), []byte(model.ColumnValueString(col.Value)))
		}
	}
	h ^= h<<4 | h>>4
	return int32(byte(h) % byte(r.partitionNum))
}
