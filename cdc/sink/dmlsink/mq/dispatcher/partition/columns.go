// Copyright 2023 PingCAP, Inc.
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
	"strconv"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/hash"
	"go.uber.org/zap"
)

// ColumnsDispatcher is a partition dispatcher
// which dispatches events based on the given columns.
type ColumnsDispatcher struct {
	hasher *hash.PositionInertia
	lock   sync.Mutex

	Columns []string
}

// NewColumnsDispatcher creates a ColumnsDispatcher.
func NewColumnsDispatcher(columns []string) *ColumnsDispatcher {
	return &ColumnsDispatcher{
		hasher:  hash.NewPositionInertia(),
		Columns: columns,
	}
}

// DispatchRowChangedEvent returns the target partition to which
// a row changed event should be dispatched.
func (r *ColumnsDispatcher) DispatchRowChangedEvent(row *model.RowChangedEvent, partitionNum int32) (int32, string, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.hasher.Reset()

	r.hasher.Write([]byte(row.Table.Schema), []byte(row.Table.Table))

	dispatchCols := row.Columns
	if len(dispatchCols) == 0 {
		dispatchCols = row.PreColumns
	}

	offsets, ok := row.TableInfo.OffsetsByNames(r.Columns)
	if !ok {
		log.Error("columns not found when dispatch event",
			zap.Any("tableName", row.Table),
			zap.Strings("columns", r.Columns))
		return 0, "", errors.ErrDispatcherFailed.GenWithStack(
			"columns not found when dispatch event, table: %v, columns: %v", row.Table, r.Columns)
	}

	for idx := 0; idx < len(r.Columns); idx++ {
		col := dispatchCols[offsets[idx]]
		if col == nil {
			continue
		}
		r.hasher.Write([]byte(r.Columns[idx]), []byte(model.ColumnValueString(col.Value)))
	}

	sum32 := r.hasher.Sum32()
	return int32(sum32 % uint32(partitionNum)), strconv.FormatInt(int64(sum32), 10), nil
}
