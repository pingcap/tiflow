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
	"strconv"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/hash"
	"go.uber.org/zap"
)

// IndexValueDispatcher is a partition dispatcher which dispatches events based on the index value.
type IndexValueDispatcher struct {
	hasher *hash.PositionInertia
	lock   sync.Mutex

	indexName string
}

// NewIndexValueDispatcher creates a IndexValueDispatcher.
func NewIndexValueDispatcher(indexName string) *IndexValueDispatcher {
	return &IndexValueDispatcher{
		hasher:    hash.NewPositionInertia(),
		indexName: indexName,
	}
}

// DispatchRowChangedEvent returns the target partition to which
// a row changed event should be dispatched.
func (r *IndexValueDispatcher) DispatchRowChangedEvent(row *model.RowChangedEvent, partitionNum int32) (int32, string, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.hasher.Reset()
	r.hasher.Write([]byte(row.Table.Schema), []byte(row.Table.Table))

	dispatchCols := row.Columns
	if len(row.Columns) == 0 {
		dispatchCols = row.PreColumns
	}

	// the most normal case, index-name is not set, use the handle key columns.
	if r.indexName == "" {
		for _, col := range dispatchCols {
			if col == nil {
				continue
			}
			if col.Flag.IsHandleKey() {
				r.hasher.Write([]byte(col.Name), []byte(model.ColumnValueString(col.Value)))
			}
		}
	} else {
		names, offsets, ok := row.IndexByName(r.indexName)
		if !ok {
			log.Error("index not found",
				zap.Any("tableName", row.Table),
				zap.String("indexName", r.indexName))
			return 0, "", errors.ErrDispatcherRuntime.GenWithStack("index not found: %s", r.indexName)
		}
		for idx := 0; idx < len(names); idx++ {
			r.hasher.Write([]byte(names[idx]), []byte(model.ColumnValueString(dispatchCols[offsets[idx]].Value)))
		}
	}

	sum32 := r.hasher.Sum32()
	return int32(sum32 % uint32(partitionNum)), strconv.FormatInt(int64(sum32), 10), nil
}
