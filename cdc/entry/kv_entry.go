// Copyright 2019 PingCAP, Inc.
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

package entry

import (
	"bytes"
	"encoding/json"

	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/tidb/types"
)

type baseKVEntry struct {
	Ts       uint64
	TableID  int64
	RecordID int64
	Delete   bool
}

type rowKVEntry struct {
	baseKVEntry
	Row map[int64]types.Datum
}

type indexKVEntry struct {
	baseKVEntry
	IndexID    int64
	IndexValue []types.Datum
}

func (idx *indexKVEntry) unflatten(tableInfo *TableInfo) error {
	if tableInfo.ID != idx.TableID {
		return errors.New("wrong table info in unflatten")
	}
	index, exist := tableInfo.GetIndexInfo(idx.IndexID)
	if !exist {
		return errors.NotFoundf("index info, indexID: %d", idx.IndexID)
	}
	if !isDistinct(index, idx.IndexValue) {
		idx.RecordID = idx.IndexValue[len(idx.IndexValue)-1].GetInt64()
		idx.IndexValue = idx.IndexValue[:len(idx.IndexValue)-1]
	}
	for i, v := range idx.IndexValue {
		colOffset := index.Columns[i].Offset
		fieldType := &tableInfo.Columns[colOffset].FieldType
		datum, err := unflatten(v, fieldType)
		if err != nil {
			return errors.Trace(err)
		}
		idx.IndexValue[i] = datum
	}
	return nil
}

func isDistinct(index *timodel.IndexInfo, indexValue []types.Datum) bool {
	if index.Primary {
		return true
	}
	if index.Unique {
		for _, value := range indexValue {
			if value.IsNull() {
				return false
			}
		}
		return true
	}
	return false
}

const ddlJobListKey = "DDLJobList"

func unmarshalDDL(raw *model.RawKVEntry) (*timodel.Job, error) {
	if raw.OpType != model.OpTypePut || !bytes.HasPrefix(raw.Key, metaPrefix) {
		return nil, nil
	}
	meta, err := decodeMetaKey(raw.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if meta.getType() != ListData {
		return nil, nil
	}
	k := meta.(metaListData)
	if k.key != ddlJobListKey {
		return nil, nil
	}
	job := &timodel.Job{}
	err = json.Unmarshal(raw.Value, job)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !job.IsDone() {
		return nil, nil
	}
	// FinishedTS is only set when the job is synced,
	// but we can use the entry's ts here
	job.BinlogInfo.FinishedTS = raw.Ts
	return job, nil
}
