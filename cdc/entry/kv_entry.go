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
	"encoding/binary"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/tidb/types"
)

type kvEntry interface {
}

type rowKVEntry struct {
	Ts       uint64
	TableID  int64
	RecordID int64
	Delete   bool
	Row      map[int64]types.Datum
}

type indexKVEntry struct {
	Ts         uint64
	TableID    int64
	IndexID    int64
	Delete     bool
	IndexValue []types.Datum
	RecordID   int64
}

type ddlJobKVEntry struct {
	Ts    uint64
	JobID int64
	Job   *timodel.Job
}

type updateTableKVEntry struct {
	Ts        uint64
	DbID      int64
	TableID   int64
	TableInfo *timodel.TableInfo
}

type unknownKVEntry struct {
	model.RawKVEntry
}

func (idx *indexKVEntry) unflatten(tableInfo *timodel.TableInfo, loc *time.Location) error {
	if tableInfo.ID != idx.TableID {
		return errors.New("wrong table info in unflatten")
	}
	index := tableInfo.Indices[idx.IndexID-1]
	if !isDistinct(index, idx.IndexValue) {
		idx.RecordID = idx.IndexValue[len(idx.IndexValue)-1].GetInt64()
		idx.IndexValue = idx.IndexValue[:len(idx.IndexValue)-1]
	}
	for i, v := range idx.IndexValue {
		colOffset := index.Columns[i].Offset
		fieldType := &tableInfo.Columns[colOffset].FieldType
		datum, err := unflatten(v, fieldType, loc)
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

func (row *rowKVEntry) unflatten(tableInfo *timodel.TableInfo, loc *time.Location) error {
	if tableInfo.ID != row.TableID {
		return errors.New("wrong table info in unflatten")
	}
	for i, v := range row.Row {
		fieldType := &tableInfo.Columns[i-1].FieldType
		datum, err := unflatten(v, fieldType, loc)
		if err != nil {
			return errors.Trace(err)
		}
		row.Row[i] = datum
	}
	return nil
}

func unmarshal(raw *model.RawKVEntry) (kvEntry, error) {
	switch {
	case bytes.HasPrefix(raw.Key, tablePrefix):
		return unmarshalTableKVEntry(raw)
	case bytes.HasPrefix(raw.Key, metaPrefix) && raw.OpType == model.OpTypePut:
		return unmarshalMetaKVEntry(raw)
	}
	return &unknownKVEntry{*raw}, nil
}

func unmarshalTableKVEntry(raw *model.RawKVEntry) (kvEntry, error) {
	key, tableID, err := decodeTableID(raw.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	switch {
	case bytes.HasPrefix(key, recordPrefix):
		key, recordID, err := decodeRecordID(key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(key) != 0 {
			return nil, errors.New("invalid record key")
		}
		row, err := decodeRow(raw.Value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return &rowKVEntry{
			Ts:       raw.Ts,
			TableID:  tableID,
			RecordID: recordID,
			Delete:   raw.OpType == model.OpTypeDelete,
			Row:      row,
		}, nil
	case bytes.HasPrefix(key, indexPrefix):
		indexID, indexValue, err := decodeIndexKey(key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var recordID int64

		if len(raw.Value) == 8 {
			// primary key or unique index
			buf := bytes.NewBuffer(raw.Value)
			err = binary.Read(buf, binary.BigEndian, &recordID)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		return &indexKVEntry{
			Ts:         raw.Ts,
			TableID:    tableID,
			IndexID:    indexID,
			IndexValue: indexValue,
			Delete:     raw.OpType == model.OpTypeDelete,
			RecordID:   recordID,
		}, nil

	}
	return &unknownKVEntry{*raw}, nil
}

const (
	ddlJobListKey    = "DDLJobList"
	ddlJobAddIDxList = "DDLJobAddIDxList"
	ddlJobHistoryKey = "DDLJobHistory"
	ddlJobReorgKey   = "DDLJobReorg"

	dbMetaPrefix    = "DB:"
	tableMetaPrefix = "Table:"
)

var (
	dbMetaPrefixLen    = len(dbMetaPrefix)
	tableMetaPrefixLen = len(tableMetaPrefix)
)

func unmarshalMetaKVEntry(raw *model.RawKVEntry) (kvEntry, error) {
	meta, err := decodeMetaKey(raw.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	switch meta.getType() {
	case ListData:
		k := meta.(metaListData)
		if k.key == ddlJobListKey && raw.OpType == model.OpTypePut {
			job := &timodel.Job{}
			err := json.Unmarshal(raw.Value, job)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if job.IsDone() {
				// FinishedTS is only set when the job is synced,
				// but we can use the entry's ts here
				job.BinlogInfo.FinishedTS = raw.Ts
				return &ddlJobKVEntry{
					Ts:    raw.Ts,
					JobID: int64(job.ID),
					Job:   job,
				}, nil
			}
		}
	case HashData:
		k := meta.(metaHashData)
		if strings.HasPrefix(k.key, dbMetaPrefix) {
			key := k.key[len(dbMetaPrefix):]
			var tableID int64
			dbID, err := strconv.ParseInt(key, 10, 64)
			if err != nil {
				return nil, errors.Trace(err)
			}
			fieldStr := string(k.field)
			if strings.HasPrefix(fieldStr, tableMetaPrefix) {
				fieldStr = fieldStr[len(tableMetaPrefix):]
				tableID, err = strconv.ParseInt(fieldStr, 10, 64)
				if err != nil {
					return nil, errors.Trace(err)
				}
				tableInfo := &timodel.TableInfo{}
				err = json.Unmarshal(raw.Value, tableInfo)
				if err != nil {
					return nil, errors.Annotatef(err, "data: %v", raw.Value)
				}
				return &updateTableKVEntry{
					Ts:        raw.Ts,
					DbID:      dbID,
					TableID:   tableID,
					TableInfo: tableInfo,
				}, nil
			}
		}
	}
	return &unknownKVEntry{*raw}, nil
}
