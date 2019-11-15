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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/tidb/types"
)

type KVEntry interface {
}

type RowKVEntry struct {
	Ts       uint64
	TableID  int64
	RecordID int64
	Delete   bool
	Row      map[int64]types.Datum
}

type IndexKVEntry struct {
	Ts         uint64
	TableID    int64
	IndexID    int64
	Delete     bool
	IndexValue []types.Datum
	RecordID   int64
}

type DDLJobKVEntry struct {
	Ts    uint64
	JobID int64
	Job   *model.Job
}

type UpdateTableKVEntry struct {
	Ts        uint64
	DbID      int64
	TableID   int64
	TableInfo *model.TableInfo
}

type UnknownKVEntry struct {
	kv.RawKVEntry
}

func (idx *IndexKVEntry) Unflatten(tableInfo *model.TableInfo, loc *time.Location) error {
	if tableInfo.ID != idx.TableID {
		return errors.New("wrong table info in Unflatten")
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

func isDistinct(index *model.IndexInfo, indexValue []types.Datum) bool {
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

func (row *RowKVEntry) Unflatten(tableInfo *model.TableInfo, loc *time.Location) error {
	if tableInfo.ID != row.TableID {
		return errors.New("wrong table info in Unflatten")
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

func Unmarshal(raw *kv.RawKVEntry) (KVEntry, error) {
	switch {
	case bytes.HasPrefix(raw.Key, tablePrefix):
		return unmarshalTableKVEntry(raw)
	case bytes.HasPrefix(raw.Key, metaPrefix) && raw.OpType == kv.OpTypePut:
		return unmarshalMetaKVEntry(raw)
	}
	return &UnknownKVEntry{*raw}, nil
}

func unmarshalTableKVEntry(raw *kv.RawKVEntry) (KVEntry, error) {
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
		return &RowKVEntry{
			Ts:       raw.Ts,
			TableID:  tableID,
			RecordID: recordID,
			Delete:   raw.OpType == kv.OpTypeDelete,
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
		return &IndexKVEntry{
			Ts:         raw.Ts,
			TableID:    tableID,
			IndexID:    indexID,
			IndexValue: indexValue,
			Delete:     raw.OpType == kv.OpTypeDelete,
			RecordID:   recordID,
		}, nil

	}
	return &UnknownKVEntry{*raw}, nil
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

func unmarshalMetaKVEntry(raw *kv.RawKVEntry) (KVEntry, error) {
	meta, err := decodeMetaKey(raw.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	switch meta.GetType() {
	case ListData:
		k := meta.(MetaListData)
		if k.key == ddlJobListKey && raw.OpType == kv.OpTypePut {
			job := &model.Job{}
			err := json.Unmarshal(raw.Value, job)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if job.IsDone() {
				// FinishedTS is only set when the job is synced,
				// but we can use the entry's ts here
				job.BinlogInfo.FinishedTS = raw.Ts
				return &DDLJobKVEntry{
					Ts:    raw.Ts,
					JobID: int64(job.ID),
					Job:   job,
				}, nil
			}
		}
	case HashData:
		k := meta.(MetaHashData)
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
				tableInfo := &model.TableInfo{}
				err = json.Unmarshal(raw.Value, tableInfo)
				if err != nil {
					return nil, errors.Annotatef(err, "data: %v", raw.Value)
				}
				return &UpdateTableKVEntry{
					Ts:        raw.Ts,
					DbID:      dbID,
					TableID:   tableID,
					TableInfo: tableInfo,
				}, nil
			}
		}
	}
	return &UnknownKVEntry{*raw}, nil
}
