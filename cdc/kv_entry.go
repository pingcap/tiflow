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

package cdc

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/types"
	"strconv"
	"strings"
)

type OpType int

const (
	OpTypeUnknow OpType = 0
	OpTypePut    OpType = 1
	OpTypeDelete OpType = 2
	OpResolvedTS OpType = 3
)

type RawKVEntry struct {
	OpType OpType
	Key    []byte
	Value  []byte
	Ts     uint64
}

type KVEntry interface {
}

type RowKVEntry struct {
	Ts       uint64
	TableId  int64
	RecordId int64
	Delete   bool
	Row      map[int64]types.Datum
}

type IndexKVEntry struct {
	Ts         uint64
	TableId    int64
	IndexId    int64
	Delete     bool
	IndexValue []types.Datum
	RecordId   int64
}

type DDLJobHistoryKVEntry struct {
	Ts    uint64
	JobId uint64
	Job   *model.Job
}

type UpdateTableKVEntry struct {
	Ts        uint64
	DbId      int64
	TableId   int64
	TableInfo *model.TableInfo
}

type ResolvedTS struct {
	Ts uint64
}

type UnknownKVEntry struct {
	RawKVEntry
}

func Unmarshal(raw *RawKVEntry) (KVEntry, error) {
	if raw.OpType == OpResolvedTS {
		return &ResolvedTS{
			Ts: raw.Ts,
		}, nil
	}
	switch {
	case hasTablePrefix(raw.Key):
		return unmarshalTableKVEntry(raw)
	case hasMetaPrefix(raw.Key):
		return unmarshalMetaKVEntry(raw)
	}
	return &UnknownKVEntry{*raw}, nil
}

func unmarshalTableKVEntry(raw *RawKVEntry) (KVEntry, error) {
	key, tableId, err := decodeTableId(raw.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	switch {
	case hasRecordPrefix(key):
		key, recordId, err := decodeRecordId(key)
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
			TableId:  tableId,
			RecordId: recordId,
			Delete:   raw.OpType == OpTypeDelete,
			Row:      row,
		}, nil
	case hasIndexPrefix(key):
		indexId, indexValue, err := decodeIndexKey(key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var recordId int64
		if len(raw.Value) > 0 {
			buf := bytes.NewBuffer(raw.Value)
			err = binary.Read(buf, binary.BigEndian, &recordId)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		return &IndexKVEntry{
			Ts:         raw.Ts,
			TableId:    tableId,
			IndexId:    indexId,
			IndexValue: indexValue,
			Delete:     raw.OpType == OpTypeDelete,
			RecordId:   recordId,
		}, nil

	}
	return &UnknownKVEntry{*raw}, nil
}

const (
	ddlJobListKey    = "DDLJobList"
	ddlJobAddIdxList = "DDLJobAddIdxList"
	ddlJobHistoryKey = "DDLJobHistory"
	ddlJobReorgKey   = "DDLJobReorg"

	dbMetaPrefix    = "DB:"
	tableMetaPrefix = "Table:"
)

var (
	dbMetaPrefixLen    = len(dbMetaPrefix)
	tableMetaPrefixLen = len(tableMetaPrefix)
)

func unmarshalMetaKVEntry(raw *RawKVEntry) (KVEntry, error) {
	key, tp, field, err := decodeMetaKey(raw.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	switch key {
	case ddlJobHistoryKey:
		if tp == HashData {
			jobId := binary.BigEndian.Uint64(field)
			job := &model.Job{}
			err = json.Unmarshal(raw.Value, job)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return &DDLJobHistoryKVEntry{
				Ts:    raw.Ts,
				JobId: jobId,
				Job:   job,
			}, nil
		}
	}
	switch {
	case strings.HasPrefix(key, dbMetaPrefix):
		key = key[len(dbMetaPrefix):]
		var tableId int64
		dbId, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			return nil, errors.Trace(err)
		}
		fieldStr := string(field)
		if tp == HashData && strings.HasPrefix(fieldStr, tableMetaPrefix) {
			fieldStr = fieldStr[len(tableMetaPrefix):]
			tableId, err = strconv.ParseInt(fieldStr, 10, 64)
			if err != nil {
				return nil, errors.Trace(err)
			}
			tableInfo := &model.TableInfo{}
			err = json.Unmarshal(raw.Value, tableInfo)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return &UpdateTableKVEntry{
				Ts:        raw.Ts,
				DbId:      dbId,
				TableId:   tableId,
				TableInfo: tableInfo,
			}, nil
		}
	}
	return &UnknownKVEntry{*raw}, nil
}
