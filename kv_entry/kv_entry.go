package kv_entry

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
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
	Value    map[int64][]byte
}

type IndexKVEntry struct {
	Ts      uint64
	TableId int64
	IndexId int64
}

type DDLJobHistoryKVEntry struct {
	Ts    uint64
	JobId uint64
	Job   *model.Job
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
		// TODO: decode the row values
		return &RowKVEntry{
			Ts:       raw.Ts,
			TableId:  tableId,
			RecordId: recordId,
			Delete:   raw.OpType == OpTypeDelete,
		}, nil
	}
	return &UnknownKVEntry{*raw}, nil
}

const (
	ddlJobListKey    = "DDLJobList"
	ddlJobAddIdxList = "DDLJobAddIdxList"
	ddlJobHistoryKey = "DDLJobHistory"
	ddlJobReorgKey   = "DDLJobReorg"
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
	fmt.Printf("Meta: %s, %#v, %s\n", key, tp, field)
	return &UnknownKVEntry{*raw}, nil
}
