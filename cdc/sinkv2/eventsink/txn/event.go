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

package txn

import (
	"encoding/binary"
	"hash/fnv"
	"sort"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"go.uber.org/zap"
)

type txnEvent struct {
	*eventsink.TxnCallbackableEvent
	start            time.Time
	conflictResolved time.Time
}

func newTxnEvent(event *eventsink.TxnCallbackableEvent) *txnEvent {
	return &txnEvent{TxnCallbackableEvent: event, start: time.Now()}
}

func (e *txnEvent) OnConflictResolved() {
	e.conflictResolved = time.Now()
}

// ConflictKeys implements causality.txnEvent interface.
func (e *txnEvent) ConflictKeys(numSlots uint64) []uint64 {
	keys := genTxnKeys(e.TxnCallbackableEvent.Event)
	sort.Slice(keys, func(i, j int) bool { return keys[i]%numSlots < keys[j]%numSlots })
	return keys
}

// genTxnKeys returns hash keys for `txn`.
func genTxnKeys(txn *model.SingleTableTxn) []uint64 {
	if len(txn.Rows) == 0 {
		return nil
	}
	hashRes := make(map[uint64]struct{}, len(txn.Rows))
	hasher := fnv.New32a()
	for _, row := range txn.Rows {
		for _, key := range genRowKeys(row) {
			if n, err := hasher.Write(key); n != len(key) || err != nil {
				log.Panic("transaction key hash fail")
			}
			hashRes[uint64(hasher.Sum32())] = struct{}{}
			hasher.Reset()
		}
	}
	keys := make([]uint64, 0, len(hashRes))
	for key := range hashRes {
		keys = append(keys, key)
	}
	return keys
}

func genRowKeys(row *model.RowChangedEvent) [][]byte {
	var keys [][]byte
	if len(row.Columns) != 0 {
		for iIdx, idxCol := range row.IndexColumns {
			key := genKeyList(row.Columns, iIdx, idxCol, row.Table.TableID)
			if len(key) == 0 {
				continue
			}
			keys = append(keys, key)
		}
	}
	if len(row.PreColumns) != 0 {
		for iIdx, idxCol := range row.IndexColumns {
			key := genKeyList(row.PreColumns, iIdx, idxCol, row.Table.TableID)
			if len(key) == 0 {
				continue
			}
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		// use table ID as key if no key generated (no PK/UK),
		// no concurrence for rows in the same table.
		log.Debug("Use table id as the key", zap.Int64("tableID", row.Table.TableID))
		tableKey := make([]byte, 8)
		binary.BigEndian.PutUint64(tableKey, uint64(row.Table.TableID))
		keys = [][]byte{tableKey}
	}
	return keys
}

func genKeyList(columns []*model.Column, iIdx int, colIdx []int, tableID int64) []byte {
	var key []byte
	for _, i := range colIdx {
		// if a column value is null, we can ignore this index
		// If the index contain generated column, we can't use this key to detect conflict with other DML,
		// Because such as insert can't specified the generated value.
		if columns[i] == nil || columns[i].Value == nil || columns[i].Flag.IsGeneratedColumn() {
			return nil
		}
		key = append(key, []byte(model.ColumnValueString(columns[i].Value))...)
		key = append(key, 0)
	}
	if len(key) == 0 {
		return nil
	}
	tableKey := make([]byte, 16)
	binary.BigEndian.PutUint64(tableKey[:8], uint64(iIdx))
	binary.BigEndian.PutUint64(tableKey[8:], uint64(tableID))
	key = append(key, tableKey...)
	return key
}
