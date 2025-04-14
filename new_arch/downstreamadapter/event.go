// Copyright 2024 PingCAP, Inc.
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

package downstreamadapter

import (
	"encoding/binary"
	"hash/fnv"
	"log"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

type EventType int8

const (
	DDLEvent EventType = iota
	DMLEvent
)

// the all event in the same txn
type TxnEvent struct {
	eventType       EventType
	PhysicalTableID int64
	StartTs         uint64
	CommitTs        uint64
	Rows            []*model.RowChangedEvent
	tableInfo       *model.TableInfo

	conflictResolved time.Time
}

// implements causality.txnEvent interface.
func (e *TxnEvent) OnConflictResolved() {
	e.conflictResolved = time.Now()
}

// ConflictKeys implements causality.txnEvent interface.
func (e *TxnEvent) ConflictKeys() []uint64 {
	if len(e.Rows) == 0 {
		return nil
	}

	hashRes := make(map[uint64]struct{}, len(e.Rows))
	hasher := fnv.New32a()
	for _, row := range e.Rows {
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
		for iIdx, idxCol := range row.TableInfo.IndexColumnsOffset {
			key := genKeyList(row.GetColumns(), iIdx, idxCol, row.PhysicalTableID)
			if len(key) == 0 {
				continue
			}
			keys = append(keys, key)
		}
	}
	if len(row.PreColumns) != 0 {
		for iIdx, idxCol := range row.TableInfo.IndexColumnsOffset {
			key := genKeyList(row.GetPreColumns(), iIdx, idxCol, row.PhysicalTableID)
			if len(key) == 0 {
				continue
			}
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		// use table ID as key if no key generated (no PK/UK),
		// no concurrence for rows in the same table.
		log.Debug("Use table id as the key", zap.Int64("tableID", row.PhysicalTableID))
		tableKey := make([]byte, 8)
		binary.BigEndian.PutUint64(tableKey, uint64(row.PhysicalTableID))
		keys = [][]byte{tableKey}
	}
	return keys
}

func genKeyList(
	columns []*model.Column, iIdx int, colIdx []int, tableID int64,
) []byte {
	var key []byte
	for _, i := range colIdx {
		// if a column value is null, we can ignore this index
		// If the index contain generated column, we can't use this key to detect conflict with other DML,
		// Because such as insert can't specify the generated value.
		if columns[i] == nil || columns[i].Value == nil || columns[i].Flag.IsGeneratedColumn() {
			return nil
		}

		val := model.ColumnValueString(columns[i].Value)
		if columnNeeds2LowerCase(columns[i].Type, columns[i].Collation) {
			val = strings.ToLower(val)
		}

		key = append(key, []byte(val)...)
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

func columnNeeds2LowerCase(mysqlType byte, collation string) bool {
	switch mysqlType {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		return collationNeeds2LowerCase(collation)
	}
	return false
}

func collationNeeds2LowerCase(collation string) bool {
	return strings.HasSuffix(collation, "_ci")
}
