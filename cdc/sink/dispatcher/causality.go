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
	"encoding/binary"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/cdc/model"
)

// causalityDispatcher provides a simple mechanism to improve the concurrency of SQLs execution
// under the premise of ensuring correctness. It use all uks to detect row data conflict.
// causalityDispatcher groups sqls that maybe contain causal relationships, and syncer executes them linearly.
// if some conflicts exist in more than one groups, then syncer waits all SQLs that are grouped be executed and reset causalityDispatcher.
// this mechanism meets quiescent consistency to ensure correctness.
type causalityDispatcher struct {
	relations    map[string]int
	workerNum    int
	curWorkerIdx int
}

func newCausalityDispatcher(workerNum int) *causalityDispatcher {
	if workerNum < 0 {
		log.Error("unexpected workerNum for dispatcher, set to 1 forcely", zap.Int32("workerNum", workerNum))
		workerNum = 1
	}

	return &causalityDispatcher{
		relations:    make(map[string]int),
		workerNum:    workerNum,
		curWorkerIdx: 0,
	}
}

// Dispatch return the oriented worker index
// if return value = -1, it means current txn conflicts with multi-workers and will clear causality cache inner
// if return value > 0, it means normal worker index
func (c *causalityDispatcher) Dispatch(tbTxn *model.RawTableTxn) int32 {
	keys := genTxnKeys(tbTxn)
	if conflict, idx := causality.detectConflict(keys); conflict {
		if idx >= 0 {
			add(keys, idx)
			return idx
		}
		reset()
		return -1
	}
	add(keys, curWorkerIdx)
	workerIdx := curWorkerIdx
	curWorkerIdx++
	curWorkerIdx = curWorkerIdx % workerNum
	return workerIdx
}

func (c *causalityDispatcher) add(keys [][]byte, idx int) {
	if len(keys) == 0 {
		return
	}
	for _, key := range keys {
		c.relations[string(key)] = idx
	}
}

func (c *causalityDispatcher) reset() {
	c.relations = make(map[string]int)
	// don't need to reset curWorkerIdx
}

// detectConflict detects whether there is a conflict
func (c *causalityDispatcher) detectConflict(keys [][]byte) (bool, int) {
	if len(keys) == 0 {
		return false, 0
	}

	firstIdx := -1
	for _, key := range keys {
		if idx, ok := c.relations[string(key)]; ok {
			if firstIdx == -1 {
				firstIdx = idx
			} else if firstIdx != idx {
				return true, -1
			}
		}
	}

	return firstIdx != -1, firstIdx
}

func genTxnKeys(tbTxn *model.RawTableTxn) [][]byte {
	if len(tbTxn.Rows) == 0 {
		return nil
	}
	keysSet := make(map[string]struct{}, len(tbTxn.Rows))
	for _, row := range tbTxn.Rows {
		rowKeys := genRowKeys(row)
		for _, key := range rowKeys {
			keysSet[string(key)] = struct{}{}
		}
	}
	keys := make([][]byte, 0, len(keysSet))
	for key := range keysSet {
		keys = append(keys, []byte(key))
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
		log.Debug("use table id as the key", zap.Int64("tableID", row.Table.TableID))
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
