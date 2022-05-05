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

package cyclic

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/cyclic/mark"
	"go.uber.org/zap"
)

// ExtractReplicaID extracts replica ID from the given mark row.
func ExtractReplicaID(markRow *model.RowChangedEvent) uint64 {
	for _, c := range markRow.Columns {
		if c == nil {
			continue
		}
		if c.Name == mark.CyclicReplicaIDCol {
			return c.Value.(uint64)
		}
	}
	log.Panic("bad mark table, " + mark.CyclicReplicaIDCol + " not found")
	return 0
}

// TxnMap maps start ts to txn may cross multiple tables.
type TxnMap map[uint64]map[model.TableName][]*model.RowChangedEvent

// MarkMap maps start ts to mark table rows.
// There is at most one mark table row that is modified for each transaction.
type MarkMap map[uint64]*model.RowChangedEvent

func (m MarkMap) shouldFilterTxn(startTs uint64, filterReplicaIDs []uint64, replicaID uint64) (*model.RowChangedEvent, bool) {
	markRow, markFound := m[startTs]
	if !markFound {
		return nil, false
	}
	from := ExtractReplicaID(markRow)
	if from == replicaID {
		log.Panic("cyclic replication loopback detected",
			zap.Any("markRow", markRow),
			zap.Uint64("replicaID", replicaID))
	}
	for i := range filterReplicaIDs {
		if filterReplicaIDs[i] == from {
			return markRow, true
		}
	}
	return markRow, false
}

// FilterAndReduceTxns filters duplicate txns bases on filterReplicaIDs and
// if the mark table dml is exist in the txn, this function will set the replicaID by mark table dml
// if the mark table dml is not exist, this function will set the replicaID by config
func FilterAndReduceTxns(
	txnsMap map[model.TableID][]*model.SingleTableTxn, filterReplicaIDs []uint64, replicaID uint64,
) (skippedRowCount int) {
	markMap := make(MarkMap)
	for _, txns := range txnsMap {
		if !mark.IsMarkTable(txns[0].Table.Schema, txns[0].Table.Table) {
			continue
		}
		for _, txn := range txns {
			for _, event := range txn.Rows {
				first, ok := markMap[txn.StartTs]
				if ok {
					// TiKV may emit the same row multiple times.
					if event.CommitTs != first.CommitTs ||
						event.RowID != first.RowID {
						log.Panic(
							"there should be at most one mark row for each txn",
							zap.Uint64("startTs", event.StartTs),
							zap.Any("first", first),
							zap.Any("second", event))
					}
				}
				markMap[event.StartTs] = event
			}
		}
	}
	for table, txns := range txnsMap {
		if mark.IsMarkTable(txns[0].Table.Schema, txns[0].Table.Table) {
			delete(txnsMap, table)
			for i := range txns {
				// For simplicity, we do not count mark table rows in statistics.
				skippedRowCount += len(txns[i].Rows)
			}
			continue
		}
		filteredTxns := make([]*model.SingleTableTxn, 0, len(txns))
		for _, txn := range txns {
			// Check if we should skip this event
			markRow, needSkip := markMap.shouldFilterTxn(txn.StartTs, filterReplicaIDs, replicaID)
			if needSkip {
				// Found cyclic mark, skip this event as it originly created from
				// downstream.
				skippedRowCount += len(txn.Rows)
				continue
			}
			txn.ReplicaID = replicaID
			if markRow != nil {
				txn.ReplicaID = ExtractReplicaID(markRow)
			}
			filteredTxns = append(filteredTxns, txn)
		}
		if len(filteredTxns) == 0 {
			delete(txnsMap, table)
		} else {
			txnsMap[table] = filteredTxns
		}
	}
	return
}
