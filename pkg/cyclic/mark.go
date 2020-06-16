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
	"context"
	"database/sql"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

// CreateMarkTables creates mark table regard to the table name.
//
// Note table name is only for avoid write hotspot there is *NO* guarantee
// normal tables and mark tables are one:one map.
func CreateMarkTables(ctx context.Context, tables []model.TableName, upstreamDSN string) error {
	db, err := sql.Open("mysql", upstreamDSN)
	if err != nil {
		return errors.Annotate(err, "Open upsteam database connection failed")
	}
	err = db.PingContext(ctx)
	if err != nil {
		return errors.Annotate(err, "fail to open upstream TiDB connection")
	}

	userTableCount := 0
	for _, name := range tables {
		if IsMarkTable(name.Schema, name.Table) {
			continue
		}
		userTableCount++
		schema, table := MarkTableName(name.Schema, name.Table)
		_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", schema))
		if err != nil {
			return errors.Annotate(err, "fail to create mark database")
		}
		_, err = db.ExecContext(ctx, fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s.%s
			(
				bucket INT NOT NULL,
				%s BIGINT UNSIGNED NOT NULL,
				val BIGINT DEFAULT 0,
				PRIMARY KEY (bucket, %s)
			);`, schema, table, CyclicReplicaIDCol, CyclicReplicaIDCol))
		if err != nil {
			return errors.Annotatef(err, "fail to create mark table %s", table)
		}
	}
	log.Info("create upstream mark done", zap.Int("count", userTableCount))
	return nil
}

// ExtractReplicaID extracts replica ID from the given mark row.
func ExtractReplicaID(markRow *model.RowChangedEvent) uint64 {
	val, ok := markRow.Columns[CyclicReplicaIDCol]
	if !ok {
		panic("bad mark table, " + CyclicReplicaIDCol + " not found")
	}
	return val.Value.(uint64)
}

// TxnMap maps start ts to txn may cross multiple tables.
type TxnMap map[uint64]map[model.TableName][]*model.RowChangedEvent

// MarkMap maps start ts to mark table rows.
// There is at most one mark table row that is modified for each transaction.
type MarkMap map[uint64]*model.RowChangedEvent

func (m MarkMap) shouldFilterTxn(startTs uint64, filterReplicaIDs []uint64) (*model.RowChangedEvent, bool) {
	markRow, markFound := m[startTs]
	if !markFound {
		return nil, false
	}
	replicaID := ExtractReplicaID(markRow)
	for i := range filterReplicaIDs {
		if filterReplicaIDs[i] == replicaID {
			return markRow, true
		}
	}
	return markRow, false
}

// FilterAndReduceTxns filters duplicate txns bases on filterReplicaIDs and
// if the mark table dml is exist in the txn, this functiong will set the replicaID by mark table dml
// if the mark table dml is not exist, this function will set the replicaID by config
func FilterAndReduceTxns(txnsMap map[model.TableName][]*model.Txn, filterReplicaIDs []uint64, replicaID uint64) {
	markMap := make(MarkMap)
	for table, txns := range txnsMap {
		if !IsMarkTable(table.Schema, table.Table) {
			continue
		}
		for _, txn := range txns {
			for _, event := range txn.Rows {
				first, ok := markMap[txn.StartTs]
				if ok {
					// TiKV may emit the same row multiple times.
					if event.CommitTs != first.CommitTs ||
						event.RowID != first.RowID {
						log.Fatal(
							"there should be at most one mark row for each txn",
							zap.Uint64("start-ts", event.StartTs),
							zap.Any("first", first),
							zap.Any("second", event))
					}
				}
				markMap[event.StartTs] = event
			}
		}
	}
	for table, txns := range txnsMap {
		if IsMarkTable(table.Schema, table.Table) {
			delete(txnsMap, table)
			continue
		}
		filteredTxns := make([]*model.Txn, 0, len(txns))
		for _, txn := range txns {
			// Check if we should skip this event
			markRow, needSkip := markMap.shouldFilterTxn(txn.StartTs, filterReplicaIDs)
			if needSkip {
				// Found cyclic mark, skip this event as it originly created from
				// downstream.
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
}
