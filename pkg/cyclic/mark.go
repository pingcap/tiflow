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
	"fmt"
	"sort"

	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
)

// CreateMarkTable returns DDLs to create mark table regard to the tableID
//
// Note table ID is only for avoid write hotspot there is *NO* guarantee
// normal tables and mark tables are one:one map.
//
// For now, it's dead code. We need create table on changefeed creation.
func CreateMarkTable(sourceSchema, sourceTable string) []*model.DDLEvent {
	schema, table := MarkTableName(sourceSchema, sourceTable)
	events := []*model.DDLEvent{
		{
			Ts:     0,
			Schema: schema,
			Table:  table,
			Query:  fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", schema),
			Type:   timodel.ActionCreateSchema,
		},
		{
			Ts:     0,
			Schema: schema,
			Table:  table,
			Query: fmt.Sprintf(
				`CREATE TABLE IF NOT EXISTS %s.%s
					(
					bucket INT NOT NULL,
					%s BIGINT UNSIGNED NOT NULL,
					val BIGINT DEFAULT 0,
					PRIMARY KEY (bucket, %s)
				);`, schema, table, CyclicReplicaIDCol, CyclicReplicaIDCol),
			Type: timodel.ActionCreateTable,
		}}
	return events
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

// MapMarkRowsGroup maps row change events into two group, normal table group
// and mark table group.
func MapMarkRowsGroup(
	inputs map[model.TableName][][]*model.RowChangedEvent,
) (output TxnMap, markMap MarkMap) {
	output = make(map[uint64]map[model.TableName][]*model.RowChangedEvent)
	markMap = make(map[uint64]*model.RowChangedEvent)
	for name, input := range inputs {
		for _, events := range input {
			// Group mark table rows by start ts
			if IsMarkTable(name.Schema, name.Table) {
				for _, event := range events {
					_, ok := markMap[event.StartTs]
					if ok {
						panic(fmt.Sprintf(
							"there should be at most one mark row for each txn, start ts %d",
							event.StartTs,
						))
					}
					markMap[event.StartTs] = event
				}
				continue
			}
			for _, event := range events {
				sameTxn, txnFound := output[event.StartTs]
				if !txnFound {
					sameTxn = make(map[model.TableName][]*model.RowChangedEvent)
				}
				table, tableFound := sameTxn[name]
				if !tableFound {
					table = make([]*model.RowChangedEvent, 0, 1)
				}
				table = append(table, event)
				sameTxn[name] = table
				output[event.StartTs] = sameTxn
			}
		}
	}
	return
}

// ReduceCyclicRowsGroup filters duplicate rows bases on filterReplicaIDs and
// join mark table rows and normal table rows into one group if start ts is
// the same.
func ReduceCyclicRowsGroup(
	input TxnMap, markMap MarkMap, filterReplicaIDs []uint64,
) map[model.TableName][][]*model.RowChangedEvent {
	output := make(map[model.TableName][][]*model.RowChangedEvent, len(input))

	for startTs, txn := range input {
		// Check if we should skip this event
		markRow, needSkip := markMap.shouldFilterTxn(startTs, filterReplicaIDs)
		if needSkip {
			// Found cyclic mark, skip this event as it originly created from
			// downstream.
			continue
		}

		for name, events := range txn {
			if startTs != events[0].StartTs {
				panic(fmt.Sprintf(
					"start ts mismatch %d != %d", startTs, events[0].StartTs))
			}
			multiRows := make([][]*model.RowChangedEvent, 0, 1)
			rows := make([]*model.RowChangedEvent, 0, len(events)+1)
			if markRow != nil {
				var mark model.RowChangedEvent = *markRow
				// Rewrite mark table name based on event's table ID.
				schema, table := MarkTableName(name.Schema, name.Table)
				mark.Table = &model.TableName{
					Schema: schema,
					Table:  table,
				}
				rows = append(rows, &mark)
			}
			rows = append(rows, events...)

			if rs, ok := output[name]; ok {
				multiRows = append(rs, rows)
			} else {
				multiRows = append(multiRows, rows)
			}
			output[name] = multiRows
		}
	}
	for _, events := range output {
		// Per table order may lose during map stage, we need to sort events.
		sort.Slice(events, func(i, j int) bool { return events[i][0].CommitTs < events[j][0].CommitTs })
	}
	return output
}
