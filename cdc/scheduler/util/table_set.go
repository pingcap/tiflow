// Copyright 2021 PingCAP, Inc.
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

package util

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

// TableSet provides a data structure to store the tables' states for the
// scheduler.
type TableSet struct {
	// all tables' records
	tableIDMap map[model.TableID]*TableRecord

	// a non-unique index to facilitate looking up tables
	// assigned to a given capture.
	captureIndex map[model.CaptureID]map[model.TableID]*TableRecord
}

// TableRecord is a record to be inserted into TableSet.
type TableRecord struct {
	TableID   model.TableID
	CaptureID model.CaptureID
	Status    TableStatus
}

// Clone returns a copy of the TableSet.
// This method is future-proof in case we add
// something not trivially copyable.
func (r *TableRecord) Clone() *TableRecord {
	return &TableRecord{
		TableID:   r.TableID,
		CaptureID: r.CaptureID,
		Status:    r.Status,
	}
}

// TableStatus is a type representing the table's replication status.
type TableStatus int32

const (
	AddingTable = TableStatus(iota) + 1
	RemovingTable
	RunningTable
)

// NewTableSet creates a new TableSet.
func NewTableSet() *TableSet {
	return &TableSet{
		tableIDMap:   map[model.TableID]*TableRecord{},
		captureIndex: map[model.CaptureID]map[model.TableID]*TableRecord{},
	}
}

// AddTableRecord inserts a new TableRecord.
// It returns true if it succeeds. Returns false if there is a duplicate.
func (s *TableSet) AddTableRecord(record *TableRecord) (successful bool) {
	if _, ok := s.tableIDMap[record.TableID]; ok {
		// duplicate tableID
		return false
	}
	recordCloned := record.Clone()
	s.tableIDMap[record.TableID] = recordCloned

	captureIndexEntry := s.captureIndex[record.CaptureID]
	if captureIndexEntry == nil {
		captureIndexEntry = make(map[model.TableID]*TableRecord)
		s.captureIndex[record.CaptureID] = captureIndexEntry
	}

	captureIndexEntry[record.TableID] = recordCloned
	return true
}

// UpdateTableRecord updates an existing TableRecord.
// All modifications to a table's status should be done by this method.
func (s *TableSet) UpdateTableRecord(record *TableRecord) (successful bool) {
	oldRecord, ok := s.tableIDMap[record.TableID]
	if !ok {
		// table does not exist
		return false
	}

	// If there is no need to modify the CaptureID, we simply
	// update the record.
	if record.CaptureID == oldRecord.CaptureID {
		recordCloned := record.Clone()
		s.tableIDMap[record.TableID] = recordCloned
		s.captureIndex[record.CaptureID][record.TableID] = recordCloned
		return true
	}

	// If the CaptureID is changed, we do a proper RemoveTableRecord followed
	// by AddTableRecord.
	if record.CaptureID != oldRecord.CaptureID {
		if ok := s.RemoveTableRecord(record.TableID); !ok {
			log.Panic("unreachable", zap.Any("record", record))
		}
		if ok := s.AddTableRecord(record); !ok {
			log.Panic("unreachable", zap.Any("record", record))
		}
	}
	return true
}

// GetTableRecord tries to obtain a record with the specified tableID.
func (s *TableSet) GetTableRecord(tableID model.TableID) (*TableRecord, bool) {
	rec, ok := s.tableIDMap[tableID]
	if ok {
		return rec.Clone(), ok
	}
	return nil, false
}

// RemoveTableRecord removes the record with tableID. Returns false
// if none exists.
func (s *TableSet) RemoveTableRecord(tableID model.TableID) bool {
	record, ok := s.tableIDMap[tableID]
	if !ok {
		return false
	}
	delete(s.tableIDMap, record.TableID)

	captureIndexEntry, ok := s.captureIndex[record.CaptureID]
	if !ok {
		log.Panic("unreachable", zap.Int64("table-id", tableID))
	}
	delete(captureIndexEntry, record.TableID)
	if len(captureIndexEntry) == 0 {
		delete(s.captureIndex, record.CaptureID)
	}
	return true
}

// RemoveTableRecordByCaptureID removes all table records associated with
// captureID.
func (s *TableSet) RemoveTableRecordByCaptureID(captureID model.CaptureID) []*TableRecord {
	captureIndexEntry, ok := s.captureIndex[captureID]
	if !ok {
		return nil
	}

	var ret []*TableRecord
	for tableID, record := range captureIndexEntry {
		delete(s.tableIDMap, tableID)
		// Since the record has been removed,
		// there is no need to clone it before returning.
		ret = append(ret, record)
	}
	delete(s.captureIndex, captureID)
	return ret
}

// CountTableByCaptureID counts the number of tables associated with the captureID.
func (s *TableSet) CountTableByCaptureID(captureID model.CaptureID) int {
	return len(s.captureIndex[captureID])
}

// GetDistinctCaptures counts distinct captures with tables.
func (s *TableSet) GetDistinctCaptures() []model.CaptureID {
	var ret []model.CaptureID
	for captureID := range s.captureIndex {
		ret = append(ret, captureID)
	}
	return ret
}

// GetAllTables returns all stored information on all tables.
func (s *TableSet) GetAllTables() map[model.TableID]*TableRecord {
	ret := make(map[model.TableID]*TableRecord)
	for tableID, record := range s.tableIDMap {
		ret[tableID] = record.Clone()
	}
	return ret
}

// GetAllTablesGroupedByCaptures returns all stored information grouped by associated CaptureID.
func (s *TableSet) GetAllTablesGroupedByCaptures() map[model.CaptureID]map[model.TableID]*TableRecord {
	ret := make(map[model.CaptureID]map[model.TableID]*TableRecord)
	for captureID, tableIDMap := range s.captureIndex {
		tableIDMapCloned := make(map[model.TableID]*TableRecord)
		for tableID, record := range tableIDMap {
			tableIDMapCloned[tableID] = record.Clone()
		}
		ret[captureID] = tableIDMapCloned
	}
	return ret
}

// CountTableByStatus counts the number of tables with the given status.
func (s *TableSet) CountTableByStatus(status TableStatus) (count int) {
	for _, record := range s.tableIDMap {
		if record.Status == status {
			count++
		}
	}
	return
}
