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
	"github.com/pingcap/ticdc/cdc/model"
)

type TableSet struct {
	tableIDMap   map[model.TableID]*TableRecord
	captureIndex map[model.CaptureID]map[model.TableID]*TableRecord
}

type TableRecord struct {
	TableID   model.TableID
	CaptureID model.CaptureID
	Status    TableStatus
}

type TableStatus int32

func NewTableSet() *TableSet {
	return &TableSet{
		tableIDMap:   map[model.TableID]*TableRecord{},
		captureIndex: map[model.CaptureID]map[model.TableID]*TableRecord{},
	}
}

func (s *TableSet) AddTableRecord(record *TableRecord) (successful bool) {
	if _, ok := s.tableIDMap[record.TableID]; ok {
		// duplicate tableID
		return false
	}
	s.tableIDMap[record.TableID] = record

	captureIndexEntry := s.captureIndex[record.CaptureID]
	if captureIndexEntry == nil {
		captureIndexEntry = make(map[model.TableID]*TableRecord)
		s.captureIndex[record.CaptureID] = captureIndexEntry
	}

	captureIndexEntry[record.TableID] = record
	return true
}

func (s *TableSet) GetTableRecord(tableID model.TableID) (*TableRecord, bool) {
	ret, ok := s.tableIDMap[tableID]
	return ret, ok
}

func (s *TableSet) RemoveTableRecord(tableID model.TableID) bool {
	record, ok := s.tableIDMap[tableID]
	if !ok {
		return false
	}
	delete(s.tableIDMap, record.TableID)

	captureIndexEntry, ok := s.captureIndex[record.CaptureID]
	if !ok {
		panic("unreachable")
	}
	delete(captureIndexEntry, record.TableID)
	if len(captureIndexEntry) == 0 {
		delete(s.captureIndex, record.CaptureID)
	}
	return true
}

func (s *TableSet) RemoveTableRecordByCaptureID(captureID model.CaptureID) {
	captureIndexEntry, ok := s.captureIndex[captureID]
	if !ok {
		return
	}

	for tableID := range captureIndexEntry {
		delete(s.tableIDMap, tableID)
	}
	delete(s.captureIndex, captureID)
}

func (s *TableSet) CountTableByCaptureID(captureID model.CaptureID) int {
	return len(s.captureIndex[captureID])
}

func (s *TableSet) GetDistinctCaptures() []model.CaptureID {
	var ret []model.CaptureID
	for captureID := range s.captureIndex {
		ret = append(ret, captureID)
	}
	return ret
}

func (s *TableSet) GetAllTables() map[model.TableID]*TableRecord {
	return s.tableIDMap
}

func (s *TableSet) GetAllTablesGroupedByCaptures() map[model.CaptureID]map[model.TableID]*TableRecord {
	return s.captureIndex
}

func (s *TableSet) CountTableByStatus(status TableStatus) (count int) {
	for _, record := range s.tableIDMap {
		if record.Status == status {
			count++
		}
	}
	return
}
