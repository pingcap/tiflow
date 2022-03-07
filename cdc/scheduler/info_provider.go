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

package scheduler

import (
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/util"
)

// InfoProvider is the interface to get information about the internal states of the scheduler.
// We need this interface so that we can provide the information through HTTP API.
type InfoProvider interface {
	// GetTaskStatuses returns the task statuses.
	GetTaskStatuses() (map[model.CaptureID]*model.TaskStatus, error)

	// GetTaskPositions returns the task positions.
	GetTaskPositions() (map[model.CaptureID]*model.TaskPosition, error)

	// GetTotalTableCounts returns the number of tables associated
	// with each capture.
	GetTotalTableCounts() map[model.CaptureID]int

	// GetPendingTableCounts returns the number of tables in a non-ready
	// status (Adding & Removing) associated with each capture.
	GetPendingTableCounts() map[model.CaptureID]int
}

// GetTaskStatuses implements InfoProvider for BaseScheduleDispatcher.
// Complexity Note: This function has O(#tables) cost. USE WITH CARE.
// Functions with cost O(#tables) are NOT recommended for regular metrics
// collection.
func (s *BaseScheduleDispatcher) GetTaskStatuses() (map[model.CaptureID]*model.TaskStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tablesPerCapture := s.tables.GetAllTablesGroupedByCaptures()
	ret := make(map[model.CaptureID]*model.TaskStatus, len(tablesPerCapture))
	for captureID, tables := range tablesPerCapture {
		ret[captureID] = &model.TaskStatus{
			Tables:    make(map[model.TableID]*model.TableReplicaInfo),
			Operation: make(map[model.TableID]*model.TableOperation),
		}
		for tableID, record := range tables {
			ret[captureID].Tables[tableID] = &model.TableReplicaInfo{
				StartTs: 0, // We no longer maintain this information
			}
			switch record.Status {
			case util.RunningTable:
				continue
			case util.AddingTable:
				ret[captureID].Operation[tableID] = &model.TableOperation{
					Delete:     false,
					Status:     model.OperDispatched,
					BoundaryTs: 0, // We no longer maintain this information
				}
			case util.RemovingTable:
				ret[captureID].Operation[tableID] = &model.TableOperation{
					Delete:     true,
					Status:     model.OperDispatched,
					BoundaryTs: 0, // We no longer maintain this information
				}
			}
		}
	}

	return ret, nil
}

// GetTaskPositions implements InfoProvider for BaseScheduleDispatcher.
// Complexity Note: This function has O(#captures) cost.
func (s *BaseScheduleDispatcher) GetTaskPositions() (map[model.CaptureID]*model.TaskPosition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ret := make(map[model.CaptureID]*model.TaskPosition, len(s.captureStatus))
	for captureID, captureStatus := range s.captureStatus {
		ret[captureID] = &model.TaskPosition{
			CheckPointTs: captureStatus.CheckpointTs,
			ResolvedTs:   captureStatus.ResolvedTs,
		}
	}

	return ret, nil
}

// GetTotalTableCounts implements InfoProvider for BaseScheduleDispatcher.
// Complexity Note: This function has O(#captures) cost.
func (s *BaseScheduleDispatcher) GetTotalTableCounts() map[model.CaptureID]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	ret := make(map[model.CaptureID]int, len(s.captureStatus))
	for captureID := range s.captureStatus {
		ret[captureID] = s.tables.CountTableByCaptureID(captureID)
	}
	return ret
}

// GetPendingTableCounts implements InfoProvider for BaseScheduleDispatcher.
// Complexity Note: This function has O(#captures) cost.
func (s *BaseScheduleDispatcher) GetPendingTableCounts() map[model.CaptureID]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	ret := make(map[model.CaptureID]int, len(s.captureStatus))
	for captureID := range s.captureStatus {
		addCount := s.tables.CountTableByCaptureIDAndStatus(captureID, util.AddingTable)
		removeCount := s.tables.CountTableByCaptureIDAndStatus(captureID, util.RemovingTable)
		ret[captureID] = addCount + removeCount
	}
	return ret
}
