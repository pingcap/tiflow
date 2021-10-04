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

import "github.com/pingcap/ticdc/cdc/model"

// StatusProvider is an interface implementing logic inside the Owner
// to provide necessary information to determine the status of all
// tables currently being replicated.
type StatusProvider interface {
	// GetTaskStatusCompat returns a data structure compatible with
	// the legacy Etcd data model. For now we need this to implement
	// API compatibility with older versions of TiCDC.
	// TODO deprecate this
	GetTaskStatusCompat() []model.CaptureTaskStatus
}

// GetTaskStatusCompat implements interface StatusProvider.
// For more see comments in the interface definition.
func (s *BaseScheduleDispatcher) GetTaskStatusCompat() (ret []model.CaptureTaskStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for captureID, captureTables := range s.tables.GetAllTablesGroupedByCaptures() {
		taskStatus := model.CaptureTaskStatus{}
		taskStatus.CaptureID = captureID
		for tableID, record := range captureTables {
			taskStatus.Tables = append(taskStatus.Tables, tableID)
			switch record.Status {
			case runningTable:
				continue
			case addingTable, removingTable:
				taskStatus.Operation[tableID] = &model.TableOperation{
					// Status is fixed to OperProcessed because the new scheduler does not
					// maintain enough information to determine whether the
					// operation has been processed or not.
					Status:     model.OperProcessed,
					// BoundaryTs is not useful anymore.
					BoundaryTs: 0,
					Delete:     record.Status == removingTable,
				}
			}
		}
		ret = append(ret, taskStatus)
	}

	return
}
