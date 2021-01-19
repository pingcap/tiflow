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

package replication

import "github.com/pingcap/ticdc/cdc/model"

type scheduler interface {
	SyncTasks(tables map[model.TableID]*tableTask)
}

type schedulerImpl struct {
	ownerState *ownerReactorState
}

func (s *schedulerImpl) SyncTasks(tables map[model.TableID]*tableTask) {
	// We do NOT want to touch these tables because they are being deleted.
	// We wait for the deletion(s) to finish before redispatching.
	pendingList := s.cleanUpOperations()
}

// cleanUpOperations returns tablesIDs of tables that are NOT suitable for immediate redispatching.
func (s *schedulerImpl) cleanUpOperations() []model.TableID {
	var pendingList []model.TableID

	for cfID, captureStatuses := range s.ownerState.TaskStatuses {
		for captureID, taskStatus := range captureStatuses {
			for tableID, operation := range taskStatus.Operation {
				if operation.Status == model.OperFinished {
					s.ownerState.CleanOperation(cfID, captureID, tableID)
				} else {
					if operation.Delete {
						pendingList = append(pendingList, tableID)
					}
				}
			}
		}
	}

	return pendingList
}
