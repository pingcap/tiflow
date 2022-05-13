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

package internal

import (
	"github.com/pingcap/tiflow/cdc/model"
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
