// Copyright 2019 PingCAP, Inc.
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

package roles

import (
	"context"
)

// Tabler is a table scheduler, which is used to table assignment among processors
type Tabler interface {
	// RemoveTables removes some tables synchronization from a processor
	RemoveTables(ctx context.Context, changefeedID, captureID string, ids []uint64) error

	// AddTables adds more tables to synchronize in a processor
	AddTables(ctx context.Context, changefeedID, captureID string, ids []uint64) error

	// EvictCapture is used when we detect a processor is abnormal, then all processors on
	// the same capture will be disabled, information of the capture will be removed and
	// the tables assigned to this capture will be re assigned to other captures.
	EvictCapture(ctx context.Context, captureID string) error
}
