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

	"github.com/pingcap/ticdc/cdc/model"
)

// Tabler is a table scheduler, which is used to table assignment among processors
type Tabler interface {
	// RemoveTables removes some tables synchronization from a processor, returns the p-lock and error
	RemoveTables(ctx context.Context, changefeedID, captureID string, ids []uint64) (*model.TableLock, error)

	// AddTables adds more tables to synchronize in a processor
	AddTables(ctx context.Context, changefeedID, captureID string, tables []*model.ProcessTableInfo) error

	// ReadTableCLock watches on the key of `model.SubChangeFeedInfo`, waits for the `table-c-lock` appears and return the C-lock
	ReadTableCLock(ctx context.Context, changefeedID, captureID string, plock *model.TableLock) (*model.TableLock, error)

	// ClearTableLock clears table p-lock and c-lock of the given processor
	ClearTableLock(ctx context.Context, changefeedID, captureID string) error
}
