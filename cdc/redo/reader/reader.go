//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package reader

import (
	"context"

	"github.com/pingcap/ticdc/cdc/model"
)

// Reader is a reader abstraction for redo log storage layer
type Reader interface {
	// ResetReader setup the reader boundary
	ResetReader(ctx context.Context, startTs, endTs uint64) error

	// Read reads up to `maxNumberOfMessages` messages from current cursor.
	// The returned redo logs sorted by commit-ts
	ReadLog(ctx context.Context, maxNumberOfMessages int) ([]*model.RedoRowChangedEvent, error)

	// ReadDDL reads `maxNumberOfDDLs` ddl events from redo logs from current cursor
	ReadDDL(ctx context.Context, maxNumberOfDDLs int) ([]*model.RedoDDLEvent, error)

	// ReadMeta reads meta from redo logs and returns the latest resovledTs and checkpointTs
	ReadMeta(ctx context.Context) (resolvedTs, checkpointTs uint64, err error)
}
