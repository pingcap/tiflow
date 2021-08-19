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

package redo

import (
	"context"

	"github.com/pingcap/ticdc/cdc/model"
)

// RowRedoLog stores one RowChangedEvent and its index column
type RowRedoLog struct {
	Row *model.RowChangedEvent `json:"row"`
	// Index is the IndexColumns field from model.RowChangedEvent, we need to persist
	// this field in order to use it when consuming redo logs. Since this filed
	// is not json encoded in model.RowChangedEvents, we store it separately here.
	Index [][]int `json:"index"`
}

// LogWriter is a writer abstraction for redo log storage layer
type LogWriter interface {
	WriteLog(ctx context.Context, tableID model.TableID, log []*RowRedoLog) (resolvedTs uint64, err error)
	// FlushLog sends resolved-ts from table pipeline to log writer, it is
	// essential to flush when a table doesn't have any row change event for
	// sometime, and the resolved ts of this table should be moved forward.
	FlushLog(ctx context.Context, tableID model.TableID, resolvedTs uint64) error

	// SendDDL, EmitCheckpointTs and EmitResolvedTs are called from owner only
	SendDDL(ctx context.Context, ddl *model.DDLEvent) error
	EmitCheckpointTs(ctx context.Context, checkpointTs uint64) error
	EmitResolvedTs(ctx context.Context, resolvedTs uint64) error

	GetTableResolvedTs(ctx context.Context, tableIDs []int64) (resolvedTsMap map[int64]uint64, err error)
}

// LogReader is a reader abstraction for redo log storage layer
type LogReader interface {
	// ResetReader setup the reader boundary
	ResetReader(ctx context.Context, startTs, endTs uint64) error

	// Read reads up to `maxNumberOfMessages` messages from current cursor.
	// The returned redo logs sorted by commit-ts
	ReadLog(ctx context.Context, maxNumberOfMessages int) ([]*RowRedoLog, error)

	// ReadDDL reads `maxNumberOfDDLs` ddl events from redo logs from current cursor
	ReadDDL(ctx context.Context, maxNumberOfDDLs int) ([]*model.DDLEvent, error)

	// ReadMeta reads meta from redo logs and returns the latest resovledTs and checkpointTs
	ReadMeta(ctx context.Context) (resolvedTs, checkpointTs uint64, err error)
}
