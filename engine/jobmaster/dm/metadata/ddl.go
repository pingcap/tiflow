// Copyright 2022 PingCAP, Inc.
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

package metadata

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/pingcap/tiflow/engine/pkg/adapter"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// SourceTable represents a upstream table
type SourceTable struct {
	Source string
	Schema string
	Table  string
}

// MarshalText implements json.MarshalText.
func (st SourceTable) MarshalText() (text []byte, err error) {
	type s SourceTable
	return json.Marshal(s(st))
}

// UnmarshalText implements json.UnmarshalText.
func (st *SourceTable) UnmarshalText(text []byte) error {
	type s SourceTable
	return json.Unmarshal(text, (*s)(st))
}

// TargetTable represents a downstream table.
type TargetTable struct {
	Schema string
	Table  string
}

// MarshalText implements json.MarshalText.
func (tt TargetTable) MarshalText() (text []byte, err error) {
	type t TargetTable
	return json.Marshal(t(tt))
}

// UnmarshalText implements json.UnmarshalText.
func (tt *TargetTable) UnmarshalText(text []byte) error {
	type t TargetTable
	return json.Unmarshal(text, (*t)(tt))
}

// DDLType defines the type of DDL
type DDLType uint64

// Defines DDLType here.
const (
	CreateTable DDLType = iota + 1
	DropTable
	OtherDDL
)

// DDLItem represents a DDL item
type DDLItem struct {
	TargetTable TargetTable
	SourceTable SourceTable
	// Tables is the table stmts in order before and after the DDL executed
	Tables []string
	DDLs   []string
	Type   DDLType
}

// DroppedColumns represents the state of dropped columns
type DroppedColumns struct {
	// column -> source table
	Cols map[string]map[SourceTable]struct{}
}

// DroppedColumnsStore manages the dropped columns state
type DroppedColumnsStore struct {
	*frameworkMetaStore

	mu          sync.Mutex
	targetTable TargetTable
}

// NewDroppedColumnsStore returns a new DroppedColumnsStore instance
func NewDroppedColumnsStore(kvClient metaModel.KVClient, targetTable TargetTable) *DroppedColumnsStore {
	droppedColumnStore := &DroppedColumnsStore{
		frameworkMetaStore: newJSONFrameworkMetaStore(kvClient),
		targetTable:        targetTable,
	}
	droppedColumnStore.frameworkMetaStore.stateFactory = droppedColumnStore
	return droppedColumnStore
}

// CreateState creates an empty DroppedColumns object
func (s *DroppedColumnsStore) createState() state {
	return &DroppedColumns{}
}

// Key returns encoded key of DroppedColumns state store
func (s *DroppedColumnsStore) key() string {
	// nolint:errcheck
	bs, _ := json.Marshal(s.targetTable)
	return adapter.DMDroppedColumnsKeyAdapter.Encode(string(bs))
}

// HasDroppedColumn returns whether the column is dropped before
func (s *DroppedColumnsStore) HasDroppedColumn(ctx context.Context, col string, sourceTable SourceTable) bool {
	state, err := s.Get(ctx)
	if err != nil {
		return false
	}
	droppedColumns := state.(*DroppedColumns)
	if tbs, ok := droppedColumns.Cols[col]; ok {
		if _, ok := tbs[sourceTable]; ok {
			return true
		}
	}
	return false
}

// AddDroppedColumns adds dropped columns to the state
func (s *DroppedColumnsStore) AddDroppedColumns(ctx context.Context, cols []string, sourceTable SourceTable) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.frameworkMetaStore.Get(ctx)
	droppedColumns := &DroppedColumns{}
	if err != nil {
		if errors.Cause(err) == ErrStateNotFound {
			droppedColumns = &DroppedColumns{
				Cols: make(map[string]map[SourceTable]struct{}),
			}
		} else {
			return err
		}
	} else {
		droppedColumns = state.(*DroppedColumns)
	}
	for _, col := range cols {
		if _, ok := droppedColumns.Cols[col]; !ok {
			droppedColumns.Cols[col] = make(map[SourceTable]struct{})
		}
		droppedColumns.Cols[col][sourceTable] = struct{}{}
	}
	return s.frameworkMetaStore.Put(ctx, droppedColumns)
}

// DelDroppedColumn deletes dropped column from the state
func (s *DroppedColumnsStore) DelDroppedColumn(ctx context.Context, col string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.frameworkMetaStore.Get(ctx)
	if err != nil {
		if errors.Cause(err) == ErrStateNotFound {
			return nil
		}
		return err
	}
	droppedColumns := state.(*DroppedColumns)
	delete(droppedColumns.Cols, col)

	if len(droppedColumns.Cols) == 0 {
		return s.frameworkMetaStore.Delete(ctx)
	}
	return s.frameworkMetaStore.Put(ctx, droppedColumns)
}

// DelDroppedColumnForTable deletes dropped column for one source table
func (s *DroppedColumnsStore) DelDroppedColumnForTable(ctx context.Context, sourceTable SourceTable) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.frameworkMetaStore.Get(ctx)
	if err != nil {
		if errors.Cause(err) == ErrStateNotFound {
			return nil
		}
		return err
	}
	droppedColumns := state.(*DroppedColumns)
	for col := range droppedColumns.Cols {
		delete(droppedColumns.Cols[col], sourceTable)
		if len(droppedColumns.Cols[col]) == 0 {
			delete(droppedColumns.Cols, col)
		}
	}

	if len(droppedColumns.Cols) == 0 {
		return s.frameworkMetaStore.Delete(ctx)
	}
	return s.frameworkMetaStore.Put(ctx, droppedColumns)
}

// DelAllDroppedColumns deletes all dropped columns in metadata.
func DelAllDroppedColumns(ctx context.Context, kvClient metaModel.KVClient) error {
	_, err := kvClient.Delete(ctx, adapter.DMDroppedColumnsKeyAdapter.Path(), metaModel.WithPrefix())
	return err
}
