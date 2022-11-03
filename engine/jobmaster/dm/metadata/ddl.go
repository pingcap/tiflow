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

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
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
	Tables      []string
	DDLs        []string
	Type        DDLType
}

// DropColumns represents the state of drop columns
type DropColumns struct {
	state
	// column -> source table
	Cols map[string]map[SourceTable]struct{}
}

// DropColumnsStore manages the drop columns state
type DropColumnsStore struct {
	*frameworkMetaStore

	mu          sync.Mutex
	targetTable TargetTable
}

// NewDropColumnsStore returns a new DropColumnsStore instance
func NewDropColumnsStore(kvClient metaModel.KVClient, targetTable TargetTable) *DropColumnsStore {
	dropColumnStore := &DropColumnsStore{
		frameworkMetaStore: newJSONFrameworkMetaStore(kvClient),
	}
	dropColumnStore.frameworkMetaStore.stateFactory = dropColumnStore
	return dropColumnStore
}

// CreateState creates an empty DropColumns object
func (s *DropColumnsStore) createState() state {
	return &DropColumns{}
}

// Key returns encoded key of DropColumns state store
func (s *DropColumnsStore) key() string {
	// nolint:errcheck
	bs, _ := json.Marshal(s.targetTable)
	return adapter.DMDropColumnsKeyAdapter.Encode(string(bs))
}

// AddDropColumns adds drop columns to the state
func (s *DropColumnsStore) AddDropColumns(ctx context.Context, cols []string, sourceTable SourceTable) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.frameworkMetaStore.Get(ctx)
	dropColumns := &DropColumns{}
	if err != nil {
		if errors.Cause(err) == ErrStateNotFound {
			dropColumns = &DropColumns{
				Cols: make(map[string]map[SourceTable]struct{}),
			}
		} else {
			return err
		}
	} else {
		dropColumns = state.(*DropColumns)
	}
	for _, col := range cols {
		if _, ok := dropColumns.Cols[col]; !ok {
			dropColumns.Cols[col] = make(map[SourceTable]struct{})
		}
		dropColumns.Cols[col][sourceTable] = struct{}{}
	}
	return s.frameworkMetaStore.Put(ctx, dropColumns)
}

// DelDropColumn deletes drop column from the state
func (s *DropColumnsStore) DelDropColumn(ctx context.Context, col string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.frameworkMetaStore.Get(ctx)
	if err != nil {
		if errors.Cause(err) == ErrStateNotFound {
			return nil
		}
		return err
	}
	dropColumns := state.(*DropColumns)
	delete(dropColumns.Cols, col)

	if len(dropColumns.Cols) == 0 {
		return s.frameworkMetaStore.Delete(ctx)
	}
	return s.frameworkMetaStore.Put(ctx, dropColumns)
}

// DelAllDropColumns deletes all drop columns in metadata.
func DelAllDropColumns(ctx context.Context, kvClient metaModel.KVClient) error {
	_, err := kvClient.Delete(ctx, adapter.DMDropColumnsKeyAdapter.Encode(""), metaModel.WithPrefix())
	return err
}
