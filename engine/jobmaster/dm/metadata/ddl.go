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
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
)

// DDL represents the state of ddls.
// TODO: implement DDL
type DDL struct {
	State
}

// DDLStore manages the state of ddls.
// Write by DDLCoordinator.
type DDLStore struct {
	*TomlStore

	id frameModel.MasterID
}

// NewDDLStore returns a new DDLStore instance
func NewDDLStore(id frameModel.MasterID, kvClient metaclient.KVClient) *DDLStore {
	ddlStore := &DDLStore{
		TomlStore: NewTomlStore(kvClient),
		id:        id,
	}
	ddlStore.TomlStore.Store = ddlStore
	return ddlStore
}

// CreateState creates an empty DDL object
func (ddlStore *DDLStore) CreateState() State {
	return &DDL{}
}

// Key returns encoded key of ddl state store
// TODO: add ddl key
func (ddlStore *DDLStore) Key() string {
	return ""
}
