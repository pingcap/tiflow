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
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
)

// DDL represents the state of ddls.
// TODO: implement DDL
type DDL struct{}

// DDLStore manages the state of ddls.
// Write by DDLCoordinator.
type DDLStore struct {
	*frameworkMetaStore
}

// NewDDLStore returns a new DDLStore instance
func NewDDLStore(kvClient metaModel.KVClient) *DDLStore {
	ddlStore := &DDLStore{
		frameworkMetaStore: newTOMLFrameworkMetaStore(kvClient),
	}
	ddlStore.frameworkMetaStore.stateFactory = ddlStore
	return ddlStore
}

// CreateState creates an empty DDL object
func (ddlStore *DDLStore) createState() state {
	return &DDL{}
}

// Key returns encoded key of ddl state store
// TODO: add ddl key
func (ddlStore *DDLStore) key() string {
	return ""
}
