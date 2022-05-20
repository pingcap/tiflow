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
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
)

// MetaData is the metadata of dm.
type MetaData struct {
	jobStore *JobStore
	ddlStore *DDLStore
}

// NewMetaData creates a new MetaData instance
func NewMetaData(id libModel.WorkerID, kvClient metaclient.KVClient) *MetaData {
	return &MetaData{
		jobStore: NewJobStore(id, kvClient),
		ddlStore: NewDDLStore(id, kvClient),
	}
}

// JobStore returns internal jobStore
func (m *MetaData) JobStore() *JobStore {
	return m.jobStore
}

// DDLStore returns internal ddlStore
func (m *MetaData) DDLStore() *DDLStore {
	return m.ddlStore
}
