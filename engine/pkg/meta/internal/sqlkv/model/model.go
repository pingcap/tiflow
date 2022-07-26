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

package sqlkv

import (
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	ormModel "github.com/pingcap/tiflow/engine/pkg/orm/model"
)

const (
	// MetaKVTableName is the explicitly defined table name for the metakv
	// To avoid misinterpretation of table name when jobID is empty
	MetaKVTableName = "meta_kvs"
)

// 1. We use a cluster-specific schema(StoreConfig.Schema) to achieve cluster-level isolation
// 2. We use projectID to construct table name(e.g. $projectID_metakvs) to achieve project-level isolation
// 3. We use union columns <JobID, Key> of the metakv table as uk to achieve job-level isolation

// MetaKV is the model for business meta kv
// DON'T ADD soft-delete for metakv
type MetaKV struct {
	ormModel.Model
	metaModel.KeyValue
	JobID metaModel.JobID `gorm:"column:job_id;type:varchar(64) not null;uniqueIndex:uidx_jk,priority:1"`
}
