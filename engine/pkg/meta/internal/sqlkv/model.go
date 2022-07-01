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
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	"gorm.io/gorm"
)

// 1. We use unique cluster uuid to construct the schema to achieve cluster isolation
// 2. We use unique projectID to construct table name to achieve project isolation
// 3. We use <jobID, Key> as uk to achieve job-level isolation

// JobID is the alias of model.JobID
type JobID = model.JobID

// MetaKV is the model for backend user meta table
// NOTE: SHOULD pay attention to the doft-delete and hard-delete
// USE db.Unscoped() to achieve hard-delete
type MetaKV struct {
	gorm.Model
	metaclient.KeyValue
	JobID JobID `gorm:"column:job_id;type:varchar(64) not null;uniqueIndex:uidx_jk,priority:1"`
}
