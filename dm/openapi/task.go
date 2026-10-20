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

package openapi

import (
	"encoding/json"

	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

var defaultMetaSchema = "dm_meta"

// Adjust adjusts task and set default value.
func (t *Task) Adjust() error {
	if t.MetaSchema == nil {
		t.MetaSchema = &defaultMetaSchema
	}
	if t.Timezone != nil && *t.Timezone != "" {
		if _, err := utils.ParseTimeZone(*t.Timezone); err != nil {
			return err
		}
	}
	return nil
}

// FromJSON unmarshal json to task.
func (t *Task) FromJSON(data []byte) error {
	return json.Unmarshal(data, t)
}

// ToJSON marshal json to task.
func (t *Task) ToJSON() ([]byte, error) {
	return json.Marshal(t)
}

// RedactTaskStorage returns a response copy with storage credentials hidden.
// It must not change task templates or configurations used for execution.
func RedactTaskStorage(task *Task) *Task {
	if task == nil {
		return nil
	}
	result := *task
	if full := task.SourceConfig.FullMigrateConf; full != nil {
		copyFull := *full
		if full.DataDir != nil {
			dir := objstore.RedactURL(*full.DataDir)
			copyFull.DataDir = &dir
		}
		result.SourceConfig.FullMigrateConf = &copyFull
	}
	return &result
}

// RedactTaskStorageList returns diagnostic copies of a task list.
func RedactTaskStorageList(tasks []Task) []Task {
	if tasks == nil {
		return nil
	}
	result := make([]Task, len(tasks))
	for i := range tasks {
		result[i] = *RedactTaskStorage(&tasks[i])
	}
	return result
}
