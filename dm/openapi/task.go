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

// Redacted returns a response copy with storage credentials hidden.
// It must not be used for task templates or configurations used for execution.
func (t *Task) Redacted() *Task {
	if t == nil {
		return nil
	}
	result := *t
	if full := t.SourceConfig.FullMigrateConf; full != nil {
		copyFull := *full
		if full.DataDir != nil {
			dir := objstore.RedactURL(*full.DataDir)
			copyFull.DataDir = &dir
		}
		result.SourceConfig.FullMigrateConf = &copyFull
	}
	return &result
}

// RestoreRedacted restores storage credentials hidden by Redacted when a client
// submits back a task it read, so that editing other fields does not replace
// the stored credentials with masked values.
func (t *Task) RestoreRedacted(stored *Task) {
	if t == nil || stored == nil {
		return
	}
	full, storedFull := t.SourceConfig.FullMigrateConf, stored.SourceConfig.FullMigrateConf
	if full == nil || full.DataDir == nil || storedFull == nil || storedFull.DataDir == nil {
		return
	}
	dir, storedDir := *full.DataDir, *storedFull.DataDir
	if dir != storedDir && dir == objstore.RedactURL(storedDir) {
		full.DataDir = &storedDir
	}
}
