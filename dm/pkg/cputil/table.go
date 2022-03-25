// Copyright 2020 PingCAP, Inc.
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

package cputil

// LoaderCheckpoint returns loader's checkpoint table name.
func LoaderCheckpoint(task string) string {
	return task + "_loader_checkpoint"
}

// LightningCheckpoint returns lightning's checkpoint table name.
func LightningCheckpoint(task string) string {
	return task + "_lightning_checkpoint_list"
}

// SyncerCheckpoint returns syncer's checkpoint table name.
func SyncerCheckpoint(task string) string {
	return task + "_syncer_checkpoint"
}

// SyncerShardMeta returns syncer's sharding meta table name for pessimistic.
func SyncerShardMeta(task string) string {
	return task + "_syncer_sharding_meta"
}

// SyncerOnlineDDL returns syncer's onlineddl checkpoint table name.
func SyncerOnlineDDL(task string) string {
	return task + "_onlineddl"
}

func ValidatorCheckpoint(task string) string {
	return task + "_validator_checkpoint"
}

func ValidatorPendingChange(task string) string {
	return task + "_validator_pending_change"
}

func ValidatorErrorChange(task string) string {
	return task + "_validator_error_change"
}

func ValidatorTableStatus(task string) string {
	return task + "_validator_table_status"
}
