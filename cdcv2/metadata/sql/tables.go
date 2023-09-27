// Copyright 2023 PingCAP, Inc.
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

package sql

import (
	"time"

	"gorm.io/gorm"
)

const (
	tableNameUpstream        = "upstream"
	tableNameChangefeedInfo  = "changefeed_info"
	tableNameChangefeedState = "changefeed_state"
	tableNameSchedule        = "schedule"
	tableNameProgress        = "progress"
)

// UpstreamDO mapped from table <upstream>
type UpstreamDO struct {
	ID        uint64      `gorm:"column:id;type:bigint(20) unsigned;primaryKey" json:"id"`
	Endpoints string      `gorm:"column:endpoints;type:text;not null" json:"endpoints"`
	Config    *Credential `gorm:"column:config;type:text" json:"config"`
	Version   uint64      `gorm:"column:version;type:bigint(20) unsigned;not null" json:"version"`
	UpdateAt  time.Time   `gorm:"column:update_at;type:datetime(6);not null;autoUpdateTime" json:"update_at"`
}

// TableName Upstream's table name
func (*UpstreamDO) TableName() string {
	return tableNameUpstream
}

// ChangefeedInfoDO mapped from table <changefeed_info>
type ChangefeedInfoDO struct {
	UUID      uint64     `gorm:"column:uuid;type:bigint(20) unsigned;primaryKey" json:"uuid"`
	Namespace string     `gorm:"column:namespace;type:varchar(128);not null;uniqueIndex:namespace,priority:1" json:"namespace"`
	ID        string     `gorm:"column:id;type:varchar(128);not null;uniqueIndex:namespace,priority:2" json:"id"`
	RemovedAt *time.Time `gorm:"column:removed_at;type:datetime(6)" json:"removed_at"`

	UpstreamID uint64 `gorm:"column:upstream_id;type:bigint(20) unsigned;not null;index:upstream_id,priority:1" json:"upstream_id"`
	SinkURI    string `gorm:"column:sink_uri;type:text;not null" json:"sink_uri"`
	StartTs    uint64 `gorm:"column:start_ts;type:bigint(20) unsigned;not null" json:"start_ts"`
	TargetTs   uint64 `gorm:"column:target_ts;type:bigint(20) unsigned;not null" json:"target_ts"`
	// Note that pointer type is used here for compatibility with the old model, and config should never be nil in practice.
	Config *ReplicaConfig `gorm:"column:config;type:longtext;not null" json:"config"`

	Version  uint64    `gorm:"column:version;type:bigint(20) unsigned;not null" json:"version"`
	UpdateAt time.Time `gorm:"column:update_at;type:datetime(6);not null;autoUpdateTime" json:"update_at"`
}

// TableName ChangefeedInfo's table name
func (*ChangefeedInfoDO) TableName() string {
	return tableNameChangefeedInfo
}

// ChangefeedStateDO mapped from table <changefeed_state>
type ChangefeedStateDO struct {
	ChangefeedUUID uint64        `gorm:"column:changefeed_uuid;type:bigint(20) unsigned;primaryKey" json:"changefeed_uuid"`
	State          string        `gorm:"column:state;type:text;not null" json:"state"`
	Warning        *RunningError `gorm:"column:warning;type:text" json:"warning"`
	Error          *RunningError `gorm:"column:error;type:text" json:"error"`
	Version        uint64        `gorm:"column:version;type:bigint(20) unsigned;not null" json:"version"`
	UpdateAt       time.Time     `gorm:"column:update_at;type:datetime(6);not null;autoUpdateTime" json:"update_at"`
}

// TableName ChangefeedState's table name
func (*ChangefeedStateDO) TableName() string {
	return tableNameChangefeedState
}

// ScheduleDO mapped from table <schedule>
type ScheduleDO struct {
	ChangefeedUUID uint64             `gorm:"column:changefeed_uuid;type:bigint(20) unsigned;primaryKey" json:"changefeed_uuid"`
	Owner          *string            `gorm:"column:owner;type:varchar(128)" json:"owner"`
	OwnerState     string             `gorm:"column:owner_state;type:text;not null" json:"owner_state"`
	Processors     *string            `gorm:"column:processors;type:text" json:"processors"`
	TaskPosition   ChangefeedProgress `gorm:"column:task_position;type:text;not null" json:"task_position"`
	Version        uint64             `gorm:"column:version;type:bigint(20) unsigned;not null" json:"version"`
	UpdateAt       time.Time          `gorm:"column:update_at;type:datetime(6);not null;autoUpdateTime" json:"update_at"`
}

// TableName Schedule's table name
func (*ScheduleDO) TableName() string {
	return tableNameSchedule
}

// ProgressDO mapped from table <progress>
type ProgressDO struct {
	CaptureID string           `gorm:"column:capture_id;type:varchar(128);primaryKey" json:"capture_id"`
	Progress  *CaptureProgress `gorm:"column:progress;type:longtext" json:"progress"`
	Version   uint64           `gorm:"column:version;type:bigint(20) unsigned;not null" json:"version"`
	UpdateAt  time.Time        `gorm:"column:update_at;type:datetime(6);not null;autoUpdateTime" json:"update_at"`
}

// TableName Progress's table name
func (*ProgressDO) TableName() string {
	return tableNameProgress
}

// AutoMigrate checks the metadata-related tables and creates or changes the table structure
// as needed based on in-memory struct definition.
func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(
		&UpstreamDO{},
		&ChangefeedInfoDO{},
		&ChangefeedStateDO{},
		&ScheduleDO{},
		&ProgressDO{},
	)
}
