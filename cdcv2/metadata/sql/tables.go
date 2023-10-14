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

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	"github.com/pingcap/tiflow/pkg/security"
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
	ID        uint64               `gorm:"column:id;type:bigint(20) unsigned;primaryKey" json:"id"`
	Endpoints string               `gorm:"column:endpoints;type:text;not null" json:"endpoints"`
	Config    *security.Credential `gorm:"column:config;type:text" json:"config"`
	VersionFileds
}

// equal checks whether two UpstreamDO are equal.
func (u *UpstreamDO) equal(other *UpstreamDO) bool {
	if u != other && (u == nil || other == nil) {
		return false
	}

	if u.ID != other.ID {
		return false
	}
	if u.Endpoints != other.Endpoints {
		return false
	}

	if u.Config == other.Config {
		return true
	}

	if u.Config == nil && other.Config != nil {
		return false
	}
	if u.Config != nil && other.Config == nil {
		return false
	}

	if u.Config.CAPath != other.Config.CAPath ||
		u.Config.CertPath != other.Config.CertPath ||
		u.Config.KeyPath != other.Config.KeyPath ||
		len(u.Config.CertAllowedCN) != len(other.Config.CertAllowedCN) {
		return false
	}
	for i := range u.Config.CertAllowedCN {
		if u.Config.CertAllowedCN[i] != other.Config.CertAllowedCN[i] {
			return false
		}
	}

	return true
}

// GetKey returns the key of the upstream.
func (u *UpstreamDO) GetKey() uint64 {
	return u.ID
}

// TableName Upstream's table name
func (*UpstreamDO) TableName() string {
	return tableNameUpstream
}

// ChangefeedInfoDO mapped from table <changefeed_info>
type ChangefeedInfoDO struct {
	metadata.ChangefeedInfo
	RemovedAt *time.Time `gorm:"column:removed_at;type:datetime(6)" json:"removed_at"`
	VersionFileds
}

// GetKey returns the key of the changefeed info.
func (c *ChangefeedInfoDO) GetKey() metadata.ChangefeedUUID {
	return c.UUID
}

// TableName ChangefeedInfo's table name
func (*ChangefeedInfoDO) TableName() string {
	return tableNameChangefeedInfo
}

// ChangefeedStateDO mapped from table <changefeed_state>
type ChangefeedStateDO struct {
	metadata.ChangefeedState
	VersionFileds
}

// GetKey returns the key of the changefeed state.
func (c *ChangefeedStateDO) GetKey() metadata.ChangefeedUUID {
	return c.ChangefeedUUID
}

// TableName ChangefeedState's table name
func (*ChangefeedStateDO) TableName() string {
	return tableNameChangefeedState
}

// ScheduleDO mapped from table <schedule>
type ScheduleDO struct {
	metadata.ScheduledChangefeed
	VersionFileds
}

// GetKey returns the key of the schedule.
func (s *ScheduleDO) GetKey() metadata.ChangefeedUUID {
	return s.ChangefeedUUID
}

// TableName Schedule's table name
func (*ScheduleDO) TableName() string {
	return tableNameSchedule
}

// ProgressDO mapped from table <progress>
type ProgressDO struct {
	CaptureID model.CaptureID           `gorm:"column:capture_id;type:varchar(128);primaryKey" json:"capture_id"`
	Progress  *metadata.CaptureProgress `gorm:"column:progress;type:longtext" json:"progress"`
	VersionFileds
}

// GetKey returns the key of the progress.
func (p *ProgressDO) GetKey() model.CaptureID {
	return p.CaptureID
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

type VersionFileds struct {
	Version  uint64    `gorm:"column:version;type:bigint(20) unsigned;not null" json:"version"`
	UpdateAt time.Time `gorm:"column:update_at;type:datetime(6);not null;autoUpdateTime" json:"update_at"`
}

// IncreaseVersion increases the version of the metadata.
func (v *VersionFileds) IncreaseVersion() {
	v.Version++
}

// GetVersion returns the version of the progress.
func (v *VersionFileds) GetVersion() uint64 {
	return v.Version
}

// GetUpdateAt returns the update time of the progress.
func (v *VersionFileds) GetUpdateAt() time.Time {
	return v.UpdateAt
}
