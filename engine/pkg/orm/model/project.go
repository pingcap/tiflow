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

package model

import "time"

// ProjectInfo for Multi-projects support
type ProjectInfo struct {
	Model
	ID   string `gorm:"column:id;type:varchar(128) not null;uniqueIndex:uidx_id"`
	Name string `gorm:"type:varchar(128) not null"`
}

// ProjectOperation records each operation of a project
type ProjectOperation struct {
	SeqID     uint      `gorm:"primaryKey;auto_increment"`
	ProjectID string    `gorm:"type:varchar(128) not null;index:idx_op"`
	Operation string    `gorm:"type:varchar(16) not null"`
	JobID     string    `gorm:"type:varchar(128) not null"`
	CreatedAt time.Time `gorm:"autoCreateTime;index:idx_op"`
}
