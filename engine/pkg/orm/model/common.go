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

import (
	"time"
)

// Model defines basic fileds used in gorm
// CreatedAt/UpdatedAt will autoupdate in the gorm lib, not in sql backend
type Model struct {
	SeqID     uint      `json:"seq-id" gorm:"primaryKey;autoIncrement"`
	CreatedAt time.Time `json:"created-at"`
	UpdatedAt time.Time `json:"updated-at"`
}

// KeyValueMap alias to key value map when updating data in gorm
type KeyValueMap = map[string]interface{}
