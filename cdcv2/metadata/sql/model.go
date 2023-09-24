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
	"database/sql/driver"
	"encoding/json"
	"errors"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/security"
)

type ChangefeedProgress struct {
	CheckpointTs      uint64
	MinTableBarrierTs uint64
}

// Value implements the driver.Valuer interface
func (cp ChangefeedProgress) Value() (driver.Value, error) {
	return json.Marshal(cp)
}

// Scan implements the sql.Scanner interface
func (cp *ChangefeedProgress) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, cp)
}

// CaptureProgress stores the progress of all ChangeFeeds on single capture.
type CaptureProgress map[uint64]ChangefeedProgress

// Value implements the driver.Valuer interface
func (cp CaptureProgress) Value() (driver.Value, error) {
	return json.Marshal(cp)
}

// Scan implements the sql.Scanner interface
func (cp *CaptureProgress) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, cp)
}

type ReplicaConfig config.ReplicaConfig

// Value implements the driver.Valuer interface
func (c ReplicaConfig) Value() (driver.Value, error) {
	return json.Marshal(c)
}

// Scan implements the sql.Scanner interface
func (c *ReplicaConfig) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, c)
}

type RunningError model.RunningError

// Value implements the driver.Valuer interface
func (e RunningError) Value() (driver.Value, error) {
	return json.Marshal(e)
}

// Scan implements the sql.Scanner interface
func (e *RunningError) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, e)
}

type Credential security.Credential

// Value implements the driver.Valuer interface
func (c Credential) Value() (driver.Value, error) {
	return json.Marshal(c)
}

// Scan implements the sql.Scanner interface
func (c *Credential) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, c)
}
