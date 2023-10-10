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

package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// RunningError represents some running error from cdc components, such as processor.
type RunningError struct {
	Time    time.Time `json:"time"`
	Addr    string    `json:"addr"`
	Code    string    `json:"code"`
	Message string    `json:"message"`
}

// ShouldFailChangefeed return true if a running error contains a changefeed not retry error.
func (r RunningError) ShouldFailChangefeed() bool {
	return cerror.ShouldFailChangefeed(errors.New(r.Message + r.Code))
}

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
