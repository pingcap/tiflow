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
	"database/sql/driver"
	"encoding/json"
	"reflect"

	engineModel "github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/label"
)

// LabelSet is a type alias for label.Set.
// It adds some extra methods for gorm to scan and convert values.
type LabelSet label.Set

// Value implements the driver.Valuer interface.
func (s LabelSet) Value() (driver.Value, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return nil, errors.Errorf("failed to marshal LabelSet: %v", err)
	}
	return string(data), nil
}

// ToMap converts a LabelSet to a plain map.
func (s LabelSet) ToMap() map[string]string {
	return label.Set(s).ToMap()
}

// Scan implements the sql.Scanner interface.
func (s *LabelSet) Scan(rawInput interface{}) error {
	*s = make(LabelSet)
	if rawInput == nil {
		return nil
	}

	// As different SQL drivers might treat the JSON value differently,
	// we need to handle two cases where the JSON value is passed as a string
	// and a byte slice respectively.
	var bytes []byte
	switch input := rawInput.(type) {
	case string:
		// SQLite is this case.
		if len(input) == 0 {
			return nil
		}
		bytes = []byte(input)
	case []byte:
		// MySQL is this case.
		if len(input) == 0 {
			return nil
		}
		bytes = input
	default:
		return errors.Errorf("failed to scan LabelSet. "+
			"Expected string or []byte, got %s", reflect.TypeOf(rawInput))
	}

	if err := json.Unmarshal(bytes, s); err != nil {
		return errors.Annotate(err, "failed to unmarshal LabelSet")
	}
	return nil
}

// Executor records the information of an executor.
type Executor struct {
	Model
	ID      engineModel.ExecutorID `json:"id" gorm:"column:id;type:varchar(256) not null;uniqueIndex:uni_id"`
	Name    string                 `json:"name" gorm:"column:name;type:varchar(256) not null"`
	Address string                 `json:"address" gorm:"column:address;type:varchar(256) not null"`

	// Labels store the label set for each executor.
	Labels LabelSet `json:"labels" gorm:"column:labels;type:json"`
}

// Map is used in gorm update.
func (e *Executor) Map() map[string]interface{} {
	return map[string]interface{}{
		"id":      e.ID,
		"name":    e.Name,
		"address": e.Address,
		"labels":  e.Labels,
	}
}
