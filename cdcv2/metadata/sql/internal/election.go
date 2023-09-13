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

package internal

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"

	"github.com/pingcap/tiflow/pkg/election"
)

const TableNameElection = "election"

// Election mapped from table <election>
type Election struct {
	ID       int32  `gorm:"column:id;type:int(11);primaryKey" json:"id"`
	Version  int64  `gorm:"column:version;type:bigint(20);not null" json:"version"`
	LeaderID string `gorm:"column:leader_id;type:text;not null" json:"leader_id"`
	Record   Record `gorm:"column:record;type:text;not null" json:"record"`
}

// NewTestElectionRow creates a new election row for test.
func NewTestElectionRow(memCnt int) (e Election) {
	leaderID := rand.Intn(memCnt)
	for i := 0; i < memCnt; i++ {
		member := &election.Member{
			ID: fmt.Sprintf("capture-%d", i),
		}
		e.Record.Members = append(e.Record.Members, member)

		if i == leaderID {
			e.Record.LeaderID = member.ID
		}
	}

	e.ID = 1
	e.Version = 1
	return
}

// TableName Election's table name
func (*Election) TableName() string {
	return TableNameElection
}

type Record election.Record

// Value implements the driver.Valuer interface
func (r Record) Value() (driver.Value, error) {
	return json.Marshal(r)
}

// Scan implements the sql.Scanner interface
func (r *Record) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, r)
}
