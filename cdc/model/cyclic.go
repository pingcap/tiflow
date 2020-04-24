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
	"fmt"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/pkg/cyclic"
)

// Some cyclic replication implemention are here, this is required to break
// cyclic imports.
//
// TODO(neil) move it package cyclic
//
// package model imports util
//         util imports cyclic
// so cyclic can not imports model

// CyclicCreateMarkTable returns DDLs to create mark table regard to the tableID
func CyclicCreateMarkTable(tableID uint64) []*DDLEvent {
	schema, table := cyclic.MarkTableName(tableID)
	events := []*DDLEvent{
		{
			Ts:     0,
			Schema: schema,
			Table:  table,
			Query:  fmt.Sprintf("CREATE SCHEMA IF NOT EXIST %s", schema),
			Type:   model.ActionCreateSchema,
		},
		{
			Ts:     0,
			Schema: schema,
			Table:  table,
			Query: fmt.Sprintf(
				`CREATE TABLE %s.%s IF NOT EXIST
					(
					bucket         INT NOT NULL,
					replica_id BIGINT NOT NULL,
					val        BIGINT DEFAULT 0,
					PRIMARY KEY (bucket, replica_id)
				);`, schema, table),
			Type: model.ActionCreateTable,
		}}
	return events
}
