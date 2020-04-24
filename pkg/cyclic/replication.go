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

// Package cyclic contains scaffolds for implementing cyclic replication
// among mutliple TiDB clusters/MySQL. It uses a mark table to identify and
// filter duplicate DMLs.
// CDC needs to watch DMLs to mark tables and ignore all DDLs to mark tables.
package cyclic

import (
	"fmt"
	"strings"

	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
)

const (
	// SchemaName is the name of schema where all mark tables are created
	schemaName string = util.CyclicSchemaName
	tableName  string = "repl_mark"
)

// Cyclic ...
type Cyclic struct {
	config util.CyclicConfig
}

// NewCyclic creates a cyclic
func NewCyclic(config *util.CyclicConfig) *Cyclic {
	if config == nil || config.ReplicaID == 0 {
		return nil
	}
	return &Cyclic{
		config: *config,
	}
}

// MarkTableName returns mark table name regards to the tableID
func (*Cyclic) MarkTableName(tableID uint64) (schema, table string) {
	table = fmt.Sprintf("%s_%d", tableName, tableID)
	schema = schemaName
	return
}

// CreateTableCyclicMark returns DDLs to create mark table regard to the tableID
func (c *Cyclic) CreateTableCyclicMark(tableID uint64) []*model.DDLEvent {
	schema, table := c.MarkTableName(tableID)
	events := []*model.DDLEvent{
		{
			Ts:     0,
			Schema: schema,
			Table:  table,
			Query:  fmt.Sprintf("CREATE SCHEMA IF NOT EXIST %s", schema),
			Type:   timodel.ActionCreateSchema,
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
			Type: timodel.ActionCreateTable,
		}}
	return events
}

// UdpateTableCyclicMark return a DML to update mark table regrad to the tableID
// bucket and clusterID.
func (*Cyclic) UdpateTableCyclicMark(tableID, bucket, clusterID uint64) string {
	return fmt.Sprintf(
		`INSERT INTO %s.%s_%d VALUES (%d, %d, 0) ON DUPLICATE KEY UPDATE val = val + 1;`,
		schemaName, tableName, tableID, bucket, clusterID)
}

// IsMarkTable tells whether the table is a mark table or not.
func (c *Cyclic) IsMarkTable(schema, table string) bool {
	if schema == schemaName {
		return true
	}
	return strings.HasPrefix(table, tableName)
}
