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
//
// Note for now, mark tables must be create manually.
package cyclic

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
)

const (
	// SchemaName is the name of schema where all mark tables are created
	SchemaName string = "tidb_cdc"
	tableName  string = "repl_mark"

	// CyclicReplicaIDCol is the name of replica ID in mark tables
	CyclicReplicaIDCol string = "replica_id"

	// OptCyclicConfig is the key that adds to changefeed options
	// automatically is cyclic replication is on.
	OptCyclicConfig string = "_cyclic_relax_sql_mode"
)

// ReplicationConfig represents config used for cyclic replication
type ReplicationConfig struct {
	ReplicaID       uint64   `toml:"enable" json:"enable"`
	FilterReplicaID []uint64 `toml:"filter-replica-ids" json:"filter-replica-ids"`
	IDBuckets       int      `toml:"id-buckets" json:"id-buckets"`
	SyncDDL         bool     `toml:"sync-ddl" json:"sync-ddl"`
}

// IsEnabled returns whether cyclic replication is enabled or not.
func (c *ReplicationConfig) IsEnabled() bool {
	return c != nil && c.ReplicaID != 0
}

// Marshal returns the json marshal format of a ReplicationConfig
func (c *ReplicationConfig) Marshal() (string, error) {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "", errors.Annotatef(err, "Unmarshal data: %v", c)
	}
	return string(cfg), nil
}

// Unmarshal unmarshals into *ReplicationConfig from json marshal byte slice
func (c *ReplicationConfig) Unmarshal(data []byte) error {
	return json.Unmarshal(data, c)
}

// RelaxSQLMode returns relaxed SQL mode, "STRICT_TRANS_TABLES" is removed.
func RelaxSQLMode(oldMode string) string {
	toRemove := "STRICT_TRANS_TABLES"

	if !strings.Contains(oldMode, toRemove) {
		return oldMode
	}

	// concatenated by "," like: mode1,mode2
	newMode := strings.Replace(oldMode, toRemove+",", "", -1)
	newMode = strings.Replace(newMode, ","+toRemove, "", -1)
	newMode = strings.Replace(newMode, toRemove, "", -1)
	return newMode
}

// Cyclic ...
type Cyclic struct {
	config ReplicationConfig
}

// NewCyclic creates a cyclic
func NewCyclic(config *ReplicationConfig) *Cyclic {
	if config == nil || config.ReplicaID == 0 {
		return nil
	}
	return &Cyclic{
		config: *config,
	}
}

// MarkTableName returns mark table name regards to the tableID
func MarkTableName(sourceSchema, sourceTable string) (schema, table string) {
	// TODO(neil) better unquote or just crc32 the name.
	sourceSchema = strings.Replace(sourceSchema, "`", "_", -1)
	sourceSchema = strings.Replace(sourceSchema, ".", "_", -1)
	sourceTable = strings.Replace(sourceTable, "`", "_", -1)
	sourceTable = strings.Replace(sourceTable, ".", "_", -1)

	source := strings.Join([]string{sourceSchema, sourceTable}, "_")
	table = fmt.Sprintf("%s_%s", tableName, source)
	schema = SchemaName
	return
}

// TableName represents name of a table, includes table name and schema name.
// TODO(neil) it's better in package model.
type TableName struct {
	Schema, Table string
	// Table ID
	ID int64
}

// IsTablesPaired checks if normal tables are paired with mark tables.
func IsTablesPaired(tables []TableName) bool {
	normalTables := make([]TableName, 0, len(tables)/2)
	markMap := make(map[TableName]struct{}, len(tables)/2)
	for _, table := range tables {
		if IsMarkTable(table.Schema, table.Table) {
			markMap[table] = struct{}{}
		} else {
			normalTables = append(normalTables, table)
		}
	}
	for _, table := range normalTables {
		markTable := TableName{}
		markTable.Schema, markTable.Table = MarkTableName(table.Schema, table.Table)
		_, ok := markMap[markTable]
		if !ok {
			return false
		}
	}
	return true
}

// UdpateTableCyclicMark return a DML to update mark table regrad to the tableID
// bucket and replicaID.
func (*Cyclic) UdpateTableCyclicMark(sourceSchema, sourceTable string, bucket, replicaID uint64) string {
	schema, table := MarkTableName(sourceSchema, sourceTable)
	return fmt.Sprintf(
		`INSERT INTO %s.%s VALUES (%d, %d, 0) ON DUPLICATE KEY UPDATE val = val + 1;`,
		schema, table, bucket, replicaID)
}

// FilterReplicaID return a slice of replica IDs needs to be filtered.
func (c *Cyclic) FilterReplicaID() []uint64 {
	return c.config.FilterReplicaID
}

// ReplicaID return a replica ID of this cluster.
func (c *Cyclic) ReplicaID() uint64 {
	return c.config.ReplicaID
}

// IsMarkTable tells whether the table is a mark table or not.
func IsMarkTable(schema, table string) bool {
	const quoteSchemaName string = "`" + SchemaName + "`"
	const quotetableName string = "`" + tableName

	if strings.HasPrefix(schema, SchemaName) {
		return true
	}
	if strings.HasPrefix(schema, quoteSchemaName) {
		return true
	}
	if strings.HasPrefix(table, quotetableName) {
		return true
	}
	return strings.HasPrefix(table, tableName)
}
