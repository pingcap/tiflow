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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/filter"
)

const (
	// SchemaName is the name of schema where all mark tables are created
	SchemaName string = "tidb_cdc"
	tableName  string = "repl_mark"

	// OptCyclicConfig is the key that adds to changefeed options
	// automatically is cyclic replication is on.
	OptCyclicConfig string = "_cyclic_relax_sql_mode"
)

// ReplicationConfig represents config used for cyclic replication
type ReplicationConfig struct {
	ReplicaID       uint64   `toml:"enable" json:"enable"`
	SyncDDL         bool     `toml:"sync-ddl" json:"sync-ddl"`
	IDBuckets       int      `toml:"id-buckets" json:"id-buckets"`
	FilterReplicaID []uint64 `toml:"filter-replica-ids" json:"filter-replica-ids"`
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

// NewCyclicFitler creates a DDL filter for cyclic replication.
// Mark table DDLs are always filtered. Turning off SyncDDL filters all DDLs.
func NewCyclicFitler(config *ReplicationConfig) (*filter.Filter, error) {
	if config.ReplicaID == 0 {
		return nil, nil
	}
	caseSensitive := true
	if config.SyncDDL {
		// Filter DDLs targets to cyclic.SchemaName
		rules := &filter.Rules{
			IgnoreDBs: []string{SchemaName},
		}
		cyclicFilter, err := filter.New(caseSensitive, rules)
		if err != nil {
			return nil, err
		}
		return cyclicFilter, nil
	}

	// Filter all DDLs
	rules := &filter.Rules{
		IgnoreDBs: []string{"~.*"},
	}
	cyclicFilter, err := filter.New(!caseSensitive, rules)
	if err != nil {
		return nil, err
	}
	return cyclicFilter, nil
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
func MarkTableName(tableID uint64) (schema, table string) {
	table = fmt.Sprintf("%s_%d", tableName, tableID)
	schema = SchemaName
	return
}

// TableName represents name of a table, includes table name and schema name.
// TODO(neil) it's better in package model.
type TableName struct {
	Schema, Table string
	// Table ID
	ID uint64
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
		markTable.Schema, markTable.Table = MarkTableName(table.ID)
		_, ok := markMap[markTable]
		if !ok {
			return false
		}
	}
	return true
}

// UdpateTableCyclicMark return a DML to update mark table regrad to the tableID
// bucket and clusterID.
func (*Cyclic) UdpateTableCyclicMark(tableID, bucket, clusterID uint64) string {
	return fmt.Sprintf(
		`INSERT INTO %s.%s_%d VALUES (%d, %d, 0) ON DUPLICATE KEY UPDATE val = val + 1;`,
		SchemaName, tableName, tableID, bucket, clusterID)
}

// IsMarkTable tells whether the table is a mark table or not.
func IsMarkTable(schema, table string) bool {
	if schema == SchemaName {
		return true
	}
	return strings.HasPrefix(table, tableName)
}
