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
	"fmt"
	"strings"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
)

// RelaxSQLMode returns relaxed SQL mode, "STRICT_TRANS_TABLES" is removed.
func RelaxSQLMode(oldMode string) string {
	toRemove := "STRICT_TRANS_TABLES"

	if !strings.Contains(oldMode, toRemove) {
		return oldMode
	}

	// concatenated by "," like: mode1,mode2
	modes := strings.Split(oldMode, ",")
	var newMode string
	for idx := range modes {
		m := modes[idx]
		if strings.Contains(m, toRemove) {
			continue
		}
		m = strings.TrimSpace(m)
		if newMode == "" {
			newMode = m
		} else {
			newMode = strings.Join([]string{newMode, modes[idx]}, ",")
		}
	}
	return newMode
}

// Cyclic wraps a cyclic config.
type Cyclic struct {
	config config.CyclicConfig
}

// UdpateSourceTableCyclicMark return a DML to update mark table regrad to
// the source table name, bucket and replicaID.
func (*Cyclic) UdpateSourceTableCyclicMark(sourceSchema, sourceTable string, bucket, replicaID uint64, startTs uint64) string {
	schema, table := mark.GetMarkTableName(sourceSchema, sourceTable)
	return fmt.Sprintf(
		`INSERT INTO %s VALUES (%d, %d, 0, %d) ON DUPLICATE KEY UPDATE val = val + 1;`,
		model.QuoteSchema(schema, table), bucket, replicaID, startTs)
}

// FilterReplicaID return a slice of replica IDs needs to be filtered.
func (c *Cyclic) FilterReplicaID() []uint64 {
	return c.config.FilterReplicaID
}

// ReplicaID return a replica ID of this cluster.
func (c *Cyclic) ReplicaID() uint64 {
	return c.config.ReplicaID
}

// NewCyclic creates a cyclic
func NewCyclic(config *config.CyclicConfig) *Cyclic {
	if config == nil || config.ReplicaID == 0 {
		return nil
	}
	return &Cyclic{
		config: *config,
	}
}

// IsTablesPaired checks if normal tables are paired with mark tables.
func IsTablesPaired(tables []model.TableName) bool {
	normalTables := make([]model.TableName, 0, len(tables)/2)
	markMap := make(map[model.TableName]struct{}, len(tables)/2)
	for _, table := range tables {
		if mark.IsMarkTable(table.Schema, table.Table) {
			markMap[table] = struct{}{}
		} else {
			normalTables = append(normalTables, table)
		}
	}
	for _, table := range normalTables {
		markTable := model.TableName{}
		markTable.Schema, markTable.Table = mark.GetMarkTableName(table.Schema, table.Table)
		_, ok := markMap[markTable]
		if !ok {
			return false
		}
	}
	return true
}
