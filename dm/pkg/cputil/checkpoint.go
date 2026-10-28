// Copyright 2026 PingCAP, Inc.
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

package cputil

import (
	"database/sql"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// SyncerCheckpointRow is a decoded row of the downstream syncer checkpoint.
// Schema, Table, ExitSafe* and TableInfo are populated only when loading details.
type SyncerCheckpointRow struct {
	Location           binlog.Location
	IsGlobal           bool
	Schema             string
	Table              string
	ExitSafeBinlogName string
	ExitSafeBinlogPos  uint32
	ExitSafeBinlogGTID sql.NullString
	TableInfo          []byte
}

// LoadSyncerCheckpoint reads checkpoint rows without initializing or modifying
// metadata. tableName must be SQL-quoted. query supplies the caller's context,
// connection, retry and metrics behavior. It must not perform metadata writes.
// With details=false only positions are fetched, avoiding table_info transfer
// during prechecks. With details=true the original syncer projection is used.
// Each row is decoded before visit; schema and safe-mode restoration stay with
// the caller. Missing databases/tables are returned as errors for the caller to
// handle, since only prechecks may treat them as an absent checkpoint.
func LoadSyncerCheckpoint(
	query func(string, ...interface{}) (*sql.Rows, error),
	tableName, sourceID, flavor string,
	details bool,
	visit func(SyncerCheckpointRow) error,
) error {
	columns := "binlog_name, binlog_pos, binlog_gtid, is_global"
	if details {
		columns = "cp_schema, cp_table, binlog_name, binlog_pos, binlog_gtid, exit_safe_binlog_name, exit_safe_binlog_pos, exit_safe_binlog_gtid, table_info, is_global"
	}
	rows, err := query("SELECT "+columns+" FROM "+tableName+" WHERE id = ?", sourceID)
	// A query adapter (e.g. syncer's failpoint) may return both rows and an error.
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}
	for rows.Next() {
		var (
			row     SyncerCheckpointRow
			pos     mysql.Position
			gtidStr sql.NullString
		)
		dest := []interface{}{&pos.Name, &pos.Pos, &gtidStr, &row.IsGlobal}
		if details {
			dest = []interface{}{
				&row.Schema, &row.Table, &pos.Name, &pos.Pos, &gtidStr,
				&row.ExitSafeBinlogName, &row.ExitSafeBinlogPos, &row.ExitSafeBinlogGTID, &row.TableInfo, &row.IsGlobal,
			}
		}
		if err := rows.Scan(dest...); err != nil {
			return terror.DBErrorAdapt(err, terror.ScopeDownstream, terror.ErrDBDriverError)
		}
		gset, err := gtid.ParserGTID(flavor, gtidStr.String)
		if err != nil {
			return err
		}
		row.Location = binlog.NewLocation(pos, gset)
		if err := visit(row); err != nil {
			return err
		}
	}
	return terror.DBErrorAdapt(rows.Err(), terror.ScopeDownstream, terror.ErrDBDriverError)
}

// IsFreshSyncerCheckpoint is shared by prechecks and Syncer.IsFreshTask. A fresh
// global point alone is insufficient: retained table points make a task non-fresh.
func IsFreshSyncerCheckpoint(global binlog.Location, hasTablePoints bool, flavor string, enableGTID bool) bool {
	return binlog.IsFreshPosition(global, flavor, enableGTID) && !hasTablePoints
}
