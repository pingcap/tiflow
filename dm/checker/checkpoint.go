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

package checker

import (
	"context"
	"database/sql"
	"fmt"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// metaPositionForCheck follows Syncer.IsFreshTask when choosing between an
// incremental task's checkpoint and configured starting position. Only read the
// checkpoint: check-task must not create metadata, and remove-meta runs later.
func (c *Checker) metaPositionForCheck(ctx context.Context, instance *mysqlInstance) (*config.Meta, bool, error) {
	cfg := instance.cfg
	if c.removeMeta || cfg.Mode != config.ModeIncrement || cfg.UseRelay ||
		(cfg.Meta != nil && cfg.Meta.BinLogName == binlog.FakeBinlogName) {
		return cfg.Meta, cfg.EnableGTID, nil
	}

	query := fmt.Sprintf("SELECT binlog_name, binlog_pos, binlog_gtid, is_global FROM %s WHERE id = ?",
		dbutil.TableName(cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name)))
	rows, err := instance.targetDB.DB.QueryContext(ctx, query, cfg.SourceID)
	if err != nil {
		if conn.IsMySQLError(err, mysql.ErrNoSuchTable) || conn.IsMySQLError(err, mysql.ErrBadDB) {
			return cfg.Meta, cfg.EnableGTID, nil
		}
		return nil, false, terror.DBErrorAdapt(err, terror.ScopeDownstream, terror.ErrDBDriverError)
	}
	defer rows.Close()

	location := binlog.MustZeroLocation(cfg.Flavor)
	hasTablePoint := false
	for rows.Next() {
		var (
			name     string
			pos      uint32
			gtidStr  sql.NullString
			isGlobal bool
		)
		if err := rows.Scan(&name, &pos, &gtidStr, &isGlobal); err != nil {
			return nil, false, terror.DBErrorAdapt(err, terror.ScopeDownstream, terror.ErrDBDriverError)
		}
		if !isGlobal {
			hasTablePoint = true
			continue
		}
		gset, err := gtid.ParserGTID(cfg.Flavor, gtidStr.String)
		if err != nil {
			return nil, false, err
		}
		point := binlog.NewLocation(gmysql.Position{Name: name, Pos: pos}, gset)
		// RemoteCheckPoint.Load also ignores fresh global points.
		if !binlog.IsFreshPosition(point, cfg.Flavor, cfg.EnableGTID) {
			location = point
		}
	}
	if err := rows.Err(); err != nil {
		return nil, false, terror.DBErrorAdapt(err, terror.ScopeDownstream, terror.ErrDBDriverError)
	}
	if binlog.IsFreshPosition(location, cfg.Flavor, cfg.EnableGTID) && !hasTablePoint {
		return cfg.Meta, cfg.EnableGTID, nil
	}
	pos := binlog.RemoveRelaySubDirSuffix(location.Position)
	enableGTID := cfg.EnableGTID
	// Syncer.adjustGlobalPointGTID reads the binlog file to fill an empty GTID
	// after enabling GTID on an existing file-position checkpoint.
	if enableGTID && gtid.CheckGTIDSetEmpty(location.GetGTID()) && pos.Name != "" && pos.Pos != 4 {
		enableGTID = false
	}
	return &config.Meta{
		BinLogName: pos.Name,
		BinLogPos:  pos.Pos,
		BinLogGTID: location.GTIDSetStr(),
	}, enableGTID, nil
}
