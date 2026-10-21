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

	location := binlog.MustZeroLocation(cfg.Flavor)
	hasTablePoint := false
	missingCheckpoint := false
	query := func(query string, args ...interface{}) (*sql.Rows, error) {
		rows, err := instance.targetDB.DB.QueryContext(ctx, query, args...)
		missingCheckpoint = conn.IsMySQLError(err, mysql.ErrNoSuchTable) || conn.IsMySQLError(err, mysql.ErrBadDB)
		return rows, terror.DBErrorAdapt(err, terror.ScopeDownstream, terror.ErrDBDriverError)
	}
	err := cputil.LoadSyncerCheckpoint(query,
		dbutil.TableName(cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name)), cfg.SourceID, cfg.Flavor, false,
		func(row cputil.SyncerCheckpointRow) error {
			if !row.IsGlobal {
				hasTablePoint = true
			} else if !binlog.IsFreshPosition(row.Location, cfg.Flavor, cfg.EnableGTID) {
				// RemoteCheckPoint.Load also ignores fresh global points.
				location = row.Location
			}
			return nil
		})
	if err != nil {
		// Only a failed query for absent metadata may fall back, not an error
		// encountered while decoding or consuming an existing result.
		if missingCheckpoint {
			return cfg.Meta, cfg.EnableGTID, nil
		}
		return nil, false, err
	}
	if cputil.IsFreshSyncerCheckpoint(location, hasTablePoint, cfg.Flavor, cfg.EnableGTID) {
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
