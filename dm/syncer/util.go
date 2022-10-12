// Copyright 2019 PingCAP, Inc.
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

package syncer

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/dumpling/export"
	dlog "github.com/pingcap/tidb/dumpling/log"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/filter"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog/common"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/relay"
)

func toBinlogType(relay relay.Process) BinlogType {
	if relay != nil {
		return LocalBinlog
	}

	return RemoteBinlog
}

func binlogTypeToString(binlogType BinlogType) string {
	switch binlogType {
	case RemoteBinlog:
		return "remote"
	case LocalBinlog:
		return "local"
	default:
		return "unknown"
	}
}

// getTableByDML gets table from INSERT/UPDATE/DELETE statement.
func getTableByDML(dml ast.DMLNode) (*filter.Table, error) {
	switch stmt := dml.(type) {
	case *ast.InsertStmt:
		if stmt.Table == nil || stmt.Table.TableRefs == nil || stmt.Table.TableRefs.Left == nil {
			return nil, terror.ErrSyncUnitInvalidTableName.Generate(fmt.Sprintf("INSERT statement %s not valid", stmt.Text()))
		}
		table, err := tableNameResultSet(stmt.Table.TableRefs.Left)
		return table, terror.Annotatef(err, "INSERT statement %s", stmt.Text())
	case *ast.UpdateStmt:
		if stmt.TableRefs == nil || stmt.TableRefs.TableRefs == nil || stmt.TableRefs.TableRefs.Left == nil {
			return nil, terror.ErrSyncUnitInvalidTableName.Generate(fmt.Sprintf("UPDATE statement %s not valid", stmt.Text()))
		}
		table, err := tableNameResultSet(stmt.TableRefs.TableRefs.Left)
		return table, terror.Annotatef(err, "UPDATE statement %s", stmt.Text())
	case *ast.DeleteStmt:
		if stmt.TableRefs == nil || stmt.TableRefs.TableRefs == nil || stmt.TableRefs.TableRefs.Left == nil {
			return nil, terror.ErrSyncUnitInvalidTableName.Generate(fmt.Sprintf("DELETE statement %s not valid", stmt.Text()))
		}
		table, err := tableNameResultSet(stmt.TableRefs.TableRefs.Left)
		return table, terror.Annotatef(err, "DELETE statement %s", stmt.Text())
	}
	return nil, terror.ErrSyncUnitNotSupportedDML.Generate(dml)
}

func tableNameResultSet(rs ast.ResultSetNode) (*filter.Table, error) {
	ts, ok := rs.(*ast.TableSource)
	if !ok {
		return nil, terror.ErrSyncUnitTableNameQuery.Generate(fmt.Sprintf("ResultSetNode %s", rs.Text()))
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		return nil, terror.ErrSyncUnitTableNameQuery.Generate(fmt.Sprintf("TableSource %s", ts.Text()))
	}
	return &filter.Table{Schema: tn.Schema.O, Name: tn.Name.O}, nil
}

// record source tbls record the tables that need to flush checkpoints.
func recordSourceTbls(sourceTbls map[string]map[string]struct{}, stmt ast.StmtNode, table *filter.Table) {
	schema, name := table.Schema, table.Name
	switch stmt.(type) {
	// these ddls' relative table checkpoints will be deleted during track ddl,
	// so we shouldn't flush these checkpoints
	case *ast.DropDatabaseStmt:
		delete(sourceTbls, schema)
	case *ast.DropTableStmt:
		if _, ok := sourceTbls[schema]; ok {
			delete(sourceTbls[schema], name)
		}
	// these ddls won't update schema tracker, no need to update them
	case *ast.LockTablesStmt, *ast.UnlockTablesStmt, *ast.CleanupTableLockStmt, *ast.TruncateTableStmt:
		break
	// flush other tables schema tracker info into checkpoint
	default:
		if _, ok := sourceTbls[schema]; !ok {
			sourceTbls[schema] = make(map[string]struct{})
		}
		sourceTbls[schema][name] = struct{}{}
	}
}

func printServerVersion(tctx *tcontext.Context, db *conn.BaseDB, scope string) {
	logger := dlog.NewAppLogger(tctx.Logger.With(zap.String("scope", scope)))
	versionInfo, err := export.SelectVersion(db.DB)
	if err != nil {
		logger.Warn("fail to get version info", zap.Error(err))
		return
	}
	version.ParseServerInfo(versionInfo)
}

func str2TimezoneOrFromDB(tctx *tcontext.Context, tzStr string, dbCfg *config.DBConfig) (*time.Location, error) {
	var err error
	if len(tzStr) == 0 {
		tzStr, err = conn.FetchTimeZoneSetting(tctx.Ctx, dbCfg)
		if err != nil {
			return nil, err
		}
	}
	loc, err := utils.ParseTimeZone(tzStr)
	if err != nil {
		return nil, err
	}
	tctx.L().Info("use timezone", zap.String("location", loc.String()),
		zap.String("host", dbCfg.Host), zap.Int("port", dbCfg.Port))
	return loc, nil
}

func subtaskCfg2BinlogSyncerCfg(cfg *config.SubTaskConfig, timezone *time.Location) (replication.BinlogSyncerConfig, error) {
	var tlsConfig *tls.Config
	var err error
	if cfg.From.Security != nil {
		if loadErr := cfg.From.Security.LoadTLSContent(); loadErr != nil {
			return replication.BinlogSyncerConfig{}, terror.ErrCtlLoadTLSCfg.Delegate(loadErr)
		}
		tlsConfig, err = util.NewTLSConfig(
			util.WithCAContent(cfg.From.Security.SSLCABytes),
			util.WithCertAndKeyContent(cfg.From.Security.SSLCertBytes, cfg.From.Security.SSLKEYBytes),
			util.WithVerifyCommonName(cfg.From.Security.CertAllowedCN),
		)
		if err != nil {
			return replication.BinlogSyncerConfig{}, terror.ErrConnInvalidTLSConfig.Delegate(err)
		}
	}

	syncCfg := replication.BinlogSyncerConfig{
		ServerID:                cfg.ServerID,
		Flavor:                  cfg.Flavor,
		Host:                    cfg.From.Host,
		Port:                    uint16(cfg.From.Port),
		User:                    cfg.From.User,
		Password:                cfg.From.Password,
		TimestampStringLocation: timezone,
		TLSConfig:               tlsConfig,
	}
	// when retry count > 1, go-mysql will retry sync from the previous GTID set in GTID mode,
	// which may get duplicate binlog event after retry success. so just set retry count = 1, and task
	// will exit when meet error, and then auto resume by DM itself.
	common.SetDefaultReplicationCfg(&syncCfg, 1)
	return syncCfg, nil
}

func safeToRedirect(e *replication.BinlogEvent) bool {
	if e != nil {
		switch e.Event.(type) {
		case *replication.GTIDEvent, *replication.MariadbGTIDEvent:
			return true
		}
	}
	return false
}
