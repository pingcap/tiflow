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
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/dumpling/export"
	dlog "github.com/pingcap/tidb/dumpling/log"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog/common"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"go.uber.org/zap"
)

const (
	// the time layout for TiDB SHOW DDL statements.
	timeLayout = "2006-01-02 15:04:05"
	// everytime retrieve 10 new rows from TiDB history jobs.
	linesOfRows = 10
	// max capacity of the block/allow list.
	maxCapacity = 100000
)

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

func str2TimezoneOrFromDB(tctx *tcontext.Context, tzStr string, dbCfg conn.ScopedDBConfig) (*time.Location, string, error) {
	var err error
	if len(tzStr) == 0 {
		baseDB, err2 := conn.DefaultDBProvider.Apply(dbCfg)
		if err2 != nil {
			return nil, "", err2
		}
		defer baseDB.Close()
		tzStr, err = config.FetchTimeZoneSetting(tctx.Ctx, baseDB.DB)
		if err != nil {
			return nil, "", err
		}
	}
	loc, err := utils.ParseTimeZone(tzStr)
	if err != nil {
		return nil, "", err
	}
	tctx.L().Info("use timezone", zap.String("location", loc.String()),
		zap.String("host", dbCfg.Host), zap.Int("port", dbCfg.Port))
	return loc, tzStr, nil
}

func subtaskCfg2BinlogSyncerCfg(cfg *config.SubTaskConfig, timezone *time.Location, baList *filter.Filter) (replication.BinlogSyncerConfig, error) {
	var tlsConfig *tls.Config
	var err error
	if cfg.From.Security != nil {
		if loadErr := cfg.From.Security.LoadTLSContent(); loadErr != nil {
			return replication.BinlogSyncerConfig{}, terror.ErrCtlLoadTLSCfg.Delegate(loadErr)
		}
		tlsConfig, err = util.NewTLSConfig(
			util.WithCAContent(cfg.From.Security.SSLCABytes),
			util.WithCertAndKeyContent(cfg.From.Security.SSLCertBytes, cfg.From.Security.SSLKeyBytes),
			util.WithVerifyCommonName(cfg.From.Security.CertAllowedCN),
		)
		if err != nil {
			return replication.BinlogSyncerConfig{}, terror.ErrConnInvalidTLSConfig.Delegate(err)
		}
	}

	var rowsEventDecodeFunc func(*replication.RowsEvent, []byte) error
	if baList != nil {
		// we don't track delete table events, so simply reset the cache if it's full
		// TODO: use LRU or CLOCK cache if needed.
		// NOTE: use Table as Key rather than TableID
		// because TableID may change when upstream switches master, and also RenameTable will not change TableID.
		allowListCache := make(map[filter.Table]struct{}, maxCapacity)
		blockListCache := make(map[filter.Table]struct{}, maxCapacity)

		rowsEventDecodeFunc = func(re *replication.RowsEvent, data []byte) error {
			pos, err := re.DecodeHeader(data)
			if err != nil {
				return err
			}
			tb := filter.Table{
				Schema: string(re.Table.Schema),
				Name:   string(re.Table.Table),
			}
			if _, ok := blockListCache[tb]; ok {
				return nil
			} else if _, ok := allowListCache[tb]; ok {
				return re.DecodeData(pos, data)
			}

			if skipByTable(baList, &tb) {
				if len(blockListCache) >= maxCapacity {
					blockListCache = make(map[filter.Table]struct{}, maxCapacity)
				}
				blockListCache[tb] = struct{}{}
				return nil
			}

			if len(allowListCache) >= maxCapacity {
				allowListCache = make(map[filter.Table]struct{}, maxCapacity)
			}
			allowListCache[tb] = struct{}{}
			return re.DecodeData(pos, data)
		}
	}

	h := cfg.WorkerName
	// https://github.com/mysql/mysql-server/blob/1bfe02bdad6604d54913c62614bde57a055c8332/include/my_hostname.h#L33-L42
	if len(h) > 60 {
		h = h[:60]
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
		RowsEventDecodeFunc:     rowsEventDecodeFunc,
		Localhost:               h,
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

// getDDLStatusFromTiDB retrieves the synchronizing status of DDL from TiDB
// hence here db should be TiDB database
// createTime should be based on the timezone of downstream, and its unit is second.
func getDDLStatusFromTiDB(tctx *tcontext.Context, db *dbconn.DBConn, ddl string, createTime int64) (string, error) {
	rowNum := linesOfRows
	rowOffset := 0
	queryMap := make(map[int]string)

	for {
		// every attempt try 10 history jobs
		showJobs := fmt.Sprintf("ADMIN SHOW DDL JOBS %d", rowNum)
		//nolint:rowserrcheck
		jobsRows, err := db.QuerySQL(tctx, nil, showJobs)
		if err != nil {
			return "", err
		}

		var jobsResults [][]string
		jobsResults, err = export.GetSpecifiedColumnValuesAndClose(jobsRows, "JOB_ID", "CREATE_TIME", "STATE")
		if err != nil {
			return "", err
		}

		for i := rowNum - linesOfRows; i < rowNum && i < len(jobsResults); i++ {
			ddlCreateTimeStr := jobsResults[i][1]
			var ddlCreateTimeParse time.Time
			ddlCreateTimeParse, err = time.Parse(timeLayout, ddlCreateTimeStr)
			if err != nil {
				return "", err
			}
			ddlCreateTime := ddlCreateTimeParse.Unix()

			// ddlCreateTime and createTime are both based on timezone of downstream
			if ddlCreateTime >= createTime {
				var jobID int
				jobID, err = strconv.Atoi(jobsResults[i][0])
				if err != nil {
					return "", err
				}

				for {
					ddlQuery, ok := queryMap[jobID]
					if !ok {
						// jobID does not exist, expand queryMap for deeper search
						showJobsLimitNext := fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET %d", rowOffset)
						var rowsLimitNext *sql.Rows
						//nolint:rowserrcheck
						rowsLimitNext, err = db.QuerySQL(tctx, nil, showJobsLimitNext)
						if err != nil {
							return "", err
						}

						var resultsLimitNext [][]string
						resultsLimitNext, err = export.GetSpecifiedColumnValuesAndClose(rowsLimitNext, "JOB_ID", "QUERY")
						if err != nil {
							return "", err
						}
						if len(resultsLimitNext) == 0 {
							// JOB QUERIES has been used up
							// requested DDL cannot be found
							return "", nil
						}

						// if new DDLs are written to TiDB after the last query 'ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET'
						// we may get duplicate rows here, but it does not affect the checking
						for k := range resultsLimitNext {
							var jobIDForLimit int
							jobIDForLimit, err = strconv.Atoi(resultsLimitNext[k][0])
							if err != nil {
								return "", err
							}
							queryMap[jobIDForLimit] = resultsLimitNext[k][1]
						}
						rowOffset += linesOfRows
					} else {
						if ddl == ddlQuery {
							return jobsResults[i][2], nil
						}
						break
					}
				}
			} else {
				// ddlCreateTime is monotonous in jobsResults
				// requested DDL cannot be found
				return "", nil
			}
		}
		if len(jobsResults) == rowNum {
			rowNum += linesOfRows
		} else {
			// jobsResults has been checked thoroughly
			return "", nil
		}
	}
}
