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

package v1dbschema

import (
	"fmt"
	"strconv"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/reader"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.uber.org/zap"
)

// UpdateSchema updates the DB schema from v1.0.x to v2.0.x, including:
// - update checkpoint.
// - update online DDL meta.
func UpdateSchema(tctx *tcontext.Context, db *conn.BaseDB, cfg *config.SubTaskConfig) error {
	// get db connection.
	dbConn, err := db.GetBaseConn(tctx.Context())
	if err != nil {
		return terror.ErrFailUpdateV1DBSchema.Delegate(err)
	}
	defer func() {
		err2 := db.ForceCloseConn(dbConn)
		if err2 != nil {
			tctx.L().Warn("fail to close connection", zap.Error(err2))
		}
	}()

	// setup a TCP binlog reader (because no relay can be used when upgrading).
	syncCfg := replication.BinlogSyncerConfig{
		ServerID:       cfg.ServerID,
		Flavor:         cfg.Flavor,
		Host:           cfg.From.Host,
		Port:           uint16(cfg.From.Port),
		User:           cfg.From.User,
		Password:       cfg.From.Password, // plaintext.
		UseDecimal:     false,
		VerifyChecksum: true,
	}
	tcpReader := reader.NewTCPReader(syncCfg)

	// update checkpoint.
	err = updateSyncerCheckpoint(tctx, dbConn, cfg.Name, dbutil.TableName(cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name)), cfg.SourceID, cfg.EnableGTID, tcpReader)
	if err != nil {
		return terror.ErrFailUpdateV1DBSchema.Delegate(err)
	}

	// update online DDL meta.
	err = updateSyncerOnlineDDLMeta(tctx, dbConn, cfg.Name, dbutil.TableName(cfg.MetaSchema, cputil.SyncerOnlineDDL(cfg.Name)), cfg.SourceID, cfg.ServerID)
	return terror.ErrFailUpdateV1DBSchema.Delegate(err)
}

// updateSyncerCheckpoint updates the checkpoint table of sync unit, including:
// - update table schema:
//   - add column `binlog_gtid VARCHAR(256)`.
//   - add column `table_info JSON NOT NULL`.
//
// - update column value:
//   - fill `binlog_gtid` based on `binlog_name` and `binlog_pos` if GTID mode enable.
//
// NOTE: no need to update the value of `table_info` because DM can get schema automatically from downstream when replicating DML.
func updateSyncerCheckpoint(tctx *tcontext.Context, dbConn *conn.BaseConn, taskName, tableName, sourceID string, fillGTIDs bool, tcpReader reader.Reader) error {
	logger := log.L().WithFields(zap.String("task", taskName), zap.String("source", sourceID))
	logger.Info("updating syncer checkpoint", zap.Bool("fill GTID", fillGTIDs))
	var gs gmysql.GTIDSet
	if fillGTIDs {
		// NOTE: get GTID sets for all (global & tables) binlog position has many problems, at least including:
		//   - it is a heavy work because it should read binlog events once for each position
		//   - some binlog file for the position may have already been purge
		// so we only get GTID sets for the global position now,
		// and this should only have side effects for in-syncing shard tables, but we can mention and warn this case in the user docs.
		pos, err := getGlobalPos(tctx, dbConn, tableName, sourceID)
		logger.Info("got global checkpoint position", zap.Stringer("position", pos))
		if err != nil {
			return terror.Annotatef(err, "get global checkpoint position for source %s", sourceID)
		}
		if pos.Name != "" {
			gs, err = getGTIDsForPos(tctx, pos, tcpReader)
			if err != nil {
				return terror.Annotatef(err, "get GTID sets for position %s", pos)
			}
			gs, err = conn.AddGSetWithPurged(tctx.Context(), gs, dbConn)
			if err != nil {
				return terror.Annotatef(err, "get GTID sets for position %s", pos)
			}
			logger.Info("got global checkpoint GTID sets", log.WrapStringerField("GTID sets", gs))
		}
	}

	// try to add columns.
	// NOTE: ignore already exists error to continue the process.
	queries := []string{
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN binlog_gtid TEXT AFTER binlog_pos`, tableName),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN table_info JSON NOT NULL AFTER binlog_gtid`, tableName),
	}
	updateTaskName := "importFromV10x"
	_, err := dbConn.ExecuteSQLWithIgnoreError(tctx, nil, updateTaskName, utils.IgnoreErrorCheckpoint, queries)
	if err != nil {
		return terror.Annotatef(err, "add columns for checkpoint table")
	}

	if fillGTIDs && gs != nil {
		// set binlog_gtid, `gs` should valid here.
		err = setGlobalGTIDs(tctx, dbConn, taskName, tableName, sourceID, gs.String())
		if err != nil {
			return terror.Annotatef(err, "set GTID sets %s for checkpoint table", gs.String())
		}
		logger.Info("filled global checkpoint GTID sets", zap.Stringer("GTID sets", gs))
	}
	return nil
}

// updateSyncerOnlineDDLMeta updates the online DDL meta data, including:
// - update the value of `id` from `server-id` to `source-id`.
// NOTE: online DDL may not exist if not enabled.
func updateSyncerOnlineDDLMeta(tctx *tcontext.Context, dbConn *conn.BaseConn, taskName, tableName, sourceID string, serverID uint32) error {
	logger := log.L().WithFields(zap.String("task", taskName), zap.String("source", sourceID))
	logger.Info("updating syncer online DDL meta")
	queries := []string{
		fmt.Sprintf(`UPDATE %s SET id=? WHERE id=?`, tableName), // for multiple columns.
	}
	args := []interface{}{sourceID, strconv.FormatUint(uint64(serverID), 10)}
	_, err := dbConn.ExecuteSQLWithIgnoreError(tctx, nil, taskName, ignoreErrorOnlineDDL, queries, args)
	return terror.Annotatef(err, "update id column for online DDL meta table")
}

// getGlobalPos tries to get the global checkpoint position.
func getGlobalPos(tctx *tcontext.Context, dbConn *conn.BaseConn, tableName, sourceID string) (gmysql.Position, error) {
	query := fmt.Sprintf(`SELECT binlog_name, binlog_pos FROM %s WHERE id=? AND is_global=? LIMIT 1`, tableName)
	args := []interface{}{sourceID, true}
	rows, err := dbConn.QuerySQL(tctx, query, args...)
	if err != nil {
		return gmysql.Position{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		return gmysql.Position{}, nil // no global checkpoint position exists.
	}

	var (
		name string
		pos  uint32
	)
	err = rows.Scan(&name, &pos)
	if err != nil {
		return gmysql.Position{}, err
	}

	return gmysql.Position{
		Name: name,
		Pos:  pos,
	}, rows.Err()
}

// getGTIDsForPos gets the GTID sets for the position.
func getGTIDsForPos(tctx *tcontext.Context, pos gmysql.Position, tcpReader reader.Reader) (gs gmysql.GTIDSet, err error) {
	// NOTE: because we have multiple unit test cases updating/clearing binlog in the upstream,
	// we may encounter errors when reading binlog event but cleared by another test case.
	failpoint.Inject("MockGetGTIDsForPos", func(val failpoint.Value) {
		str := val.(string)
		gs, _ = gtid.ParserGTID(gmysql.MySQLFlavor, str)
		tctx.L().Info("set gs for position", zap.String("failpoint", "MockGetGTIDsForPos"), zap.Stringer("pos", pos))
		failpoint.Return(gs, nil)
	})

	var realPos gmysql.Position
	realPos, err = binlog.RealMySQLPos(pos)
	if err != nil {
		return nil, err
	}
	gs, err = reader.GetGTIDsForPos(tctx.Ctx, tcpReader, realPos)
	if err != nil {
		return nil, err
	}
	return gs, nil
}

// setGlobalGTIDs tries to set `binlog_gtid` for the global checkpoint.
func setGlobalGTIDs(tctx *tcontext.Context, dbConn *conn.BaseConn, taskName, tableName, sourceID, gs string) error {
	queries := []string{
		fmt.Sprintf(`UPDATE %s SET binlog_gtid=? WHERE id=? AND is_global=? LIMIT 1`, tableName),
	}
	args := []interface{}{gs, sourceID, true}
	_, err := dbConn.ExecuteSQL(tctx, nil, taskName, queries, args)
	return err
}

func ignoreErrorOnlineDDL(err error) bool {
	err = errors.Cause(err) // check the original error
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}

	switch mysqlErr.Number {
	case errno.ErrNoSuchTable:
		return true
	default:
		return false
	}
}
