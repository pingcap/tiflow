// Copyright 2022 PingCAP, Inc.
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
	"encoding/json"
	"fmt"

	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
)

type validatorCheckpointHelper struct {
	tctx      *tcontext.Context
	cfg       *config.SubTaskConfig
	db        *conn.BaseDB
	dbConn    *dbconn.DBConn
	validator *DataValidator

	checkpointTableName    string
	pendingChangeTableName string
	errorChangeTableName   string
	tableStatusTableName   string
	revision               int64
}

func newValidatorCheckpointHelper(validator *DataValidator) *validatorCheckpointHelper {
	cfg := validator.cfg
    c := &validatorCheckpointHelper{
		tctx: validator.tctx,
		cfg: cfg,
		validator: validator,

		checkpointTableName:    dbutil.TableName(cfg.MetaSchema, cputil.ValidatorCheckpoint(cfg.Name)),
		pendingChangeTableName: dbutil.TableName(cfg.MetaSchema,cputil.ValidatorPendingChange(cfg.Name)),
		errorChangeTableName:dbutil.TableName(cfg.MetaSchema,cputil.ValidatorErrorChange(cfg.Name)),
		tableStatusTableName:dbutil.TableName(cfg.MetaSchema,cputil.ValidatorTableStatus(cfg.Name)),
	}

	return c
}

func (c *validatorCheckpointHelper) init() error {
	tctx, cancelFunc := c.tctx.WithTimeout(unit.DefaultInitTimeout)
	defer cancelFunc()

	dbCfg := c.cfg.To
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxDMLConnectionTimeout)
	db, conns, err := dbconn.CreateConns(tctx, c.cfg, &dbCfg, 1)
	if err != nil {
		return err
	}

	c.db = db
	c.dbConn = conns[0]
	defer func() {
		if err == nil {
			return
		}
		dbconn.CloseBaseDB(tctx, c.db)
	}()

	if err = c.createSchema(tctx); err != nil {
		return err
	}

	err = c.createTable(tctx)
	return err
}

func (c *validatorCheckpointHelper) createSchema(tctx *tcontext.Context) error {
	sql2 := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", dbutil.ColumnName(c.cfg.MetaSchema))
	args := make([]interface{}, 0)
	_, err := c.dbConn.ExecuteSQL(tctx, []string{sql2}, [][]interface{}{args}...)
	tctx.L().Info("create checkpoint schema", zap.String("statement", sql2))
	return err
}

func (c *validatorCheckpointHelper) createTable(tctx *tcontext.Context) error {
	sqls := []string{
		`CREATE TABLE IF NOT EXISTS ` + c.checkpointTableName + ` (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			source VARCHAR(32) NOT NULL,
			binlog_name VARCHAR(128),
			binlog_pos INT UNSIGNED,
			binlog_gtid TEXT,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_source (source)
		)`,
		`CREATE TABLE IF NOT EXISTS ` + c.pendingChangeTableName + ` (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			source VARCHAR(32) NOT NULL,
			schema_name VARCHAR(128) NOT NULL,
			table_name VARCHAR(128) NOT NULL,
			key VARCHAR(512) NOT NULL,
			data JSON NOT NULL,
			revision bigint NOT NULL,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_source_schema_table_key(source, schema_name, table_name, key),
			INDEX idx_revision(revision)
		)`,
		`CREATE TABLE IF NOT EXISTS ` + c.errorChangeTableName + ` (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			source VARCHAR(32) NOT NULL,
			schema_name VARCHAR(128) NOT NULL,
			table_name VARCHAR(128) NOT NULL,
			key VARCHAR(512) NOT NULL,
			data JSON NOT NULL,
			dst_data JSON NOT NULL,
			error_type int NOT NULL,
			status int not null,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_source_schema_table_key(source, schema_name, table_name, key)
		)`,
		`CREATE TABLE IF NOT EXISTS ` + c.tableStatusTableName + ` (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			source VARCHAR(32) NOT NULL,
			src_schema_name VARCHAR(128) NOT NULL,
			src_table_name VARCHAR(128) NOT NULL,
			dst_schema_name VARCHAR(128) NOT NULL,
			dst_table_name VARCHAR(128) NOT NULL,
			stage VARCHAR(32) NOT NULL,
            message VARCHAR(512) NOT NULL,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_source_schema_table_key(source, src_schema_name, src_table_name)
		)`,
	}
	_, err := c.dbConn.ExecuteSQL(tctx, sqls)
	tctx.L().Info("create checkpoint table", zap.Strings("statements", sqls))
	return err
}

func (c *validatorCheckpointHelper) flush(loc binlog.Location) error {
	var queries []string
	var args [][]interface{}
	nextRevision := c.revision + 1

	// update checkpoint
	queries = append(queries, `INSERT INTO `+c.checkpointTableName+
		`(source, binlog_name, binlog_pos, binlog_gtid) VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			source = VALUES(source),
			binlog_name = VALUES(binlog_name),
			binlog_pos = VALUES(binlog_pos),
			binlog_gtid = VALUES(binlog_gtid)
		`)
	args = append(args, []interface{}{c.cfg.SourceID, loc.Position.Name, loc.Position.Pos, loc.GTIDSetStr()})

	// update/insert pending row changes
	for _, worker := range c.validator.workers {
		for _, tblChange := range worker.getPendingChangesMap() {
			for key, row := range tblChange.rows {
				rowJson, err := json.Marshal(&row)
				if err != nil {
					return err
				}
				sql := `INSERT INTO ` + c.pendingChangeTableName + `
						(source, schema_name, table_name, key, data, revision) VALUES (?, ?, ?, ?, ?, ?)
						ON DUPLICATE KEY UPDATE
							source = VALUES(source),
							schema_name = VALUES(schema_name),
							table_name = VALUES(table_name),
							key = VALUES(key),
							data = VALUES(data),
							revision = VALUES(revision)`
				queries = append(queries, sql)
				args = append(args, []interface{}{
					c.cfg.SourceID,
					row.table.Source.Schema,
					row.table.Source.Name,
					key,
					rowJson,
					nextRevision,
				})
			}
		}
	}

	// delete success row changes, i.e. rows with previous revision
	queries = append(queries, `DELETE FROM `+c.pendingChangeTableName+` WHERE revision = ?`)
	args = append(args, []interface{}{c.revision})

	// unsupported table info
	for _, state := range c.validator.tableStatus {
		sql := `INSERT INTO ` + c.tableStatusTableName + `
						(source, src_schema_name, src_table_name, dst_schema_name, dst_table_name, stage, message)
						VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE
						source = VALUES(source),
						src_schema_name = VALUES(src_schema_name),
						src_table_name = VALUES(src_table_name),
						dst_schema_name = VALUES(dst_schema_name),
						dst_table_name = VALUES(dst_table_name),
						stage = VALUES(stage),
						message = VALUES(message)
				`
		queries = append(queries, sql)
		args = append(args, []interface{}{
			c.cfg.SourceID, state.source.Schema, state.source.Name, state.target.Schema, state.target.Name,
			int(state.stage), state.message},
		)
	}
	_, err := c.dbConn.ExecuteSQL(c.tctx, queries, args...)
	if err != nil {
		return err
	}
	c.revision++
	return nil
}
