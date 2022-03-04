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

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
)

type validateErrorState int

const (
	// todo: maybe move to some common place, dmctl may use it
	newValidateErrorRow validateErrorState = iota
	ignoredValidateErrorRow
	resolvedValidateErrorRow
)

type validatorPersistHelper struct {
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

func newValidatorCheckpointHelper(validator *DataValidator) *validatorPersistHelper {
	cfg := validator.cfg
	c := &validatorPersistHelper{
		tctx:      validator.tctx,
		cfg:       cfg,
		validator: validator,

		checkpointTableName:    dbutil.TableName(cfg.MetaSchema, cputil.ValidatorCheckpoint(cfg.Name)),
		pendingChangeTableName: dbutil.TableName(cfg.MetaSchema, cputil.ValidatorPendingChange(cfg.Name)),
		errorChangeTableName:   dbutil.TableName(cfg.MetaSchema, cputil.ValidatorErrorChange(cfg.Name)),
		tableStatusTableName:   dbutil.TableName(cfg.MetaSchema, cputil.ValidatorTableStatus(cfg.Name)),
	}

	return c
}

func (c *validatorPersistHelper) init() error {
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

func (c *validatorPersistHelper) createSchema(tctx *tcontext.Context) error {
	sql2 := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", dbutil.ColumnName(c.cfg.MetaSchema))
	args := make([]interface{}, 0)
	_, err := c.dbConn.ExecuteSQL(tctx, []string{sql2}, [][]interface{}{args}...)
	tctx.L().Info("create checkpoint schema", zap.String("statement", sql2))
	return err
}

func (c *validatorPersistHelper) createTable(tctx *tcontext.Context) error {
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
			src_schema_name VARCHAR(128) NOT NULL,
			src_table_name VARCHAR(128) NOT NULL,
			key VARCHAR(512) NOT NULL,
			dst_schema_name VARCHAR(128) NOT NULL,
			dst_table_name VARCHAR(128) NOT NULL,
			data JSON NOT NULL,
			dst_data JSON NOT NULL,
			error_type int NOT NULL,
			status int not null,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_source_schema_table_key(source, src_schema_name, src_table_name, key)
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

func (c *validatorPersistHelper) persist(loc binlog.Location) error {
	var queries []string
	var args [][]interface{}
	nextRevision := c.revision + 1

	// update checkpoint
	queries = append(queries, `INSERT INTO `+c.checkpointTableName+
		`(source, binlog_name, binlog_pos, binlog_gtid) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE
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

	// delete success row changes, i.e. rows with different revision
	queries = append(queries, `DELETE FROM `+c.pendingChangeTableName+` WHERE revision != ?`)
	args = append(args, []interface{}{nextRevision})

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
	// error rows
	for _, worker := range c.validator.workers {
		for _, r := range worker.getErrorRows() {
			sql := `INSERT INTO ` + c.errorChangeTableName + `
					(source, src_schema_name, src_table_name, key, dst_schema_name, dst_table_name, data, dst_data, error_type, status)
					VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE
					source = VALUES(source),
					src_schema_name = VALUES(src_schema_name),
					src_table_name = VALUES(src_table_name),
					key = VALUES(key),
					dst_schema_name = VALUES(dst_schema_name),
					dst_table_name = VALUES(dst_table_name),
					data = VALUES(data),
					dst_data = VALUES(dst_data),
					error_type = VALUES(error_type),
					status = VALUES(status)
			`
			queries = append(queries, sql)

			srcDataStr, err := json.Marshal(r.srcRow.Data)
			if err != nil {
				return err
			}
			dstData := make([]interface{}, len(r.dstData))
			for i, d := range r.dstData {
				if d.Valid {
					dstData[i] = d.String
				}
			}
			dstDataStr, err := json.Marshal(dstData)
			if err != nil {
				return err
			}
			table := r.srcRow.table
			args = append(args, []interface{}{
				c.cfg.SourceID, table.Source.Schema, table.Source.Name, r.srcRow.Key,
				table.Target.Schema, table.Target.Name,
				srcDataStr, dstDataStr, r.tp, newValidateErrorRow,
			})
		}
	}
	_, err := c.dbConn.ExecuteSQL(c.tctx, queries, args...)
	if err != nil {
		return err
	}
	c.revision++

	// reset errors after save
	for _, worker := range c.validator.workers {
		worker.resetErrorRows()
	}

	return nil
}

func (c *validatorPersistHelper) close() {
	c.db.Close()
}

func (c *validatorPersistHelper) loadCheckpoint(tctx *tcontext.Context) (*binlog.Location, error) {
	sql := "select binlog_name, binlog_pos, binlog_gtid from " + c.checkpointTableName + " where source = ?"
	rows, err := c.dbConn.QuerySQL(tctx, sql, c.cfg.SourceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var location *binlog.Location

	// at most one row
	if rows.Next() {
		var (
			binlogName, binlogGtidStr string
			binlogPos                 uint32
		)

		err = rows.Scan(&binlogName, &binlogPos, &binlogGtidStr)
		if err != nil {
			return nil, err
		}
		gset, err := gtid.ParserGTID(c.cfg.Flavor, binlogGtidStr) // default to "".
		if err != nil {
			return nil, err
		}
		tmpLoc := binlog.InitLocation(mysql.Position{Name: binlogName, Pos: binlogPos}, gset)
		location = &tmpLoc
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return location, nil
}

func (c *validatorPersistHelper) loadPendingChange(tctx *tcontext.Context) (map[string]*tableChange, int64, error) {
	res := make(map[string]*tableChange)
	rev := int64(1)
	sql := "select schema_name, table_name, key, data, revision from " + c.pendingChangeTableName + " where source = ?"
	rows, err := c.dbConn.QuerySQL(tctx, sql, c.cfg.SourceID)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			schemaName, tableName, key string
			data                       []byte
			revision                   int64
		)
		err = rows.Scan(&schemaName, &tableName, &key, &data, &revision)
		if err != nil {
			return nil, 0, err
		}
		sourceTbl := filter.Table{Schema: schemaName, Name: tableName}
		fullTableName := sourceTbl.String()
		tblChange, ok := res[fullTableName]
		if !ok {
			tblChange = newTableChange(&validateTableInfo{Source: &sourceTbl})
			res[fullTableName] = tblChange
		}
		var row *rowChange
		err = json.Unmarshal(data, &row)
		if err != nil {
			return nil, 0, err
		}
		tblChange.rows[key] = row
		rev = revision
	}

	if err = rows.Err(); err != nil {
		return nil, 0, err
	}
	return res, rev, nil
}

func (c *validatorPersistHelper) loadTableStatus(tctx *tcontext.Context) (map[string]*tableValidateStatus, error) {
	res := make(map[string]*tableValidateStatus)
	sql := "select src_schema_name, src_table_name, dst_schema_name, dst_table_name, stage, message from " +
		c.tableStatusTableName + " where source = ?"
	rows, err := c.dbConn.QuerySQL(tctx, sql, c.cfg.SourceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			srcSchemaName, srcTableName, dstSchemaName, dstTableName string
			stage                                                    int
			message                                                  string
		)
		err = rows.Scan(&srcSchemaName, &srcTableName, &dstSchemaName, &dstTableName, &stage, &message)
		if err != nil {
			return nil, err
		}
		srcTbl := filter.Table{Schema: srcSchemaName, Name: srcTableName}
		fullTableName := srcTbl.String()
		res[fullTableName] = &tableValidateStatus{
			source:  srcTbl,
			target:  filter.Table{Schema: dstSchemaName, Name: dstTableName},
			stage:   pb.Stage(stage),
			message: message,
		}
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

func (c *validatorPersistHelper) setRevision(rev int64) {
	c.revision = rev
}
