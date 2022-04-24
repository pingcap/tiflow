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
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tidb/util/filter"
	"go.uber.org/atomic"
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

const (
	maxRowKeyLength = 64
)

var mapErrType2Str = map[validateFailedType]string{
	deletedRowExists: "Deleted rows exist",
	rowNotExist:      "Expected rows not exist",
	rowDifferent:     "Column data not matched",
}

var maxRowKeyLengthStr = strconv.Itoa(maxRowKeyLength)

type validatorPersistHelper struct {
	tctx              *tcontext.Context
	cfg               *config.SubTaskConfig
	db                *conn.BaseDB
	dbConn            *dbconn.DBConn
	validator         *DataValidator
	schemaInitialized atomic.Bool

	checkpointTableName    string
	pendingChangeTableName string
	errorChangeTableName   string
	tableStatusTableName   string
	revision               int64
}

func newValidatorCheckpointHelper(validator *DataValidator) *validatorPersistHelper {
	cfg := validator.cfg
	c := &validatorPersistHelper{
		cfg:       cfg,
		validator: validator,

		checkpointTableName:    dbutil.TableName(cfg.MetaSchema, cputil.ValidatorCheckpoint(cfg.Name)),
		pendingChangeTableName: dbutil.TableName(cfg.MetaSchema, cputil.ValidatorPendingChange(cfg.Name)),
		errorChangeTableName:   dbutil.TableName(cfg.MetaSchema, cputil.ValidatorErrorChange(cfg.Name)),
		tableStatusTableName:   dbutil.TableName(cfg.MetaSchema, cputil.ValidatorTableStatus(cfg.Name)),
	}

	return c
}

func (c *validatorPersistHelper) init(tctx *tcontext.Context) error {
	c.tctx = tctx

	newCtx, cancelFunc := c.tctx.WithTimeout(unit.DefaultInitTimeout)
	defer cancelFunc()

	dbCfg := c.cfg.To
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxDMLConnectionTimeout).SetMaxIdleConns(1)
	db, conns, err := dbconn.CreateConns(newCtx, c.cfg, &dbCfg, 1)
	if err != nil {
		return err
	}

	c.db = db
	c.dbConn = conns[0]
	defer func() {
		if err == nil {
			return
		}
		dbconn.CloseBaseDB(newCtx, c.db)
	}()

	if !c.schemaInitialized.Load() {
		if err = c.createSchema(newCtx); err != nil {
			return err
		}

		err = c.createTable(newCtx)

		c.schemaInitialized.Store(true)
	}
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
			row_pk VARCHAR(` + maxRowKeyLengthStr + `) NOT NULL,
			data JSON NOT NULL,
			revision bigint NOT NULL,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_source_schema_table_key(source, schema_name, table_name, row_pk),
			INDEX idx_revision(revision)
		)`,
		`CREATE TABLE IF NOT EXISTS ` + c.errorChangeTableName + ` (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			source VARCHAR(32) NOT NULL,
			src_schema_name VARCHAR(128) NOT NULL,
			src_table_name VARCHAR(128) NOT NULL,
			row_pk VARCHAR(` + maxRowKeyLengthStr + `) NOT NULL,
			dst_schema_name VARCHAR(128) NOT NULL,
			dst_table_name VARCHAR(128) NOT NULL,
			data JSON NOT NULL,
			dst_data JSON NOT NULL,
			error_type int NOT NULL,
			status int NOT NULL,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_source_schema_table_key(source, src_schema_name, src_table_name, row_pk),
            INDEX idx_status(status)
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
	tctx.L().Info("create checkpoint and data table", zap.Strings("statements", sqls))
	return err
}

type tableChangeDataForPersist struct {
	sourceTable *filter.Table
	columnCount int
	rows        map[string]*rowChangeDataForPersist
}

type rowChangeDataForPersist struct {
	Key             string           `json:"key"`
	Tp              rowChangeJobType `json:"tp"`
	Data            []interface{}    `json:"data"`
	FirstValidateTS int64            `json:"first-ts"`
	FailedCnt       int              `json:"failed-cnt"` // failed count
}

func (c *validatorPersistHelper) persist(loc binlog.Location) error {
	// get snapshot of the current table status
	tableStatus := c.validator.getTableStatusMap()
	count := len(tableStatus)
	for _, worker := range c.validator.getWorkers() {
		for _, tblChange := range worker.getPendingChangesMap() {
			count += len(tblChange.jobs)
		}
		count += len(worker.errorRows)
	}
	queries := make([]string, 0, count+2)
	args := make([][]interface{}, 0, count+2)
	nextRevision := c.revision + 1

	c.tctx.L().Info("persist checkpoint and intermediate data")

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
	for _, worker := range c.validator.getWorkers() {
		for _, tblChange := range worker.getPendingChangesMap() {
			for key, j := range tblChange.jobs {
				row := j.row
				rowForPersist := rowChangeDataForPersist{
					Key:             key,
					Tp:              j.Tp,
					Data:            row.RowValues(),
					FirstValidateTS: j.FirstValidateTS,
					FailedCnt:       j.FailedCnt,
				}
				rowJSON, err := json.Marshal(&rowForPersist)
				if err != nil {
					return err
				}
				sql := `INSERT INTO ` + c.pendingChangeTableName + `
						(source, schema_name, table_name, row_pk, data, revision) VALUES (?, ?, ?, ?, ?, ?)
						ON DUPLICATE KEY UPDATE
							source = VALUES(source),
							schema_name = VALUES(schema_name),
							table_name = VALUES(table_name),
							row_pk = VALUES(row_pk),
							data = VALUES(data),
							revision = VALUES(revision)`
				queries = append(queries, sql)
				sourceTable := row.GetSourceTable()
				args = append(args, []interface{}{
					c.cfg.SourceID,
					sourceTable.Schema,
					sourceTable.Table,
					key,
					rowJSON,
					nextRevision,
				})
			}
		}
	}

	// delete success row changes, i.e. rows with different revision
	queries = append(queries, `DELETE FROM `+c.pendingChangeTableName+` WHERE revision != ?`)
	args = append(args, []interface{}{nextRevision})

	// unsupported table info
	for _, state := range tableStatus {
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
			int(state.stage), state.message,
		},
		)
	}
	// error rows
	for _, worker := range c.validator.getWorkers() {
		for _, r := range worker.getErrorRows() {
			sql := `INSERT INTO ` + c.errorChangeTableName + `
					(source, src_schema_name, src_table_name, row_pk, dst_schema_name, dst_table_name, data, dst_data, error_type, status)
					VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE
					source = VALUES(source),
					src_schema_name = VALUES(src_schema_name),
					src_table_name = VALUES(src_table_name),
					row_pk = VALUES(row_pk),
					dst_schema_name = VALUES(dst_schema_name),
					dst_table_name = VALUES(dst_table_name),
					data = VALUES(data),
					dst_data = VALUES(dst_data),
					error_type = VALUES(error_type),
					status = VALUES(status)
			`
			queries = append(queries, sql)

			row := r.srcJob.row
			srcDataStr, err := json.Marshal(row.RowValues())
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
			sourceTable := row.GetSourceTable()
			targetTable := row.GetTargetTable()
			args = append(args, []interface{}{
				c.cfg.SourceID, sourceTable.Schema, sourceTable.Table, r.srcJob.Key,
				targetTable.Schema, targetTable.Table,
				srcDataStr, dstDataStr, r.tp, pb.ValidateErrorState_NewErr,
			})
		}
	}
	// todo: performance issue when using insert on duplicate? https://asktug.com/t/topic/33147
	// todo: will this transaction too big? but checkpoint & pending changes should be saved in one tx
	var err error
	failpoint.Inject("ValidatorCheckPointSkipExecuteSQL", func(val failpoint.Value) {
		str := val.(string)
		if str != "" {
			err = errors.New(str)
		}
		failpoint.Goto("afterExecuteSQL")
	})
	_, err = c.dbConn.ExecuteSQL(c.tctx, queries, args...)
	failpoint.Label("afterExecuteSQL")
	if err != nil {
		return err
	}

	return nil
}

func (c *validatorPersistHelper) incrRevision() {
	c.revision++
}

func (c *validatorPersistHelper) close() {
	if c.db != nil {
		c.db.Close()
	}
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
		gset, err2 := gtid.ParserGTID(c.cfg.Flavor, binlogGtidStr)
		if err2 != nil {
			return nil, err2
		}
		tmpLoc := binlog.InitLocation(mysql.Position{Name: binlogName, Pos: binlogPos}, gset)
		location = &tmpLoc
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	c.tctx.L().Info("checkpoint loaded", zap.Reflect("loc", location))
	return location, nil
}

func (c *validatorPersistHelper) loadPendingChange(tctx *tcontext.Context) (map[string]*tableChangeDataForPersist, int64, error) {
	res := make(map[string]*tableChangeDataForPersist)
	rev := int64(1)
	sql := "select schema_name, table_name, row_pk, data, revision from " + c.pendingChangeTableName + " where source = ?"
	rows, err := c.dbConn.QuerySQL(tctx, sql, c.cfg.SourceID)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var count int
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
		var row *rowChangeDataForPersist
		err = json.Unmarshal(data, &row)
		if err != nil {
			return nil, 0, err
		}

		sourceTbl := filter.Table{Schema: schemaName, Name: tableName}
		fullTableName := sourceTbl.String()
		tblChange, ok := res[fullTableName]
		if !ok {
			tblChange = &tableChangeDataForPersist{
				sourceTable: &sourceTbl,
				columnCount: len(row.Data),
				rows:        make(map[string]*rowChangeDataForPersist),
			}
			res[fullTableName] = tblChange
		}
		tblChange.rows[key] = row
		rev = revision
		count++
	}

	if err = rows.Err(); err != nil {
		return nil, 0, err
	}
	c.tctx.L().Info("pending change loaded", zap.Reflect("count", count), zap.Reflect("rev", rev))
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
	c.tctx.L().Info("table status loaded", zap.Reflect("count", len(res)))
	return res, nil
}

func (c *validatorPersistHelper) loadErrorCount(tctx *tcontext.Context) (map[pb.ValidateErrorState]int64, error) {
	res := make(map[pb.ValidateErrorState]int64)
	sql := "select status, count(*) from " + c.errorChangeTableName + " where source = ? group by status"
	rows, err := c.dbConn.QuerySQL(tctx, sql, c.cfg.SourceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var status int
		var count int64
		err = rows.Scan(&status, &count)
		if err != nil {
			return nil, err
		}
		res[pb.ValidateErrorState(status)] = count
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	c.tctx.L().Info("error count loaded", zap.Reflect("counts", res))
	return res, nil
}

func (c *validatorPersistHelper) setRevision(rev int64) {
	c.revision = rev
}

func (c *validatorPersistHelper) loadError(filterState pb.ValidateErrorState) ([]*pb.ValidationError, error) {
	var (
		rows *sql.Rows
		err  error
	)
	res := make([]*pb.ValidationError, 0)
	args := []interface{}{
		c.cfg.SourceID,
	}
	sql := "SELECT (id, source, src_schema_name, src_table_name, dst_schema_name, dst_table_name, data, dst_data, error_type, status, update_time) " +
		"FROM " + c.errorChangeTableName + " WHERE source=?"
	if filterState != pb.ValidateErrorState_InvalidErr {
		sql += " AND status=?"
		args = append(args, int(filterState))
	}
	rows, err = c.dbConn.QuerySQL(c.tctx, sql, args...)
	if err != nil {
		return res, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id, status, errType                                                                 int
			source, srcSchemaName, srcTableName, dstSchemaName, dstTableName, data, dstData, ts string
		)
		err = rows.Scan(&id, &source, &srcSchemaName, &srcTableName, &dstSchemaName, &dstTableName, &data, &dstData, &errType, &status, &ts)
		if err != nil {
			return []*pb.ValidationError{}, err
		}
		res = append(res, &pb.ValidationError{
			Id:        strconv.Itoa(id),
			Source:    source,
			SrcTable:  dbutil.TableName(srcSchemaName, srcTableName),
			DstTable:  dbutil.TableName(dstSchemaName, dstTableName),
			SrcData:   data,
			DstData:   dstData,
			ErrorType: mapErrType2Str[validateFailedType(errType)],
			Status:    pb.ValidateErrorState(status),
			Time:      ts,
		})
	}
	if err = rows.Err(); err != nil {
		return []*pb.ValidationError{}, err
	}
	c.tctx.L().Info("load validator errors", zap.Reflect("errors", res))
	return res, nil
}

func (c *validatorPersistHelper) operateError(validateOp pb.ValidationErrOp, errID uint64, isAll bool) error {
	if validateOp == pb.ValidationErrOp_ClearErrOp {
		return c.deleteError(errID, isAll)
	}
	sql := "UPDATE " + c.errorChangeTableName + " SET status=? WHERE source=?"
	var setStatus pb.ValidateErrorState
	switch validateOp {
	case pb.ValidationErrOp_IgnoreErrOp:
		setStatus = pb.ValidateErrorState_IgnoredErr
	case pb.ValidationErrOp_ResolveErrOp:
		setStatus = pb.ValidateErrorState_ResolvedErr
	default:
		// unsupported op should be caught by caller
		c.tctx.L().Warn("unsupported validator error operation", zap.Reflect("op", validateOp))
		return nil
	}
	args := []interface{}{
		int(setStatus),
		c.cfg.SourceID,
	}
	if !isAll {
		args = append(args, errID)
		sql += " AND id=?"
	}
	_, err := c.dbConn.QuerySQL(c.tctx, sql, args...)
	return err
}

func (c *validatorPersistHelper) deleteError(errID uint64, isAll bool) error {
	args := []interface{}{
		c.cfg.SourceID,
	}
	sql := "DELETE FROM " + c.errorChangeTableName + " WHERE source=?"
	if !isAll {
		sql += " AND id=?"
		args = append(args, errID)
	}
	_, err := c.dbConn.QuerySQL(c.tctx, sql, args...)
	return err
}
