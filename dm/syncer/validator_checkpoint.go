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
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tidb/util/filter"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	maxRowKeyLength = 64

	validationDBTimeout = queryTimeout * 5
)

var mapErrType2Str = map[validateFailedType]string{
	deletedRowExists: "Deleted rows exist",
	rowNotExist:      "Expected rows not exist",
	rowDifferent:     "Column data not matched",
}

var maxRowKeyLengthStr = strconv.Itoa(maxRowKeyLength)

type validatorPersistHelper struct {
	L         log.Logger
	cfg       *config.SubTaskConfig
	validator *DataValidator
	retryer   *retry.FiniteRetryer

	checkpointTableName    string
	pendingChangeTableName string
	errorChangeTableName   string
	tableStatusTableName   string

	db                *conn.BaseDB
	schemaInitialized atomic.Bool
	revision          int64
}

func newValidatorCheckpointHelper(validator *DataValidator) *validatorPersistHelper {
	cfg := validator.cfg
	logger := validator.L
	retryer := &retry.FiniteRetryer{
		Params: retry.NewParams(3, 5*time.Second, retry.LinearIncrease,
			func(i int, err error) bool {
				logger.Warn("met error", zap.Error(err))
				return isRetryableDBError(err)
			},
		),
	}
	c := &validatorPersistHelper{
		L:         logger,
		cfg:       cfg,
		validator: validator,
		retryer:   retryer,

		checkpointTableName:    dbutil.TableName(cfg.MetaSchema, cputil.ValidatorCheckpoint(cfg.Name)),
		pendingChangeTableName: dbutil.TableName(cfg.MetaSchema, cputil.ValidatorPendingChange(cfg.Name)),
		errorChangeTableName:   dbutil.TableName(cfg.MetaSchema, cputil.ValidatorErrorChange(cfg.Name)),
		tableStatusTableName:   dbutil.TableName(cfg.MetaSchema, cputil.ValidatorTableStatus(cfg.Name)),
	}

	return c
}

func (c *validatorPersistHelper) init(tctx *tcontext.Context) error {
	c.db = c.validator.toDB

	if !c.schemaInitialized.Load() {
		workFunc := func(tctx *tcontext.Context) (interface{}, error) {
			return nil, c.createSchemaAndTables(tctx)
		}
		if _, cnt, err := c.retryer.Apply(tctx, workFunc); err != nil {
			tctx.L().Error("failed to init validator helper after retry",
				zap.Int("retry-times", cnt), zap.Error(err))
			return err
		}

		c.schemaInitialized.Store(true)
	}
	return nil
}

func (c *validatorPersistHelper) createSchemaAndTables(tctx *tcontext.Context) error {
	if err := c.createSchema(tctx); err != nil {
		return err
	}
	return c.createTable(tctx)
}

func (c *validatorPersistHelper) createSchema(tctx *tcontext.Context) error {
	query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", dbutil.ColumnName(c.cfg.MetaSchema))
	_, err := c.db.ExecContext(tctx, query)
	tctx.L().Info("create checkpoint schema", zap.String("statement", query))
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
			procd_ins BIGINT UNSIGNED NOT NULL,
			procd_upd BIGINT UNSIGNED NOT NULL,
			procd_del BIGINT UNSIGNED NOT NULL,
			revision bigint NOT NULL,
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
			INDEX idx_source_schema_table_key(source, schema_name, table_name, row_pk),
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
			UNIQUE KEY uk_source_schema_table_key(source, src_schema_name, src_table_name),
			INDEX idx_stage(stage)
		)`,
	}
	tctx.L().Info("create checkpoint and data table", zap.Strings("statements", sqls))
	for _, q := range sqls {
		if _, err := c.db.ExecContext(tctx, q); err != nil {
			return err
		}
	}
	return nil
}

type tableChangeDataForPersist struct {
	sourceTable *filter.Table
	columnCount int
	rows        map[string]*rowChangeDataForPersist
}

type rowChangeDataForPersist struct {
	Key       string           `json:"key"`
	Tp        rowChangeJobType `json:"tp"`
	Size      int32            `json:"size"`
	Data      []interface{}    `json:"data"`
	FailedCnt int              `json:"failed-cnt"` // failed count
}

var triggeredFailOnPersistForIntegrationTest bool

func (c *validatorPersistHelper) execQueriesWithRetry(tctx *tcontext.Context, queries []string, args [][]interface{}) error {
	workFunc := func(tctx *tcontext.Context) (interface{}, error) {
		for i, q := range queries {
			failpoint.Inject("ValidatorFailOnPersist", func() {
				// on persist pending row changes, the queries would be [delete, insert...]
				// if there are 5 inserts, we fail for one time
				// for source mysql-replica-01, fail on the 3rd
				// for source mysql-replica-02, fail on the 4th
				if strings.Contains(q, "_validator_pending_change") && len(queries) == 6 &&
					!triggeredFailOnPersistForIntegrationTest {
					if (c.cfg.SourceID == "mysql-replica-01" && i == 3) ||
						(c.cfg.SourceID == "mysql-replica-02" && i == 4) {
						triggeredFailOnPersistForIntegrationTest = true
						// "Error 1406" is non-resumable error, so we can't retry it
						failpoint.Return(nil, errors.New("ValidatorFailOnPersist Error 1366"))
					}
				}
			})
			if _, err := c.db.ExecContext(tctx, q, args[i]...); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	_, _, err2 := c.retryer.Apply(tctx, workFunc)
	return err2
}

func (c *validatorPersistHelper) persistTableStatusAndErrors(tctx *tcontext.Context) error {
	// get snapshot of the current table status
	tableStatus := c.validator.getTableStatusMap()
	count := len(tableStatus) + int(c.validator.getNewErrorRowCount())
	queries := make([]string, 0, count)
	args := make([][]interface{}, 0, count)

	// upsert table status
	for _, state := range tableStatus {
		query := `INSERT INTO ` + c.tableStatusTableName + `
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
		queries = append(queries, query)
		args = append(args, []interface{}{
			c.cfg.SourceID, state.source.Schema, state.source.Name, state.target.Schema, state.target.Name,
			int(state.stage), state.message,
		},
		)
	}
	// upsert error rows
	for _, worker := range c.validator.getWorkers() {
		for _, r := range worker.getErrorRows() {
			query := `INSERT INTO ` + c.errorChangeTableName + `
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
			queries = append(queries, query)

			row := r.srcJob.row
			srcDataBytes, err := json.Marshal(row.RowValues())
			if err != nil {
				return err
			}
			dstData := make([]interface{}, len(r.dstData))
			for i, d := range r.dstData {
				if d.Valid {
					dstData[i] = d.String
				}
			}
			dstDataBytes, err := json.Marshal(dstData)
			if err != nil {
				return err
			}
			sourceTable := row.GetSourceTable()
			targetTable := row.GetTargetTable()
			args = append(args, []interface{}{
				c.cfg.SourceID, sourceTable.Schema, sourceTable.Table, r.srcJob.Key,
				targetTable.Schema, targetTable.Table,
				string(srcDataBytes), string(dstDataBytes), r.tp, pb.ValidateErrorState_NewErr,
			})
		}
	}

	return c.execQueriesWithRetry(tctx, queries, args)
}

func (c *validatorPersistHelper) persistPendingRows(tctx *tcontext.Context, rev int64) error {
	count := int(c.validator.getAllPendingRowCount()) + 1
	queries := make([]string, 0, count)
	args := make([][]interface{}, 0, count)

	// delete pending rows left by previous failed call of "persist"
	queries = append(queries, `DELETE FROM `+c.pendingChangeTableName+` WHERE source = ? and revision = ?`)
	args = append(args, []interface{}{c.cfg.SourceID, rev})
	// insert pending row changes with revision=rev
	for _, worker := range c.validator.getWorkers() {
		for _, tblChange := range worker.getPendingChangesMap() {
			for key, j := range tblChange.jobs {
				row := j.row
				// we don't store FirstValidateTS into meta
				rowForPersist := rowChangeDataForPersist{
					Key:       key,
					Tp:        j.Tp,
					Size:      j.size,
					Data:      row.RowValues(),
					FailedCnt: j.FailedCnt,
				}
				rowJSON, err := json.Marshal(&rowForPersist)
				if err != nil {
					return err
				}
				query := `INSERT INTO ` + c.pendingChangeTableName + `
						(source, schema_name, table_name, row_pk, data, revision) VALUES (?, ?, ?, ?, ?, ?)`
				queries = append(queries, query)
				sourceTable := row.GetSourceTable()
				args = append(args, []interface{}{
					c.cfg.SourceID,
					sourceTable.Schema,
					sourceTable.Table,
					key,
					string(rowJSON),
					rev,
				})
			}
		}
	}
	return c.execQueriesWithRetry(tctx, queries, args)
}

func (c *validatorPersistHelper) persist(tctx *tcontext.Context, loc binlog.Location) error {
	newCtx, cancelFunc := tctx.WithTimeout(validationDBTimeout)
	defer cancelFunc()
	// we run sql one by one to avoid potential "transaction too large" error since pending data may quite large.
	// we use "upsert" to save table status and error row changes, if error happens when persist checkpoint and
	// pending data, those data maybe inconsistent with each other. but it doesn't matter in validation case,
	// since the status of the table will be in correct stage after resume and reach the location again.
	if err := c.persistTableStatusAndErrors(newCtx); err != nil {
		return err
	}

	nextRevision := c.revision + 1

	// we use "insert" to save pending data to speed up(see https://asktug.com/t/topic/33147) since
	// the number of pending rows maybe large.
	// we run sql one by one to avoid potential "transaction too large" error since pending data may quite large.
	// And use revision field to associate checkpoint table and pending row table
	if err := c.persistPendingRows(newCtx, nextRevision); err != nil {
		return err
	}

	// upsert checkpoint
	query := `INSERT INTO ` + c.checkpointTableName +
		`(source, binlog_name, binlog_pos, binlog_gtid, procd_ins, procd_upd, procd_del, revision)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE
			source = VALUES(source),
			binlog_name = VALUES(binlog_name),
			binlog_pos = VALUES(binlog_pos),
			binlog_gtid = VALUES(binlog_gtid),
			procd_ins = VALUES(procd_ins),
			procd_upd = VALUES(procd_upd),
			procd_del = VALUES(procd_del),
			revision = VALUES(revision)
		`
	rowCounts := c.validator.getProcessedRowCounts()
	args := []interface{}{
		c.cfg.SourceID, loc.Position.Name, loc.Position.Pos, loc.GTIDSetStr(),
		rowCounts[rowInsert], rowCounts[rowUpdated], rowCounts[rowDeleted],
		nextRevision,
	}
	if err := c.execQueriesWithRetry(newCtx, []string{query}, [][]interface{}{args}); err != nil {
		return err
	}

	// if we reach here, checkpoint with new revision is persisted successfully,
	// but we need to clean up previous pending row changes, i.e. rows with different revision.
	// it's ok to fail here, next persist will try to delete again, so just log it.
	query = `DELETE FROM ` + c.pendingChangeTableName + ` WHERE source = ? and revision != ?`
	args = []interface{}{c.cfg.SourceID, nextRevision}
	if err := c.execQueriesWithRetry(newCtx, []string{query}, [][]interface{}{args}); err != nil {
		c.L.Warn("failed to delete previous pending row changes", zap.Error(err), zap.Reflect("args", args))
		// nolint:nilerr
	}

	// to next revision
	c.revision++

	return nil
}

type persistedData struct {
	checkpoint         *binlog.Location
	processedRowCounts []int64
	pendingChanges     map[string]*tableChangeDataForPersist // key is full name of source table
	rev                int64
	tableStatus        map[string]*tableValidateStatus // key is full name of source table
}

func (c *validatorPersistHelper) loadPersistedDataRetry(tctx *tcontext.Context) (*persistedData, error) {
	start := time.Now()
	newCtx, cancelFunc := tctx.WithTimeout(validationDBTimeout)
	defer cancelFunc()
	workFunc := func(tctx *tcontext.Context) (interface{}, error) {
		return c.loadPersistedData(tctx)
	}
	ret, i, err := c.retryer.Apply(newCtx, workFunc)
	if err != nil {
		c.L.Error("failed load persisted data after retry", zap.Int("retry-times", i), zap.Error(err))
		return nil, err
	}

	c.L.Info("loaded persisted data", zap.Duration("time taken", time.Since(start)))
	return ret.(*persistedData), nil
}

func (c *validatorPersistHelper) loadPersistedData(tctx *tcontext.Context) (*persistedData, error) {
	var err error
	data := &persistedData{}
	data.checkpoint, data.processedRowCounts, data.rev, err = c.loadCheckpoint(tctx)
	if err != nil {
		return data, err
	}

	data.pendingChanges, err = c.loadPendingChange(tctx, data.rev)
	if err != nil {
		return data, err
	}

	data.tableStatus, err = c.loadTableStatus(tctx)
	if err != nil {
		return data, err
	}

	return data, nil
}

func (c *validatorPersistHelper) loadCheckpoint(tctx *tcontext.Context) (*binlog.Location, []int64, int64, error) {
	start := time.Now()
	query := `select
				binlog_name, binlog_pos, binlog_gtid,
				procd_ins, procd_upd, procd_del,
				revision
			  from ` + c.checkpointTableName + ` where source = ?`
	rows, err := c.db.QueryContext(tctx, query, c.cfg.SourceID)
	if err != nil {
		return nil, nil, 0, err
	}
	defer rows.Close()

	var location *binlog.Location
	processRowCounts := make([]int64, rowChangeTypeCount)

	// at most one row
	var revision int64
	if rows.Next() {
		var (
			binlogName, binlogGtidStr string
			ins, upd, del             int64
			binlogPos                 uint32
		)

		err = rows.Scan(&binlogName, &binlogPos, &binlogGtidStr, &ins, &upd, &del, &revision)
		if err != nil {
			return nil, nil, 0, err
		}
		gset, err2 := gtid.ParserGTID(c.cfg.Flavor, binlogGtidStr)
		if err2 != nil {
			return nil, nil, 0, err2
		}
		tmpLoc := binlog.NewLocation(mysql.Position{Name: binlogName, Pos: binlogPos}, gset)
		location = &tmpLoc
		processRowCounts = []int64{ins, upd, del}
	}
	if err = rows.Err(); err != nil {
		return nil, nil, 0, err
	}
	c.L.Info("checkpoint loaded", zap.Reflect("loc", location),
		zap.Int64s("processed(i, u, d)", processRowCounts), zap.Int64("rev", revision),
		zap.Duration("time taken", time.Since(start)))
	return location, processRowCounts, revision, nil
}

func (c *validatorPersistHelper) loadPendingChange(tctx *tcontext.Context, rev int64) (map[string]*tableChangeDataForPersist, error) {
	start := time.Now()
	res := make(map[string]*tableChangeDataForPersist)
	query := "select schema_name, table_name, row_pk, data, revision from " + c.pendingChangeTableName +
		" where source = ? and revision = ?"
	rows, err := c.db.QueryContext(tctx, query, c.cfg.SourceID, rev)
	if err != nil {
		return nil, err
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
			return nil, err
		}
		var row *rowChangeDataForPersist
		err = json.Unmarshal(data, &row)
		if err != nil {
			return nil, err
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
		return nil, err
	}
	c.L.Info("pending change loaded", zap.Reflect("count", count), zap.Reflect("rev", rev),
		zap.Duration("time taken", time.Since(start)))
	return res, nil
}

func (c *validatorPersistHelper) loadTableStatus(tctx *tcontext.Context) (map[string]*tableValidateStatus, error) {
	start := time.Now()
	res := make(map[string]*tableValidateStatus)
	query := "select src_schema_name, src_table_name, dst_schema_name, dst_table_name, stage, message from " +
		c.tableStatusTableName + " where source = ?"
	rows, err := c.db.QueryContext(tctx, query, c.cfg.SourceID)
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
	c.L.Info("table status loaded", zap.Reflect("count", len(res)),
		zap.Duration("time taken", time.Since(start)))
	return res, nil
}

func (c *validatorPersistHelper) loadErrorCount(tctx *tcontext.Context, db *conn.BaseDB) (map[pb.ValidateErrorState]int64, error) {
	res := make(map[pb.ValidateErrorState]int64)
	query := "select status, count(*) from " + c.errorChangeTableName + " where source = ? group by status"
	rows, err := db.QueryContext(tctx, query, c.cfg.SourceID)
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
	c.L.Info("error count loaded", zap.Reflect("counts", res))
	return res, nil
}

func (c *validatorPersistHelper) setRevision(rev int64) {
	c.revision = rev
}

func (c *validatorPersistHelper) loadError(tctx *tcontext.Context, db *conn.BaseDB, filterState pb.ValidateErrorState) ([]*pb.ValidationError, error) {
	var (
		rows *sql.Rows
		err  error
	)
	res := make([]*pb.ValidationError, 0)
	args := []interface{}{
		c.cfg.SourceID,
	}
	query := "SELECT id, source, src_schema_name, src_table_name, dst_schema_name, dst_table_name, data, dst_data, error_type, status, update_time " +
		"FROM " + c.errorChangeTableName + " WHERE source = ?"
	if filterState != pb.ValidateErrorState_InvalidErr {
		query += " AND status=?"
		args = append(args, int(filterState))
	}
	// we do not retry, let user do it
	rows, err = db.QueryContext(tctx, query, args...)
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
	c.L.Info("load validator errors", zap.Int("count", len(res)))
	return res, nil
}

func (c *validatorPersistHelper) operateError(tctx *tcontext.Context, db *conn.BaseDB, validateOp pb.ValidationErrOp, errID uint64, isAll bool) error {
	if validateOp == pb.ValidationErrOp_ClearErrOp {
		return c.deleteError(tctx, db, errID, isAll)
	}
	query := "UPDATE " + c.errorChangeTableName + " SET status=? WHERE source=?"
	var setStatus pb.ValidateErrorState
	switch validateOp {
	case pb.ValidationErrOp_IgnoreErrOp:
		setStatus = pb.ValidateErrorState_IgnoredErr
	case pb.ValidationErrOp_ResolveErrOp:
		setStatus = pb.ValidateErrorState_ResolvedErr
	default:
		// unsupported op should be caught by caller
		c.L.Warn("unsupported validator error operation", zap.Reflect("op", validateOp))
		return nil
	}
	args := []interface{}{
		int(setStatus),
		c.cfg.SourceID,
	}
	if !isAll {
		args = append(args, errID)
		query += " AND id=?"
	}
	// we do not retry, let user do it
	_, err := db.ExecContext(tctx, query, args...)
	return err
}

func (c *validatorPersistHelper) deleteError(tctx *tcontext.Context, db *conn.BaseDB, errID uint64, isAll bool) error {
	args := []interface{}{
		c.cfg.SourceID,
	}
	query := "DELETE FROM " + c.errorChangeTableName + " WHERE source=?"
	if !isAll {
		query += " AND id=?"
		args = append(args, errID)
	}
	// we do not retry, let user do it
	_, err := db.ExecContext(tctx, query, args...)
	return err
}
