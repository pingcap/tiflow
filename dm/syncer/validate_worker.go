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
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/meta/model"
	tidbmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/filter"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/unit"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	workerChannelSize = 1000

	maxAccumulatedRow = config.ValidatorMaxAccumulatedRow
	queryTimeout      = time.Minute
)

type validateFailedType int

const (
	deletedRowExists validateFailedType = iota
	rowNotExist
	rowDifferent
)

type validateFailedRow struct {
	tp      validateFailedType
	dstData []*sql.NullString

	srcJob *rowValidationJob
}

type validateWorker struct {
	sync.Mutex
	cfg                config.ValidatorConfig
	ctx                context.Context
	interval           time.Duration
	validator          *DataValidator
	L                  log.Logger
	db                 *conn.BaseDB
	rowChangeCh        chan *rowValidationJob
	batchSize          int
	rowErrorDelayInSec int64
	maxPendingRowSize  int64
	maxPendingRowCount int64

	pendingChangesMap map[string]*tableChangeJob
	pendingRowCounts  []int64
	pendingRowSize    int64
	accuRowCount      atomic.Int64 // accumulated row count from channel
	errorRows         []*validateFailedRow
}

func newValidateWorker(v *DataValidator, id int) *validateWorker {
	workerLog := v.L.WithFields(zap.Int("id", id))
	rowErrorDelayInSec := int64(v.cfg.ValidatorCfg.RowErrorDelay.Duration.Seconds())
	// skip error, already check it in cfg.Adjust
	maxPendingRowSize, _ := units.RAMInBytes(v.cfg.ValidatorCfg.MaxPendingRowSize)
	return &validateWorker{
		cfg:                v.cfg.ValidatorCfg,
		ctx:                v.ctx,
		interval:           v.validateInterval,
		validator:          v,
		L:                  workerLog,
		db:                 v.toDB,
		rowChangeCh:        make(chan *rowValidationJob, workerChannelSize),
		batchSize:          v.cfg.ValidatorCfg.BatchQuerySize,
		rowErrorDelayInSec: rowErrorDelayInSec,
		maxPendingRowSize:  maxPendingRowSize,
		maxPendingRowCount: int64(v.cfg.ValidatorCfg.MaxPendingRowCount),

		pendingChangesMap: make(map[string]*tableChangeJob),
		pendingRowCounts:  make([]int64, rowChangeTypeCount),
	}
}

func (vw *validateWorker) run() {
	validatedBeforeTimer := false
outer:
	for {
		select {
		case change, ok := <-vw.rowChangeCh:
			if !ok {
				break outer
			}
			if change.Tp == flushCheckpoint {
				// validate before flush to reduce the number of row changes
				vw.validateTableChange()
				validatedBeforeTimer = true
				change.wg.Done()
				break
			}

			vw.updateRowChange(change)
			vw.accuRowCount.Add(1)
			// reduce number of pending rows
			if vw.accuRowCount.Load() >= maxAccumulatedRow {
				vw.validateTableChange()
				validatedBeforeTimer = true
			}
		case <-time.After(vw.interval):
			// reduce the number of validation
			if validatedBeforeTimer {
				validatedBeforeTimer = false
				break
			}
			vw.validateTableChange()
		}
	}
}

func (vw *validateWorker) updateRowChange(job *rowValidationJob) {
	vw.Lock()
	defer vw.Unlock()
	// cluster using target table
	tbl := job.row.GetTargetTable()
	targetTable := filter.Table{Schema: tbl.Schema, Name: tbl.Table}
	fullTableName := targetTable.String()
	change := vw.pendingChangesMap[fullTableName]
	if change == nil {
		// no change of this table
		change = newTableChangeJob()
		vw.pendingChangesMap[fullTableName] = change
	}
	if change.addOrUpdate(job) {
		vw.newJobAdded(job)
	}
}

func (vw *validateWorker) validateTableChange() {
	var err error
	defer func() {
		if err != nil && !isRetryableValidateError(err) {
			vw.validator.sendError(terror.ErrValidatorValidateChange.Delegate(err))
		}
		if panicErr := recover(); panicErr != nil {
			vw.L.Error("worker panic", zap.Any("err", panicErr))
			vw.validator.sendError(terror.ErrValidatorPanic.Generate(panicErr))
		}
	}()

	// clear accumulated row counter
	vw.accuRowCount.Store(0)

	if vw.getAllPendingRowCount() == 0 {
		vw.L.Debug("pending row count = 0, skip validation")
		return
	}

	failedChanges := make(map[string]map[string]*validateFailedRow)
	for k, tblChange := range vw.pendingChangesMap {
		var insertUpdateChanges, deleteChanges []*rowValidationJob
		for _, r := range tblChange.jobs {
			if r.Tp == rowDeleted {
				deleteChanges = append(deleteChanges, r)
			} else {
				insertUpdateChanges = append(insertUpdateChanges, r)
			}
		}
		allFailedRows := make(map[string]*validateFailedRow)
		validateFunc := func(rows []*rowValidationJob, isDelete bool) error {
			if len(rows) == 0 {
				return nil
			}
			failedRows, err2 := vw.validateRowChanges(rows, isDelete)
			if err2 != nil {
				return err2
			}
			for key, val := range failedRows {
				allFailedRows[key] = val
			}
			return nil
		}
		if err = validateFunc(insertUpdateChanges, false); err != nil {
			return
		}
		if err = validateFunc(deleteChanges, true); err != nil {
			return
		}
		if len(allFailedRows) > 0 {
			failedChanges[k] = allFailedRows
		}
	}

	vw.updatePendingAndErrorRows(failedChanges)

	// check whether we need to stop validation
	pendingRowSize := vw.validator.getPendingRowSize()
	allPendingRowCount := vw.validator.getAllPendingRowCount()
	if pendingRowSize > vw.maxPendingRowSize || allPendingRowCount > vw.maxPendingRowCount {
		vw.validator.sendError(terror.ErrValidatorTooMuchPending.Generate(
			pendingRowSize, vw.maxPendingRowSize,
			allPendingRowCount, vw.maxPendingRowCount),
		)
	}
}

func (vw *validateWorker) updatePendingAndErrorRows(failedChanges map[string]map[string]*validateFailedRow) {
	vw.Lock()
	defer vw.Unlock()

	newPendingCnt := make([]int64, rowChangeTypeCount)
	newPendingRowSize := int64(0)
	allErrorRows := make([]*validateFailedRow, 0)
	newPendingChanges := make(map[string]*tableChangeJob)
	validateTS := time.Now().Unix()
	for tblKey, rows := range failedChanges {
		tblChange := vw.pendingChangesMap[tblKey]
		newPendingRows := make(map[string]*rowValidationJob)
		for pk, row := range rows {
			job := tblChange.jobs[pk]
			if vw.validator.isMarkErrorStarted() {
				job.FailedCnt++
				if job.FirstValidateTS == 0 {
					job.FirstValidateTS = validateTS
				}

				if validateTS-job.FirstValidateTS >= vw.rowErrorDelayInSec {
					row.srcJob = job
					allErrorRows = append(allErrorRows, row)
				} else {
					newPendingRows[pk] = job
					newPendingCnt[job.Tp]++
					newPendingRowSize += int64(job.size)
				}
			} else {
				newPendingRows[pk] = job
				newPendingCnt[job.Tp]++
				newPendingRowSize += int64(job.size)
			}
		}
		if len(newPendingRows) > 0 {
			newPendingChanges[tblKey] = &tableChangeJob{
				jobs: newPendingRows,
			}
		}
	}

	vw.L.Debug("pending row count (insert, update, delete)", zap.Int64s("before", vw.pendingRowCounts),
		zap.Int64s("after", newPendingCnt))
	vw.setPendingRowCountsAndSize(newPendingCnt, newPendingRowSize)
	vw.pendingChangesMap = newPendingChanges
	vw.errorRows = append(vw.errorRows, allErrorRows...)
	vw.validator.incrErrorRowCount(len(allErrorRows))
}

func (vw *validateWorker) validateRowChanges(rows []*rowValidationJob, deleteChange bool) (map[string]*validateFailedRow, error) {
	res := make(map[string]*validateFailedRow)
	for start := 0; start < len(rows); start += vw.batchSize {
		end := start + vw.batchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[start:end]
		failedRows, err := vw.batchValidateRowChanges(batch, deleteChange)
		if err != nil {
			return nil, err
		}
		for k, v := range failedRows {
			res[k] = v
		}
	}
	return res, nil
}

func (vw *validateWorker) getPendingChangesMap() map[string]*tableChangeJob {
	vw.Lock()
	defer vw.Unlock()
	return vw.pendingChangesMap
}

func (vw *validateWorker) getErrorRows() []*validateFailedRow {
	vw.Lock()
	defer vw.Unlock()
	return vw.errorRows
}

func (vw *validateWorker) batchValidateRowChanges(rows []*rowValidationJob, deleteChange bool) (map[string]*validateFailedRow, error) {
	failpoint.Inject("ValidatorWorkerPanic", func() {})

	pkValues := make([][]string, 0, len(rows))
	for _, r := range rows {
		pkValues = append(pkValues, r.row.RowStrIdentity())
	}
	firstRow := rows[0].row
	cond := &Cond{
		TargetTbl: firstRow.TargetTableID(),
		Columns:   firstRow.SourceTableInfo().Columns,
		PK:        firstRow.UniqueNotNullIdx(),
		PkValues:  pkValues,
	}
	var failedRows map[string]*validateFailedRow
	var err error
	if deleteChange {
		failedRows, err = vw.validateDeletedRows(cond)
	} else {
		failedRows, err = vw.validateInsertAndUpdateRows(rows, cond)
	}
	if err != nil {
		return nil, err
	}
	return failedRows, nil
}

func (vw *validateWorker) validateDeletedRows(cond *Cond) (map[string]*validateFailedRow, error) {
	targetRows, err := vw.getTargetRows(cond)
	if err != nil {
		return nil, err
	}

	failedRows := make(map[string]*validateFailedRow, len(targetRows))
	for key, val := range targetRows {
		failedRows[key] = &validateFailedRow{tp: deletedRowExists, dstData: val}
	}
	return failedRows, nil
}

func (vw *validateWorker) validateInsertAndUpdateRows(rows []*rowValidationJob, cond *Cond) (map[string]*validateFailedRow, error) {
	failedRows := make(map[string]*validateFailedRow)
	sourceRows := getSourceRowsForCompare(rows)
	targetRows, err := vw.getTargetRows(cond)
	if err != nil {
		return nil, err
	}

	if len(targetRows) > len(sourceRows) {
		// if this happens, downstream should have removed the primary key
		vw.L.Debug("more data on downstream, may come from other client")
	}

	firstRow := rows[0].row
	tableInfo := firstRow.SourceTableInfo()
	validateContext := &validateCompareContext{
		logger:      vw.L,
		sourceTable: firstRow.GetSourceTable(),
		targetTable: firstRow.GetTargetTable(),
		columns:     tableInfo.Columns,
	}
	for key, sourceRow := range sourceRows {
		targetRow, ok := targetRows[key]
		if !ok {
			failedRows[key] = &validateFailedRow{tp: rowNotExist}
			continue
		}
		if vw.cfg.Mode == config.ValidationFull {
			// only compare the whole row in full mode
			eq, err2 := validateContext.compareData(key, sourceRow, targetRow)
			if err2 != nil {
				return nil, err2
			}
			if !eq {
				failedRows[key] = &validateFailedRow{tp: rowDifferent, dstData: targetRow}
			}
		}
	}
	return failedRows, nil
}

func (vw *validateWorker) getTargetRows(cond *Cond) (map[string][]*sql.NullString, error) {
	ctx, cancelFunc := context.WithTimeout(vw.ctx, queryTimeout)
	defer cancelFunc()
	tctx := tcontext.NewContext(ctx, vw.L)
	columnNames := make([]string, 0, len(cond.Columns))
	for _, col := range cond.Columns {
		columnNames = append(columnNames, dbutil.ColumnName(col.Name.O))
	}
	columns := strings.Join(columnNames, ", ")
	rowsQuery := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ %s FROM %s WHERE %s",
		columns, cond.TargetTbl, cond.GetWhere())
	// query using sql.DB directly, BaseConn is more than what we need
	rows, err := vw.db.QueryContext(tctx, rowsQuery, cond.GetArgs()...)
	if err != nil {
		if isRetryableValidateError(err) {
			vw.L.Info("met retryable error", zap.Error(err))
		} else {
			vw.L.Error("failed to query",
				zap.String("query", utils.TruncateString(rowsQuery, -1)),
				zap.String("args", utils.TruncateInterface(cond.GetArgs(), -1)))
			err = errors.Trace(err)
		}
		return nil, err
	}
	defer rows.Close()

	result := make(map[string][]*sql.NullString)
	for rows.Next() {
		rowData, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		pkCols := cond.PK.Columns
		pkValues := make([]string, 0, len(pkCols))
		for _, col := range pkCols {
			pkValues = append(pkValues, rowData[col.Offset].String)
		}
		pk := genRowKeyByString(pkValues)
		result[pk] = rowData
	}
	return result, rows.Err()
}

func (vw *validateWorker) close() {
	close(vw.rowChangeCh)
}

func (vw *validateWorker) resetErrorRows() {
	vw.Lock()
	defer vw.Unlock()
	vw.errorRows = make([]*validateFailedRow, 0)
}

func (vw *validateWorker) newJobAdded(job *rowValidationJob) {
	tp := job.Tp
	vw.pendingRowCounts[tp]++
	vw.validator.addPendingRowCount(tp, 1)

	vw.pendingRowSize += int64(job.size)
	vw.validator.addPendingRowSize(int64(job.size))
}

func (vw *validateWorker) setPendingRowCountsAndSize(newCounts []int64, newSize int64) {
	for tp, val := range newCounts {
		diff := val - vw.pendingRowCounts[tp]
		vw.pendingRowCounts[tp] = val
		vw.validator.addPendingRowCount(rowChangeJobType(tp), diff)
	}

	diff := newSize - vw.pendingRowSize
	vw.pendingRowSize = newSize
	vw.validator.addPendingRowSize(diff)
}

func (vw *validateWorker) getAllPendingRowCount() int64 {
	return vw.pendingRowCounts[rowInsert] + vw.pendingRowCounts[rowUpdated] + vw.pendingRowCounts[rowDeleted]
}

type validateCompareContext struct {
	logger      log.Logger
	sourceTable *cdcmodel.TableName
	targetTable *cdcmodel.TableName
	columns     []*model.ColumnInfo
}

// a simplified version of https://github.com/pingcap/tidb-tools/blob/d9fdfa2f9040aab3fab7cd11774a82226f467fe7/sync_diff_inspector/utils/utils.go#L487-L606
func (c *validateCompareContext) compareData(key string, sourceData, targetData []*sql.NullString) (bool, error) {
	for i, column := range c.columns {
		data1, data2 := sourceData[i], targetData[i]
		if data1.Valid != data2.Valid {
			return false, nil
		}
		str1, str2 := data1.String, data2.String
		if str1 == str2 {
			continue
		} else if column.FieldType.GetType() == tidbmysql.TypeFloat || column.FieldType.GetType() == tidbmysql.TypeDouble {
			// source and target data have different precision?
			num1, err1 := strconv.ParseFloat(str1, 64)
			num2, err2 := strconv.ParseFloat(str2, 64)
			if err1 != nil || err2 != nil {
				// should not happen
				return false, errors.Errorf("convert %s, %s to float failed, err1: %v, err2: %v", str1, str2, err1, err2)
			}
			if math.Abs(num1-num2) <= 1e-6 {
				continue
			}
		}
		if c.logger.Core().Enabled(zap.DebugLevel) {
			c.logger.Debug("compare failed",
				zap.Stringer("src table", c.sourceTable),
				zap.Stringer("dst table", c.targetTable),
				zap.String("col", fmt.Sprintf("%s %s", column.Name, column.GetTypeDesc())),
				zap.String("key", key),
				zap.Reflect("src data", data1), zap.Reflect("dst data", data2))
		}
		return false, nil
	}

	return true, nil
}

func isRetryableValidateError(err error) bool {
	err = errors.Cause(err)
	return err == context.DeadlineExceeded || isRetryableDBError(err)
}

func isRetryableDBError(err error) bool {
	return unit.IsResumableDBError(err)
}

func scanRow(rows *sql.Rows) ([]*sql.NullString, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	colVals := make([][]byte, len(cols))
	colValsI := make([]interface{}, len(colVals))
	for i := range colValsI {
		colValsI[i] = &colVals[i]
	}

	err = rows.Scan(colValsI...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := make([]*sql.NullString, len(cols))
	for i := range colVals {
		result[i] = &sql.NullString{
			String: string(colVals[i]),
			Valid:  colVals[i] != nil,
		}
	}

	return result, nil
}

func getSourceRowsForCompare(jobs []*rowValidationJob) map[string][]*sql.NullString {
	rowMap := make(map[string][]*sql.NullString, len(jobs))
	for _, j := range jobs {
		r := j.row
		colValues := make([]*sql.NullString, r.ColumnCount())
		rowValues := r.RowValues()
		for i := range rowValues {
			var colData string
			if rowValues[i] != nil {
				colData = sqlmodel.ColValAsStr(rowValues[i])
			}
			colValues[i] = &sql.NullString{
				String: colData,
				Valid:  rowValues[i] != nil,
			}
		}
		rowMap[j.Key] = colValues
	}
	return rowMap
}
