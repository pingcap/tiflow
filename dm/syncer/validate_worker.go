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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/model"
	tidbmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/dbutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
)

const (
	workerChannelSize = 1000

	MaxAccumulatedRowBeforeValidate = 1000 // todo: make it configurable
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

	srcRow *rowChange
}

type validateWorker struct {
	sync.Mutex
	cfg                config.ValidatorConfig
	ctx                context.Context
	interval           time.Duration
	validator          *DataValidator
	L                  log.Logger
	conn               *dbconn.DBConn
	rowChangeCh        chan *rowChange
	batchSize          int
	rowErrorDelayInSec int64

	pendingChangesMap map[string]*tableChange
	pendingRowCounts  []int64
	accuRowCount      atomic.Int64 // accumulated row count from channel
	errorRows         []*validateFailedRow
}

func newValidateWorker(v *DataValidator, id int) *validateWorker {
	workerLog := v.L.WithFields(zap.Int("id", id))
	rowErrorDelayInSec := int64(v.cfg.ValidatorCfg.RowErrorDelay.Duration.Seconds())
	return &validateWorker{
		cfg:                v.cfg.ValidatorCfg,
		ctx:                v.ctx,
		interval:           v.validateInterval,
		validator:          v,
		L:                  workerLog,
		conn:               v.toDBConns[id],
		rowChangeCh:        make(chan *rowChange, workerChannelSize),
		batchSize:          v.cfg.ValidatorCfg.BatchQuerySize,
		rowErrorDelayInSec: rowErrorDelayInSec,

		pendingChangesMap: make(map[string]*tableChange),
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
			if vw.accuRowCount.Load() >= MaxAccumulatedRowBeforeValidate {
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

func (vw *validateWorker) updateRowChange(row *rowChange) {
	vw.Lock()
	defer vw.Unlock()
	// cluster using target table
	fullTableName := row.table.Target.String()
	change := vw.pendingChangesMap[fullTableName]
	if change == nil {
		// no change of this table
		change = newTableChange(row.table)
		vw.pendingChangesMap[fullTableName] = change
	}
	if val, ok := change.rows[row.Key]; ok {
		val.Data = row.Data
		val.Tp = row.Tp
		val.FirstValidateTS = 0
		val.FailedCnt = 0 // clear failed count
	} else {
		change.rows[row.Key] = row
		vw.incrPendingRowCount(row.Tp)
	}
}

func (vw *validateWorker) validateTableChange() {
	var err error
	defer func() {
		if err != nil {
			// todo: better error handling
			vw.validator.errChan <- terror.Annotate(err, "failed to validate table change")
		}
	}()

	// clear accumulated row counter
	vw.accuRowCount.Store(0)

	failedChanges := make(map[string]map[string]*validateFailedRow)
	for k, tblChange := range vw.pendingChangesMap {
		var insertUpdateChanges, deleteChanges []*rowChange
		for _, r := range tblChange.rows {
			if r.Tp == rowDeleted {
				deleteChanges = append(deleteChanges, r)
			} else {
				insertUpdateChanges = append(insertUpdateChanges, r)
			}
		}
		allFailedRows := make(map[string]*validateFailedRow)
		validateFunc := func(rows []*rowChange, isDelete bool) error {
			if len(rows) == 0 {
				return nil
			}
			failedRows, err2 := vw.validateRowChanges(tblChange.table, rows, isDelete)
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
}

func (vw *validateWorker) updatePendingAndErrorRows(failedChanges map[string]map[string]*validateFailedRow) {
	vw.Lock()
	defer vw.Unlock()

	newPendingCnt := make([]int64, rowChangeTypeCount)
	allErrorRows := make([]*validateFailedRow, 0)
	newPendingChanges := make(map[string]*tableChange)
	validateTS := time.Now().Unix()
	for tblKey, rows := range failedChanges {
		tblChange := vw.pendingChangesMap[tblKey]
		newPendingRows := make(map[string]*rowChange)
		for pk, row := range rows {
			r := tblChange.rows[pk]
			if vw.validator.hasReachedSyncer() {
				r.FailedCnt++
				if r.FirstValidateTS == 0 {
					r.FirstValidateTS = validateTS
				}

				if validateTS-r.FirstValidateTS >= vw.rowErrorDelayInSec {
					row.srcRow = r
					allErrorRows = append(allErrorRows, row)
				} else {
					newPendingRows[pk] = r
					newPendingCnt[r.Tp]++
				}
			} else {
				newPendingRows[pk] = r
				newPendingCnt[r.Tp]++
			}
		}
		if len(newPendingRows) > 0 {
			newPendingChanges[tblKey] = &tableChange{
				table: tblChange.table,
				rows:  newPendingRows,
			}
		}
	}

	vw.L.Debug("pending row count (insert, update, delete)", zap.Int64s("before", vw.pendingRowCounts),
		zap.Int64s("after", newPendingCnt))
	vw.setPendingRowCounts(newPendingCnt)
	vw.pendingChangesMap = newPendingChanges
	vw.errorRows = append(vw.errorRows, allErrorRows...)
	vw.validator.incrErrorRowCount(newValidateErrorRow, len(allErrorRows))
}

func (vw *validateWorker) validateRowChanges(table *validateTableInfo, rows []*rowChange, deleteChange bool) (map[string]*validateFailedRow, error) {
	res := make(map[string]*validateFailedRow)
	for start := 0; start < len(rows); start += vw.batchSize {
		end := start + vw.batchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[start:end]
		failedRows, err := vw.batchValidateRowChanges(table, batch, deleteChange)
		if err != nil {
			return nil, err
		}
		for k, v := range failedRows {
			res[k] = v
		}
	}
	return res, nil
}

func (vw *validateWorker) getPendingChangesMap() map[string]*tableChange {
	vw.Lock()
	defer vw.Unlock()
	return vw.pendingChangesMap
}

func (vw *validateWorker) getErrorRows() []*validateFailedRow {
	vw.Lock()
	defer vw.Unlock()
	return vw.errorRows
}

func (vw *validateWorker) batchValidateRowChanges(table *validateTableInfo, rows []*rowChange, deleteChange bool) (map[string]*validateFailedRow, error) {
	pkValues := make([][]string, 0, len(rows))
	for _, r := range rows {
		vals := make([]string, 0, len(table.PrimaryKey.Columns))
		for _, col := range table.PrimaryKey.Columns {
			vals = append(vals, genColData(r.Data[col.Offset]))
		}
		pkValues = append(pkValues, vals)
	}
	colCnt := len(rows[0].Data)
	cond := &Cond{
		Table:     table,
		ColumnCnt: colCnt,
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
		vw.L.Warn("fail to validate row changes of table", zap.Error(err))
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

func (vw *validateWorker) validateInsertAndUpdateRows(rows []*rowChange, cond *Cond) (map[string]*validateFailedRow, error) {
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

	tableInfo := cond.Table.Info
	for key, sourceRow := range sourceRows {
		targetRow, ok := targetRows[key]
		if !ok {
			failedRows[key] = &validateFailedRow{tp: rowNotExist}
			continue
		}
		if vw.cfg.Mode == config.ValidationFull {
			// only compare the whole row in full mode
			eq, err := vw.compareData(sourceRow, targetRow, tableInfo.Columns[:cond.ColumnCnt])
			if err != nil {
				return nil, err
			}
			if !eq {
				failedRows[key] = &validateFailedRow{tp: rowDifferent, dstData: targetRow}
			}
		}
	}
	return failedRows, nil
}

// a simplified version of https://github.com/pingcap/tidb-tools/blob/d9fdfa2f9040aab3fab7cd11774a82226f467fe7/sync_diff_inspector/utils/utils.go#L487-L606
func (vw *validateWorker) compareData(sourceData, targetData []*sql.NullString, columns []*model.ColumnInfo) (bool, error) {
	for i, column := range columns {
		data1, data2 := sourceData[i], targetData[i]
		if data1.Valid != data2.Valid {
			return false, nil
		}
		str1, str2 := data1.String, data2.String
		if str1 == str2 {
			continue
		} else if column.FieldType.Tp == tidbmysql.TypeFloat || column.FieldType.Tp == tidbmysql.TypeDouble {
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
		return false, nil
	}

	return true, nil
}

func (vw *validateWorker) getTargetRows(cond *Cond) (map[string][]*sql.NullString, error) {
	tctx := tcontext.NewContext(vw.ctx, vw.L)
	fullTableName := cond.Table.Target.String()
	columnNames := make([]string, 0, cond.ColumnCnt)
	for i := 0; i < cond.ColumnCnt; i++ {
		col := cond.Table.Info.Columns[i]
		columnNames = append(columnNames, dbutil.ColumnName(col.Name.O))
	}
	columns := strings.Join(columnNames, ", ")
	rowsQuery := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ %s FROM %s WHERE %s",
		columns, fullTableName, cond.GetWhere())
	rows, err := vw.conn.QuerySQL(tctx, rowsQuery, cond.GetArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string][]*sql.NullString)
	for rows.Next() {
		rowData, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		pkCols := cond.Table.PrimaryKey.Columns
		pkValues := make([]string, 0, len(pkCols))
		for _, col := range pkCols {
			pkValues = append(pkValues, rowData[col.Offset].String)
		}
		pk := genRowKey(pkValues)
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

func (vw *validateWorker) incrPendingRowCount(tp rowChangeType) {
	vw.pendingRowCounts[tp]++
	vw.validator.addPendingRowCount(tp, 1)
}

func (vw *validateWorker) setPendingRowCounts(newCounts []int64) {
	for tp, val := range newCounts {
		diff := val - vw.pendingRowCounts[tp]
		vw.pendingRowCounts[tp] = val
		vw.validator.addPendingRowCount(rowChangeType(tp), diff)
	}
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

func getSourceRowsForCompare(rows []*rowChange) map[string][]*sql.NullString {
	rowMap := make(map[string][]*sql.NullString, len(rows))
	for _, r := range rows {
		colValues := make([]*sql.NullString, len(r.Data))
		for i := range r.Data {
			var colData string
			if r.Data[i] != nil {
				colData = genColData(r.Data[i])
			}
			colValues[i] = &sql.NullString{
				String: colData,
				Valid:  r.Data[i] != nil,
			}
		}
		rowMap[r.Key] = colValues
	}
	return rowMap
}
