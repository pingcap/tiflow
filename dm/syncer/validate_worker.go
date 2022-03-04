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
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/parser/model"
	tidbmysql "github.com/pingcap/tidb/parser/mysql"
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

	maxBatchSize           = 100 // todo: choose a proper value, or configurable?
	failedRowRetryDuration = 1 * time.Hour
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
	cfg               config.ValidatorConfig
	ctx               context.Context
	interval          time.Duration
	validator         *DataValidator
	L                 log.Logger
	conn              *dbconn.DBConn
	rowChangeCh       chan *rowChange
	pendingChangesMap map[string]*tableChange
	pendingRowCount   atomic.Int64
	batchSize         int
	maxFailedCount    int
	errorRows         []*validateFailedRow
	sync.Mutex
}

func newValidateWorker(v *DataValidator, id int) *validateWorker {
	workerLog := v.L.WithFields(zap.Int("id", id))
	return &validateWorker{
		cfg:               v.cfg.ValidatorCfg,
		ctx:               v.ctx,
		interval:          v.validateInterval,
		validator:         v,
		L:                 workerLog,
		conn:              v.toDBConns[id],
		rowChangeCh:       make(chan *rowChange, workerChannelSize),
		pendingChangesMap: make(map[string]*tableChange),
		batchSize:         maxBatchSize,
		// =600 if using default validate interval=10s
		maxFailedCount: int(failedRowRetryDuration / v.validateInterval),
	}
}

func (vw *validateWorker) run() {
outer:
	for {
		select {
		case change, ok := <-vw.rowChangeCh:
			if !ok {
				break outer
			}
			if change.Tp == flushCheckpoint {
				change.wg.Done()
				break
			}
			// todo: limit number of pending rows.
			// todo: trigger validation when pending rows count reaches a threshold, but we don't want trigger it too often
			// todo: since it may not reduce the number of pending row changes as the downstream may have changed again,
			// todo: we need to catchup with the progress of syncer first, so row changes which are changed again can be merged.
			vw.updateRowChange(change)
		case <-time.After(vw.interval):
			err := vw.validateTableChange()
			if err != nil {
				// todo: better error handling
				vw.validator.errChan <- terror.Annotate(err, "failed to validate table change")
			}
			// todo: if a row failed too many times, add it to failed changes
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
		val.LastMeetTS = row.LastMeetTS
		val.FailedCnt = 0 // clear failed count
	} else {
		change.rows[row.Key] = row
		vw.pendingRowCount.Inc()
	}
}

func (vw *validateWorker) validateTableChange() error {
	vw.Lock()
	defer vw.Unlock()

	failedChanges := make(map[string]*tableChange)
	allErrorRows := make([]*validateFailedRow, 0)
	var failedRowCnt int64
	for k, tblChange := range vw.pendingChangesMap {
		var insertUpdateChanges, deleteChanges []*rowChange
		for _, r := range tblChange.rows {
			if r.Tp == rowDeleted {
				deleteChanges = append(deleteChanges, r)
			} else {
				insertUpdateChanges = append(insertUpdateChanges, r)
			}
		}
		allFailedRows := make(map[string]*rowChange)
		validateFunc := func(rows []*rowChange, isDelete bool) error {
			if len(rows) == 0 {
				return nil
			}
			failedRows, err := vw.validateRowChanges(tblChange.table, rows, false)
			if err != nil {
				return err
			}
			// todo: if row failed count > threshold, should mark it as data inconsistent error
			for pk, val := range failedRows {
				r := tblChange.rows[pk]
				r.FailedCnt++
				if r.FailedCnt >= vw.maxFailedCount {
					val.srcRow = r
					allErrorRows = append(allErrorRows, val)
				} else {
					allFailedRows[pk] = r
				}
			}
			return nil
		}
		if err := validateFunc(insertUpdateChanges, false); err != nil {
			return err
		}
		if err := validateFunc(deleteChanges, true); err != nil {
			return err
		}
		if len(allFailedRows) > 0 {
			failedChanges[k] = &tableChange{
				table: tblChange.table,
				rows:  allFailedRows,
			}
			failedRowCnt += int64(len(allFailedRows))
		}
	}
	vw.L.Debug("pending row count", zap.Int64("before", vw.pendingRowCount.Load()), zap.Int64("after", failedRowCnt))
	vw.pendingRowCount.Store(failedRowCnt)
	vw.pendingChangesMap = failedChanges
	vw.errorRows = append(vw.errorRows, allErrorRows...)
	return nil
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
		vals := make([]string, 0, len(table.pkIndices))
		for _, idx := range table.pkIndices {
			vals = append(vals, string(genColData(r.Data[idx])))
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

		eq, err := vw.compareData(sourceRow, targetRow, tableInfo.Columns[:cond.ColumnCnt])
		if err != nil {
			return nil, err
		}
		if !eq {
			failedRows[key] = &validateFailedRow{tp: rowDifferent, dstData: targetRow}
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
	tctx := tcontext.NewContext(vw.ctx, log.L())
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
		pkValues := make([]string, 0, len(cond.Table.pkIndices))
		for _, idx := range cond.Table.pkIndices {
			pkValues = append(pkValues, rowData[idx].String)
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
