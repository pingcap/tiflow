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
)

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
	sync.Mutex

	// used for test
	receivedRowCount atomic.Int64 // number of rowChange received from channel
	validationCount  atomic.Int64 // number of successful validation
}

func newValidateWorker(v *DataValidator, id int) *validateWorker {
	return &validateWorker{
		cfg:               v.cfg.ValidatorCfg,
		ctx:               v.ctx,
		interval:          v.validateInterval,
		validator:         v,
		L:                 v.L,
		conn:              v.toDBConns[id],
		rowChangeCh:       make(chan *rowChange, workerChannelSize),
		pendingChangesMap: make(map[string]*tableChange),
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
			// todo: limit number of pending rows.
			// todo: trigger validation when pending rows count reaches a threshold, but we don't want trigger it too often
			// todo: since it may not reduce the number of pending row changes as the downstream may have changed again,
			// todo: we need to catchup with the progress of syncer first, so row changes which are changed again can be merged.
			vw.updateRowChange(change)
			vw.receivedRowCount.Inc()
		case <-time.After(vw.interval):
			err := vw.validateTableChange()
			if err != nil {
				// todo: better error handling
				vw.validator.errChan <- terror.Annotate(err, "failed to validate table change")
			}
			vw.validationCount.Inc()
			// todo: if a row failed too many times, add it to failed changes
		}
	}
}

func (vw *validateWorker) updateRowChange(row *rowChange) {
	// cluster using target table
	fullTableName := row.table.Target.String()
	change := vw.pendingChangesMap[fullTableName]
	if change == nil {
		// no change of this table
		change = &tableChange{
			table: row.table,
			rows:  make(map[string]*rowChange),
		}
		vw.pendingChangesMap[fullTableName] = change
	}
	if val, ok := change.rows[row.key]; ok {
		val.data = row.data
		val.tp = row.tp
		val.lastMeetTS = row.lastMeetTS
		val.failedCnt = 0 // clear failed count
	} else {
		change.rows[row.key] = row
		vw.pendingRowCount.Inc()
	}
}

func (vw *validateWorker) validateTableChange() error {
	failedChanges := make(map[string]*tableChange)
	var failedRowCnt int64
	for k, val := range vw.pendingChangesMap {
		var insertUpdateChanges, deleteChanges []*rowChange
		for _, r := range val.rows {
			if r.tp == rowDeleted {
				deleteChanges = append(deleteChanges, r)
			} else {
				insertUpdateChanges = append(insertUpdateChanges, r)
			}
		}
		rows := make(map[string]*rowChange)
		if len(insertUpdateChanges) > 0 {
			// todo: limit number of validated rows
			failedRows, err := vw.validateRowChanges(val.table, insertUpdateChanges, false)
			if err != nil {
				return err
			}
			for _, pk := range failedRows {
				rows[pk] = val.rows[pk]
				rows[pk].failedCnt++
			}
		}
		if len(deleteChanges) > 0 {
			// todo: limit number of validated rows
			failedRows, err := vw.validateRowChanges(val.table, deleteChanges, true)
			if err != nil {
				return err
			}
			for _, pk := range failedRows {
				rows[pk] = val.rows[pk]
				rows[pk].failedCnt++
			}
		}
		if len(rows) > 0 {
			failedChanges[k] = &tableChange{
				table: val.table,
				rows:  rows,
			}
			failedRowCnt += int64(len(rows))
		}
	}
	vw.pendingRowCount.Store(failedRowCnt)
	vw.pendingChangesMap = failedChanges
	return nil
}

func (vw *validateWorker) validateRowChanges(table *validateTableInfo, rows []*rowChange, deleteChange bool) ([]string, error) {
	pkValues := make([][]string, 0, len(rows))
	for _, r := range rows {
		pkValues = append(pkValues, r.pkValues)
	}
	colCnt := len(rows[0].data)
	cond := &Cond{
		Table:     table,
		ColumnCnt: colCnt,
		PkValues:  pkValues,
	}
	var failedRows []string
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

func (vw *validateWorker) validateDeletedRows(cond *Cond) ([]string, error) {
	targetRows, err := vw.getTargetRows(cond)
	if err != nil {
		return []string{}, err
	}

	failedRows := make([]string, 0, len(targetRows))
	for key := range targetRows {
		failedRows = append(failedRows, key)
	}
	return failedRows, nil
}

func (vw *validateWorker) validateInsertAndUpdateRows(rows []*rowChange, cond *Cond) ([]string, error) {
	var failedRows []string
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
			failedRows = append(failedRows, key)
			continue
		}

		eq, err := vw.compareData(sourceRow, targetRow, tableInfo.Columns[:cond.ColumnCnt])
		if err != nil {
			return nil, err
		}
		if !eq {
			failedRows = append(failedRows, key)
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
		colValues := make([]*sql.NullString, len(r.data))
		for i := range r.data {
			var colData string
			if r.data[i] != nil {
				colData = genColData(r.data[i])
			}
			colValues[i] = &sql.NullString{
				String: colData,
				Valid:  r.data[i] != nil,
			}
		}
		rowMap[r.key] = colValues
	}
	return rowMap
}
