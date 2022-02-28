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
	for {
		select {
		case change := <-vw.rowChangeCh:
			// todo: limit number of pending rows
			vw.updateRowChange(change)
			vw.receivedRowCount.Inc()
		case <-vw.ctx.Done():
			return
		case <-time.After(vw.interval):
			err := vw.validateTableChange()
			if err != nil {
				// todo: better error handling
				vw.validator.errChan <- terror.Annotate(err, "failed to validate table change")
				return
			}
			vw.validationCount.Inc()
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
		val.theType = row.theType
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
			if r.theType == rowDeleted {
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

func (vw *validateWorker) compareData(sourceData, targetData []*dbutil.ColumnData, columns []*model.ColumnInfo) (bool, error) {
	for i, column := range columns {
		data1, data2 := sourceData[i], targetData[i]
		if data1.IsNull != data2.IsNull {
			return false, nil
		}
		str1, str2 := string(data1.Data), string(data2.Data)
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

func (vw *validateWorker) getTargetRows(cond *Cond) (map[string][]*dbutil.ColumnData, error) {
	tctx := tcontext.NewContext(vw.ctx, log.L())
	fullTableName := cond.Table.Target.String()
	pkColumnNames := make([]string, 0, len(cond.Table.pkIndices))
	for i := range cond.Table.pkIndices {
		pkColumnNames = append(pkColumnNames, cond.Table.Info.Columns[i].Name.O)
	}
	columnNames := make([]string, 0, cond.ColumnCnt)
	for i := 0; i < cond.ColumnCnt; i++ {
		col := cond.Table.Info.Columns[i]
		columnNames = append(columnNames, dbutil.ColumnName(col.Name.O))
	}
	columns := strings.Join(columnNames, ", ")
	rowsQuery := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ %s FROM %s WHERE %s ORDER BY %s",
		columns, fullTableName, cond.GetWhere(), strings.Join(pkColumnNames, ","))
	rows, err := vw.conn.QuerySQL(tctx, rowsQuery, cond.GetArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string][]*dbutil.ColumnData)
	for rows.Next() {
		rowData, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		pkVals := make([]string, 0, len(cond.Table.pkIndices))
		for _, idx := range cond.Table.pkIndices {
			colVal := genColData(rowData[idx].Data)
			pkVals = append(pkVals, string(colVal))
		}
		pk := genRowKey(pkVals)
		result[pk] = rowData
	}
	return result, rows.Err()
}

func scanRow(rows *sql.Rows) ([]*dbutil.ColumnData, error) {
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

	result := make([]*dbutil.ColumnData, len(cols))
	for i := range colVals {
		result[i] = &dbutil.ColumnData{
			Data:   colVals[i],
			IsNull: colVals[i] == nil,
		}
	}

	return result, nil
}

func getSourceRowsForCompare(rows []*rowChange) map[string][]*dbutil.ColumnData {
	rowMap := make(map[string][]*dbutil.ColumnData, len(rows))
	for _, r := range rows {
		colValues := make([]*dbutil.ColumnData, len(r.data))
		for i := range r.data {
			var colData []byte
			if r.data[i] != nil {
				colData = genColData(r.data[i])
			}
			colValues[i] = &dbutil.ColumnData{
				Data:   colData,
				IsNull: r.data[i] == nil,
			}
		}
		rowMap[r.key] = colValues
	}
	return rowMap
}
