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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/parser/model"
	tidbmysql "github.com/pingcap/tidb/parser/mysql"
	"go.uber.org/zap"

	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
)

type RowDataIterator interface {
	// Next seeks the next row data, it used when compared rows.
	Next() (map[string]*dbutil.ColumnData, error)
	// Close release the resource.
	Close()
}

type RowDataIteratorImpl struct {
	Rows *sql.Rows
}

func (r *RowDataIteratorImpl) Next() (map[string]*dbutil.ColumnData, error) {
	for r.Rows.Next() {
		rowData, err := dbutil.ScanRow(r.Rows)
		return rowData, err
	}
	return nil, nil
}

func (r *RowDataIteratorImpl) Close() {
	r.Rows.Close()
}

type RowChangeIteratorImpl struct {
	Rows []map[string]*dbutil.ColumnData
	Idx  int
}

func (b *RowChangeIteratorImpl) Next() (map[string]*dbutil.ColumnData, error) {
	if b.Idx >= len(b.Rows) {
		return nil, nil
	}
	row := b.Rows[b.Idx]
	b.Idx++
	return row, nil
}

func (b *RowChangeIteratorImpl) Close() {
	// skip: nothing to do
}

type validateWorker struct {
	ctx               context.Context
	interval          time.Duration
	validator         *DataValidator
	L                 log.Logger
	conn              *dbconn.DBConn
	rowChangeCh       chan *rowChange
	pendingChangesMap map[string]*tableChange
	rowCount          int64
}

func (vw *validateWorker) run() {
	for {
		select {
		case change := <-vw.rowChangeCh:
			// todo: limit number of pending rows
			vw.updateRowChange(change)
		case <-vw.ctx.Done():
			return
		case <-time.After(vw.interval):
			err := vw.validateTableChange()
			if err != nil {
				vw.validator.errChan <- terror.Annotate(err, "failed to validate table change")
				return
			}
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
	key := strings.Join(row.pk, "-")
	if val, ok := change.rows[key]; ok {
		val.data = row.data
		val.theType = row.theType
		val.lastMeetTs = row.lastMeetTs
		val.retryCnt = row.retryCnt
	} else {
		change.rows[key] = row
		vw.rowCount++
	}
}

func (vw *validateWorker) validateTableChange() error {
	failedChanges := make(map[string]*tableChange)
	for k, val := range vw.pendingChangesMap {
		var insertUpdateChanges, deleteChanges []*rowChange
		for _, r := range val.rows {
			if r.theType == rowDeleted {
				deleteChanges = append(deleteChanges, r)
			} else {
				insertUpdateChanges = append(insertUpdateChanges, r)
			}
		}
		rows := make(map[string]*rowChange, 0)
		if len(insertUpdateChanges) > 0 {
			failedRows, err := vw.validateRowChanges(val.table, insertUpdateChanges, false)
			if err != nil {
				return err
			}
			for _, pk := range failedRows {
				rows[pk] = val.rows[pk]
			}
		}
		if len(deleteChanges) > 0 {
			failedRows, err := vw.validateRowChanges(val.table, deleteChanges, true)
			if err != nil {
				return err
			}
			for _, pk := range failedRows {
				rows[pk] = val.rows[pk]
			}
		}
		if len(rows) > 0 {
			failedChanges[k] = &tableChange{
				table: val.table,
				rows:  rows,
			}
		}
	}
	vw.pendingChangesMap = failedChanges
	return nil
}

func (vw *validateWorker) validateRowChanges(table *validateTableInfo, rows []*rowChange, deleteChange bool) ([]string, error) {
	pkValues := make([][]string, 0, len(rows))
	for _, r := range rows {
		pkValues = append(pkValues, r.pk)
	}
	cond := &Cond{Table: table, PkValues: pkValues}
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
	downstreamRowsIterator, err := vw.getRowsFrom(cond)
	if err != nil {
		return []string{}, err
	}
	defer downstreamRowsIterator.Close()

	var failedRows []string
	for {
		data, err := downstreamRowsIterator.Next()
		if err != nil {
			return nil, err
		}
		if data == nil {
			break
		}
		failedRows = append(failedRows, getPKValues(data, cond))
	}
	return failedRows, nil
}

func (vw *validateWorker) validateInsertAndUpdateRows(rows []*rowChange, cond *Cond) ([]string, error) {
	var failedRows []string
	var upstreamRowsIterator, downstreamRowsIterator RowDataIterator
	var err error
	upstreamRowsIterator, err = getRowChangeIterator(cond.Table, rows)
	if err != nil {
		return nil, err
	}
	defer upstreamRowsIterator.Close()
	downstreamRowsIterator, err = vw.getRowsFrom(cond)
	if err != nil {
		return nil, err
	}
	defer downstreamRowsIterator.Close()

	var lastUpstreamData, lastDownstreamData map[string]*dbutil.ColumnData

	tableInfo := cond.Table.Info
	_, orderKeyCols := dbutil.SelectUniqueOrderKey(tableInfo)
	for {
		if lastUpstreamData == nil {
			lastUpstreamData, err = upstreamRowsIterator.Next()
			if err != nil {
				return nil, err
			}
		}

		if lastDownstreamData == nil {
			lastDownstreamData, err = downstreamRowsIterator.Next()
			if err != nil {
				return nil, err
			}
		}

		// may have deleted on upstream and haven't synced to downstream,
		// we mark this as success as we'll check the delete-event later
		// or downstream removed the pk and added more data by other clients, skip it.
		if lastUpstreamData == nil && lastDownstreamData != nil {
			vw.L.Debug("more data on downstream, may come from other client, skip it")
			break
		}

		if lastDownstreamData == nil {
			// target lack some data, should insert the last source datas
			for lastUpstreamData != nil {
				failedRows = append(failedRows, getPKValues(lastUpstreamData, cond))

				lastUpstreamData, err = upstreamRowsIterator.Next()
				if err != nil {
					return nil, err
				}
			}
			break
		}

		eq, cmp, err := vw.compareData(lastUpstreamData, lastDownstreamData, orderKeyCols, tableInfo.Columns)
		if err != nil {
			return nil, err
		}
		if eq {
			lastDownstreamData = nil
			lastUpstreamData = nil
			continue
		}

		switch cmp {
		case 1:
			// may have deleted on upstream and haven't synced to downstream,
			// we mark this as success as we'll check the delete-event later
			// or downstream removed the pk and added more data by other clients, skip it.
			vw.L.Debug("more data on downstream, may come from other client, skip it", zap.Reflect("data", lastDownstreamData))
			lastDownstreamData = nil
		case -1:
			failedRows = append(failedRows, getPKValues(lastUpstreamData, cond))
			lastUpstreamData = nil
		case 0:
			failedRows = append(failedRows, getPKValues(lastUpstreamData, cond))
			lastUpstreamData = nil
			lastDownstreamData = nil
		}
	}
	return failedRows, nil
}

func (vw *validateWorker) compareData(map1, map2 map[string]*dbutil.ColumnData, orderKeyCols, columns []*model.ColumnInfo) (equal bool, cmp int32, err error) {
	var (
		data1, data2 *dbutil.ColumnData
		str1, str2   string
		key          string
		ok           bool
	)

	equal = true

	defer func() {
		if equal || err != nil {
			return
		}

		if cmp == 0 {
			vw.L.Warn("find different row", zap.String("column", key), zap.String("row1", rowToString(map1)), zap.String("row2", rowToString(map2)))
		} else if cmp > 0 {
			vw.L.Warn("target had superfluous data", zap.String("row", rowToString(map2)))
		} else {
			vw.L.Warn("target lack data", zap.String("row", rowToString(map1)))
		}
	}()

	for _, column := range columns {
		if data1, ok = map1[column.Name.O]; !ok {
			return false, 0, errors.Errorf("upstream doesn't have key %s", key)
		}
		if data2, ok = map2[column.Name.O]; !ok {
			return false, 0, errors.Errorf("downstream doesn't have key %s", key)
		}
		str1 = string(data1.Data)
		str2 = string(data2.Data)
		if column.FieldType.Tp == tidbmysql.TypeFloat || column.FieldType.Tp == tidbmysql.TypeDouble {
			if data1.IsNull == data2.IsNull && data1.IsNull {
				continue
			}

			num1, err1 := strconv.ParseFloat(str1, 64)
			num2, err2 := strconv.ParseFloat(str2, 64)
			if err1 != nil || err2 != nil {
				err = errors.Errorf("convert %s, %s to float failed, err1: %v, err2: %v", str1, str2, err1, err2)
				return
			}
			if math.Abs(num1-num2) <= 1e-6 {
				continue
			}
		} else {
			if (str1 == str2) && (data1.IsNull == data2.IsNull) {
				continue
			}
		}

		equal = false
		break

	}
	if equal {
		return
	}

	// Not Equal. Compare orderkeycolumns.
	for _, col := range orderKeyCols {
		if data1, ok = map1[col.Name.O]; !ok {
			err = errors.Errorf("upstream doesn't have key %s", col.Name.O)
			return
		}
		if data2, ok = map2[col.Name.O]; !ok {
			err = errors.Errorf("downstream doesn't have key %s", col.Name.O)
			return
		}

		if NeedQuotes(col.FieldType.Tp) {
			strData1 := string(data1.Data)
			strData2 := string(data2.Data)

			if len(strData1) == len(strData2) && strData1 == strData2 {
				continue
			}

			if strData1 < strData2 {
				cmp = -1
			} else {
				cmp = 1
			}
			break
		} else if data1.IsNull || data2.IsNull {
			if data1.IsNull && data2.IsNull {
				continue
			}

			if data1.IsNull {
				cmp = -1
			} else {
				cmp = 1
			}
			break
		} else {
			num1, err1 := strconv.ParseFloat(string(data1.Data), 64)
			num2, err2 := strconv.ParseFloat(string(data2.Data), 64)
			if err1 != nil || err2 != nil {
				err = errors.Errorf("convert %s, %s to float failed, err1: %v, err2: %v", string(data1.Data), string(data2.Data), err1, err2)
				return
			}

			if num1 == num2 {
				continue
			}

			if num1 < num2 {
				cmp = -1
			} else {
				cmp = 1
			}
			break
		}
	}

	return
}

func (vw *validateWorker) getRowsFrom(cond *Cond) (RowDataIterator, error) {
	tctx := tcontext.NewContext(context.Background(), log.L())
	fullTableName := cond.Table.Target.String()
	orderKeys, _ := dbutil.SelectUniqueOrderKey(cond.Table.Info)
	columnNames := make([]string, 0, len(cond.Table.Info.Columns))
	for _, col := range cond.Table.ColumnMap {
		columnNames = append(columnNames, dbutil.ColumnName(col.Name.O))
	}
	columns := strings.Join(columnNames, ", ")
	rowsQuery := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ %s FROM %s WHERE %s ORDER BY %s",
		columns, fullTableName, cond.GetWhere(), strings.Join(orderKeys, ","))
	rows, err := vw.conn.QuerySQL(tctx, rowsQuery, cond.GetArgs()...)
	if err != nil {
		return nil, err
	}
	newRowDataIter := &RowDataIteratorImpl{
		Rows: rows,
	}
	return newRowDataIter, nil
}

func getRowChangeIterator(table *validateTableInfo, rows []*rowChange) (RowDataIterator, error) {
	sort.Slice(rows, func(i, j int) bool {
		left, right := rows[i], rows[j]
		for idx := range left.pk {
			if left.pk[idx] != right.pk[idx] {
				return left.pk[idx] < right.pk[idx]
			}
		}
		return false
	})
	it := &RowChangeIteratorImpl{}
	for _, r := range rows {
		colMap := make(map[string]*dbutil.ColumnData)
		for _, c := range table.Info.Columns {
			var colData []byte
			if r.data[c.Offset] != nil {
				colData = []byte(fmt.Sprintf("%v", r.data[c.Offset]))
			}
			colMap[c.Name.O] = &dbutil.ColumnData{
				Data:   colData,
				IsNull: r.data[c.Offset] == nil,
			}
		}
		it.Rows = append(it.Rows, colMap)
	}
	return it, nil
}

func rowToString(row map[string]*dbutil.ColumnData) string {
	var s strings.Builder
	s.WriteString("{ ")
	for key, val := range row {
		if val.IsNull {
			s.WriteString(fmt.Sprintf("%s: IsNull, ", key))
		} else {
			s.WriteString(fmt.Sprintf("%s: %s, ", key, val.Data))
		}
	}
	s.WriteString(" }")

	return s.String()
}

func getPKValues(data map[string]*dbutil.ColumnData, cond *Cond) string {
	var pkValues []string
	for _, pkColumn := range cond.Table.PrimaryKey.Columns {
		// TODO primary key cannot be null, if we uses unique key should make sure all columns are not null
		pkValues = append(pkValues, string(data[pkColumn.Name.O].Data))
	}
	return strings.Join(pkValues, "-")
}

func NeedQuotes(tp byte) bool {
	return !(dbutil.IsNumberType(tp) || dbutil.IsFloatType(tp))
}
