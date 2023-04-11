// Copyright 2019 PingCAP, Inc.
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
	"encoding/binary"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/util/filter"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"golang.org/x/text/encoding/charmap"
)

// genDMLParam stores original data and table structure.
type genDMLParam struct {
	sourceTable     *filter.Table // origin table
	targetTable     *filter.Table
	safeMode        bool             // only used in update
	originalData    [][]interface{}  // all data
	sourceTableInfo *model.TableInfo // all table info
	extendData      [][]interface{}  // all data include extend data
}

// latin1Decider is not usually ISO8859_1 in MySQL.
// ref https://dev.mysql.com/doc/refman/8.0/en/charset-we-sets.html
var latin1Decoder = charmap.Windows1252.NewDecoder()

// adjustValueFromBinlogData adjust the values obtained from go-mysql so that
// - the values can be correctly converted to TiDB datum
// - the values are in the correct type that go-sql-driver/mysql uses.
func adjustValueFromBinlogData(
	data []interface{},
	sourceTI *model.TableInfo,
) ([]interface{}, error) {
	value := make([]interface{}, 0, len(data))
	var err error

	columns := make([]*model.ColumnInfo, 0, len(sourceTI.Columns))
	for _, col := range sourceTI.Columns {
		if !col.Hidden {
			columns = append(columns, col)
		}
	}
	if len(data) != len(columns) {
		return nil, terror.ErrSyncerUnitDMLColumnNotMatch.Generate(len(columns), len(data))
	}

	for i, d := range data {
		d = castUnsigned(d, &columns[i].FieldType)
		isLatin1 := columns[i].GetCharset() == charset.CharsetLatin1 || columns[i].GetCharset() == "" && sourceTI.Charset == charset.CharsetLatin1

		switch v := d.(type) {
		case int8:
			d = int64(v)
		case int16:
			d = int64(v)
		case int32:
			d = int64(v)
		case uint8:
			d = uint64(v)
		case uint16:
			d = uint64(v)
		case uint32:
			d = uint64(v)
		case uint:
			d = uint64(v)
		case decimal.Decimal:
			d = v.String()
		case []byte:
			if isLatin1 {
				d, err = latin1Decoder.Bytes(v)
				// replicate wrong data and don't break task
				if err != nil {
					log.L().DPanic("can't convert latin1 to utf8", zap.ByteString("value", v), zap.Error(err))
				}
			}
		case string:
			isGBK := columns[i].GetCharset() == charset.CharsetGBK || columns[i].GetCharset() == "" && sourceTI.Charset == charset.CharsetGBK
			switch {
			case isGBK:
				// convert string to []byte so that go-sql-driver/mysql can use _binary'value' for DML
				d = []byte(v)
			case isLatin1:
				// TiDB has bug in latin1 so we must convert it to utf8 at DM's scope
				// https://github.com/pingcap/tidb/issues/18955
				d, err = latin1Decoder.String(v)
				// replicate wrong data and don't break task
				if err != nil {
					log.L().DPanic("can't convert latin1 to utf8", zap.String("value", v), zap.Error(err))
				}
			}
		}
		value = append(value, d)
	}
	return value, nil
}

// nolint:dupl
func (s *Syncer) genAndFilterInsertDMLs(tctx *tcontext.Context, param *genDMLParam, filterExprs []expression.Expression) ([]*sqlmodel.RowChange, error) {
	var (
		tableID         = utils.GenTableID(param.targetTable)
		originalDataSeq = param.originalData
		ti              = param.sourceTableInfo
		extendData      = param.extendData
		dmls            = make([]*sqlmodel.RowChange, 0, len(originalDataSeq))
	)

	// if downstream pk/uk(not null) exits, then use downstream pk/uk(not null)
	downstreamTableInfo, err := s.schemaTracker.GetDownStreamTableInfo(tctx, tableID, ti)
	if err != nil {
		return nil, err
	}

	if extendData != nil {
		originalDataSeq = extendData
	}

RowLoop:
	for _, data := range originalDataSeq {
		originalValue, err := adjustValueFromBinlogData(data, ti)
		if err != nil {
			return nil, err
		}

		for _, expr := range filterExprs {
			skip, err := SkipDMLByExpression(s.sessCtx, originalValue, expr, ti.Columns)
			if err != nil {
				return nil, err
			}
			if skip {
				s.filteredInsert.Add(1)
				continue RowLoop
			}
		}

		rowChange := sqlmodel.NewRowChange(
			&cdcmodel.TableName{Schema: param.sourceTable.Schema, Table: param.sourceTable.Name},
			&cdcmodel.TableName{Schema: param.targetTable.Schema, Table: param.targetTable.Name},
			nil,
			originalValue,
			param.sourceTableInfo,
			downstreamTableInfo.TableInfo,
			s.sessCtx,
		)
		rowChange.SetWhereHandle(downstreamTableInfo.WhereHandle)
		dmls = append(dmls, rowChange)
	}

	return dmls, nil
}

// nolint:dupl
func (s *Syncer) genAndFilterUpdateDMLs(
	tctx *tcontext.Context,
	param *genDMLParam,
	oldValueFilters []expression.Expression,
	newValueFilters []expression.Expression,
) ([]*sqlmodel.RowChange, error) {
	var (
		tableID      = utils.GenTableID(param.targetTable)
		originalData = param.originalData
		ti           = param.sourceTableInfo
		extendData   = param.extendData
		dmls         = make([]*sqlmodel.RowChange, 0, len(originalData)/2)
	)

	// if downstream pk/uk(not null) exits, then use downstream pk/uk(not null)
	downstreamTableInfo, err := s.schemaTracker.GetDownStreamTableInfo(tctx, tableID, ti)
	if err != nil {
		return nil, err
	}

	if extendData != nil {
		originalData = extendData
	}

RowLoop:
	for i := 0; i < len(originalData); i += 2 {
		oriOldData := originalData[i]
		oriChangedData := originalData[i+1]

		if len(oriOldData) != len(oriChangedData) {
			return nil, terror.ErrSyncerUnitDMLOldNewValueMismatch.Generate(len(oriOldData), len(oriChangedData))
		}

		oriOldValues, err := adjustValueFromBinlogData(oriOldData, ti)
		if err != nil {
			return nil, err
		}
		oriChangedValues, err := adjustValueFromBinlogData(oriChangedData, ti)
		if err != nil {
			return nil, err
		}

		for j := range oldValueFilters {
			// AND logic
			oldExpr, newExpr := oldValueFilters[j], newValueFilters[j]
			skip1, err := SkipDMLByExpression(s.sessCtx, oriOldValues, oldExpr, ti.Columns)
			if err != nil {
				return nil, err
			}
			skip2, err := SkipDMLByExpression(s.sessCtx, oriChangedValues, newExpr, ti.Columns)
			if err != nil {
				return nil, err
			}
			if skip1 && skip2 {
				s.filteredUpdate.Add(1)
				// TODO: we skip generating the UPDATE SQL, so we left the old value here. Is this expected?
				continue RowLoop
			}
		}

		rowChange := sqlmodel.NewRowChange(
			&cdcmodel.TableName{Schema: param.sourceTable.Schema, Table: param.sourceTable.Name},
			&cdcmodel.TableName{Schema: param.targetTable.Schema, Table: param.targetTable.Name},
			oriOldValues,
			oriChangedValues,
			param.sourceTableInfo,
			downstreamTableInfo.TableInfo,
			s.sessCtx,
		)
		rowChange.SetWhereHandle(downstreamTableInfo.WhereHandle)
		dmls = append(dmls, rowChange)
	}

	return dmls, nil
}

// nolint:dupl
func (s *Syncer) genAndFilterDeleteDMLs(tctx *tcontext.Context, param *genDMLParam, filterExprs []expression.Expression) ([]*sqlmodel.RowChange, error) {
	var (
		tableID    = utils.GenTableID(param.targetTable)
		dataSeq    = param.originalData
		ti         = param.sourceTableInfo
		extendData = param.extendData
		dmls       = make([]*sqlmodel.RowChange, 0, len(dataSeq))
	)

	// if downstream pk/uk(not null) exits, then use downstream pk/uk(not null)
	downstreamTableInfo, err := s.schemaTracker.GetDownStreamTableInfo(tctx, tableID, ti)
	if err != nil {
		return nil, err
	}

	if extendData != nil {
		dataSeq = extendData
	}

RowLoop:
	for _, data := range dataSeq {
		value, err := adjustValueFromBinlogData(data, ti)
		if err != nil {
			return nil, err
		}

		for _, expr := range filterExprs {
			skip, err := SkipDMLByExpression(s.sessCtx, value, expr, ti.Columns)
			if err != nil {
				return nil, err
			}
			if skip {
				s.filteredDelete.Add(1)
				continue RowLoop
			}
		}

		rowChange := sqlmodel.NewRowChange(
			&cdcmodel.TableName{Schema: param.sourceTable.Schema, Table: param.sourceTable.Name},
			&cdcmodel.TableName{Schema: param.targetTable.Schema, Table: param.targetTable.Name},
			value,
			nil,
			param.sourceTableInfo,
			downstreamTableInfo.TableInfo,
			s.sessCtx,
		)
		rowChange.SetWhereHandle(downstreamTableInfo.WhereHandle)
		dmls = append(dmls, rowChange)
	}

	return dmls, nil
}

func castUnsigned(data interface{}, ft *types.FieldType) interface{} {
	if !mysql.HasUnsignedFlag(ft.GetFlag()) {
		return data
	}

	switch v := data.(type) {
	case int:
		return uint(v)
	case int8:
		return uint8(v)
	case int16:
		return uint16(v)
	case int32:
		if ft.GetType() == mysql.TypeInt24 {
			// we use int32 to store MEDIUMINT, if the value is signed, it's fine
			// but if the value is un-signed, simply convert it use `uint32` may out of the range
			// like -4692783 converted to 4290274513 (2^32 - 4692783), but we expect 12084433 (2^24 - 4692783)
			data := make([]byte, 4)
			binary.LittleEndian.PutUint32(data, uint32(v))
			return uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
		}
		return uint32(v)
	case int64:
		return uint64(v)
	}

	return data
}

// checkLogColumns returns error when not all rows in skipped is empty, which means the binlog doesn't contain all
// columns.
// TODO: don't return error when all skipped columns is non-PK.
func checkLogColumns(skipped [][]int) error {
	for _, row := range skipped {
		if len(row) > 0 {
			return terror.ErrBinlogNotLogColumn
		}
	}
	return nil
}

// genSQLMultipleRows generates multiple rows SQL with different dmlOpType.
func genSQLMultipleRows(op sqlmodel.DMLType, dmls []*sqlmodel.RowChange) (queries string, args []interface{}) {
	if len(dmls) > 1 {
		log.L().Debug("generate DMLs with multiple rows", zap.Stringer("op", op), zap.Stringer("original op", dmls[0].Type()), zap.Int("rows", len(dmls)))
	}
	switch op {
	case sqlmodel.DMLInsert, sqlmodel.DMLReplace, sqlmodel.DMLInsertOnDuplicateUpdate:
		return sqlmodel.GenInsertSQL(op, dmls...)
	case sqlmodel.DMLUpdate:
		return sqlmodel.GenUpdateSQL(dmls...)
	case sqlmodel.DMLDelete:
		return sqlmodel.GenDeleteSQL(dmls...)
	}
	return
}

// genDMLsWithSameCols group and gen dmls by same columns.
// in optimistic shard mode, different upstream tables may have different columns.
// e.g.
// insert into tb(a,b,c) values(1,1,1)
// insert into tb(a,b,d) values(2,2,2)
// we can only combine DMLs with same column names.
// all dmls should have same dmlOpType and same tableName.
func genDMLsWithSameCols(op sqlmodel.DMLType, dmls []*sqlmodel.RowChange) ([]string, [][]interface{}) {
	queries := make([]string, 0, len(dmls))
	args := make([][]interface{}, 0, len(dmls))
	var lastDML *sqlmodel.RowChange
	var query string
	var arg []interface{}
	groupDMLs := make([]*sqlmodel.RowChange, 0, len(dmls))

	// group dmls by same columns
	for i, dml := range dmls {
		if i == 0 {
			lastDML = dml
		}
		if !sqlmodel.SameTypeTargetAndColumns(lastDML, dml) {
			query, arg = genSQLMultipleRows(op, groupDMLs)
			queries = append(queries, query)
			args = append(args, arg)

			groupDMLs = groupDMLs[0:0]
			lastDML = dml
		}
		groupDMLs = append(groupDMLs, dml)
	}
	if len(groupDMLs) > 0 {
		query, arg = genSQLMultipleRows(op, groupDMLs)
		queries = append(queries, query)
		args = append(args, arg)
	}
	return queries, args
}

// genDMLsWithSameTable groups and generates dmls with same table.
// all the dmls should have same dmlOpType.
func genDMLsWithSameTable(op sqlmodel.DMLType, jobs []*job) ([]string, [][]interface{}) {
	queries := make([]string, 0, len(jobs))
	args := make([][]interface{}, 0, len(jobs))
	var lastTable string
	groupDMLs := make([]*sqlmodel.RowChange, 0, len(jobs))

	if op == sqlmodel.DMLUpdate {
		for i, j := range jobs {
			if j.safeMode {
				query, arg := j.dml.GenSQL(sqlmodel.DMLDelete)
				queries = append(queries, query)
				args = append(args, arg)
				query, arg = j.dml.GenSQL(sqlmodel.DMLReplace)
				queries = append(queries, query)
				args = append(args, arg)
				continue
			}

			if i == 0 {
				lastTable = j.dml.TargetTableID()
			}
			if lastTable != j.dml.TargetTableID() {
				query, arg := genDMLsWithSameCols(op, groupDMLs)
				queries = append(queries, query...)
				args = append(args, arg...)

				groupDMLs = groupDMLs[0:0]
				lastTable = j.dml.TargetTableID()
			}
			groupDMLs = append(groupDMLs, j.dml)
		}
		if len(groupDMLs) > 0 {
			query, arg := genDMLsWithSameCols(op, groupDMLs)
			queries = append(queries, query...)
			args = append(args, arg...)
		}
		return queries, args
	}

	// group dmls with same table
	for i, j := range jobs {
		if i == 0 {
			lastTable = j.dml.TargetTableID()
		}
		if lastTable != j.dml.TargetTableID() {
			query, arg := genDMLsWithSameCols(op, groupDMLs)
			queries = append(queries, query...)
			args = append(args, arg...)

			groupDMLs = groupDMLs[0:0]
			lastTable = j.dml.TargetTableID()
		}
		groupDMLs = append(groupDMLs, j.dml)
	}
	if len(groupDMLs) > 0 {
		query, arg := genDMLsWithSameCols(op, groupDMLs)
		queries = append(queries, query...)
		args = append(args, arg...)
	}
	return queries, args
}

// genDMLsWithSameOp groups and generates dmls by dmlOpType.
// TODO: implement a volcano iterator interface for genDMLsWithSameXXX.
func genDMLsWithSameOp(jobs []*job) ([]string, [][]interface{}) {
	queries := make([]string, 0, len(jobs))
	args := make([][]interface{}, 0, len(jobs))
	var lastOp sqlmodel.DMLType
	jobsWithSameOp := make([]*job, 0, len(jobs))

	// group dmls with same dmlOp
	for i, j := range jobs {
		var curOp sqlmodel.DMLType
		switch j.dml.Type() {
		case sqlmodel.RowChangeUpdate:
			// if update statement didn't update identify values and not in safemode, regard it as insert on duplicate.
			if !j.dml.IsIdentityUpdated() && !j.safeMode {
				curOp = sqlmodel.DMLInsertOnDuplicateUpdate
				break
			}

			curOp = sqlmodel.DMLUpdate
		case sqlmodel.RowChangeInsert:
			// if insert with safemode, regard it as replace
			if j.safeMode {
				curOp = sqlmodel.DMLReplace
				break
			}

			curOp = sqlmodel.DMLInsert
		case sqlmodel.RowChangeDelete:
			curOp = sqlmodel.DMLDelete
		}

		if i == 0 {
			lastOp = curOp
		}

		// now there are 5 situations: [insert, replace(insert with safemode), insert on duplicate(update without identify keys), update(update identify keys/update with safemode), delete]
		if lastOp != curOp {
			query, arg := genDMLsWithSameTable(lastOp, jobsWithSameOp)
			queries = append(queries, query...)
			args = append(args, arg...)

			jobsWithSameOp = jobsWithSameOp[0:0]
			lastOp = curOp
		}
		jobsWithSameOp = append(jobsWithSameOp, j)
	}
	if len(jobsWithSameOp) > 0 {
		query, arg := genDMLsWithSameTable(lastOp, jobsWithSameOp)
		queries = append(queries, query...)
		args = append(args, arg...)
	}
	return queries, args
}
