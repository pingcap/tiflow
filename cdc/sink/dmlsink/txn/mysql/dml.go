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

package mysql

import (
	"strings"

	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/quotes"
)

// prepareUpdate builds a parametrics UPDATE statement as following
// sql: `UPDATE `test`.`t` SET {} = ?, {} = ? WHERE {} = ?, {} = {} LIMIT 1`
// `WHERE` conditions come from `preCols` and SET clause targets come from `cols`.
func prepareUpdate(quoteTable string, preCols, cols []*model.Column, forceReplicate bool) (string, []interface{}) {
	var builder strings.Builder
	builder.WriteString("UPDATE " + quoteTable + " SET ")

	columnNames := make([]string, 0, len(cols))
	args := make([]interface{}, 0, len(cols)+len(preCols))
	for _, col := range cols {
		if col == nil || col.Flag.IsGeneratedColumn() {
			continue
		}
		columnNames = append(columnNames, col.Name)
		args = appendQueryArgs(args, col)
	}
	if len(args) == 0 {
		return "", nil
	}
	for i, column := range columnNames {
		if i == len(columnNames)-1 {
			builder.WriteString("`" + quotes.EscapeName(column) + "` = ?")
		} else {
			builder.WriteString("`" + quotes.EscapeName(column) + "` = ?, ")
		}
	}

	builder.WriteString(" WHERE ")
	colNames, wargs := whereSlice(preCols, forceReplicate)
	if len(wargs) == 0 {
		return "", nil
	}
	for i := 0; i < len(colNames); i++ {
		if i > 0 {
			builder.WriteString(" AND ")
		}
		if wargs[i] == nil {
			builder.WriteString(quotes.QuoteName(colNames[i]) + " IS NULL")
		} else {
			builder.WriteString(quotes.QuoteName(colNames[i]) + " = ?")
			args = append(args, wargs[i])
		}
	}
	builder.WriteString(" LIMIT 1")
	sql := builder.String()
	return sql, args
}

// prepareReplace builds a parametrics REPLACE statement as following
// sql: `REPLACE INTO `test`.`t` VALUES (?,?,?)`
func prepareReplace(
	quoteTable string,
	cols []*model.Column,
	appendPlaceHolder bool,
	translateToInsert bool,
) (string, []interface{}) {
	var builder strings.Builder
	columnNames := make([]string, 0, len(cols))
	args := make([]interface{}, 0, len(cols))
	for _, col := range cols {
		if col == nil || col.Flag.IsGeneratedColumn() {
			continue
		}
		columnNames = append(columnNames, col.Name)
		args = appendQueryArgs(args, col)
	}
	if len(args) == 0 {
		return "", nil
	}

	colList := "(" + buildColumnList(columnNames) + ")"
	if translateToInsert {
		builder.WriteString("INSERT INTO " + quoteTable + " " + colList + " VALUES ")
	} else {
		builder.WriteString("REPLACE INTO " + quoteTable + " " + colList + " VALUES ")
	}
	if appendPlaceHolder {
		builder.WriteString("(" + placeHolder(len(columnNames)) + ")")
	}

	return builder.String(), args
}

// if the column value type is []byte and charset is not binary, we get its string
// representation. Because if we use the byte array respresentation, the go-sql-driver
// will automatically set `_binary` charset for that column, which is not expected.
// See https://github.com/go-sql-driver/mysql/blob/ce134bfc/connection.go#L267
func appendQueryArgs(args []interface{}, col *model.Column) []interface{} {
	if col.Charset != "" && col.Charset != charset.CharsetBin {
		colValBytes, ok := col.Value.([]byte)
		if ok {
			args = append(args, string(colValBytes))
		} else {
			args = append(args, col.Value)
		}
	} else {
		args = append(args, col.Value)
	}

	return args
}

// prepareDelete builds a parametric DELETE statement as following
// sql: `DELETE FROM `test`.`t` WHERE x = ? AND y >= ? LIMIT 1`
func prepareDelete(quoteTable string, cols []*model.Column, forceReplicate bool) (string, []interface{}) {
	var builder strings.Builder
	builder.WriteString("DELETE FROM " + quoteTable + " WHERE ")

	colNames, wargs := whereSlice(cols, forceReplicate)
	if len(wargs) == 0 {
		return "", nil
	}
	args := make([]interface{}, 0, len(wargs))
	for i := 0; i < len(colNames); i++ {
		if i > 0 {
			builder.WriteString(" AND ")
		}
		if wargs[i] == nil {
			builder.WriteString(quotes.QuoteName(colNames[i]) + " IS NULL")
		} else {
			builder.WriteString(quotes.QuoteName(colNames[i]) + " = ?")
			args = append(args, wargs[i])
		}
	}
	builder.WriteString(" LIMIT 1")
	sql := builder.String()
	return sql, args
}

// whereSlice builds a parametric WHERE clause as following
// sql: `WHERE {} = ? AND {} > ?`
func whereSlice(cols []*model.Column, forceReplicate bool) (colNames []string, args []interface{}) {
	// Try to use unique key values when available
	for _, col := range cols {
		if col == nil || !col.Flag.IsHandleKey() {
			continue
		}
		colNames = append(colNames, col.Name)
		args = appendQueryArgs(args, col)
	}
	// if no explicit row id but force replicate, use all key-values in where condition
	if len(colNames) == 0 && forceReplicate {
		colNames = make([]string, 0, len(cols))
		args = make([]interface{}, 0, len(cols))
		for _, col := range cols {
			colNames = append(colNames, col.Name)
			args = appendQueryArgs(args, col)
		}
	}
	return
}

func buildColumnList(names []string) string {
	var b strings.Builder
	for i, name := range names {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(quotes.QuoteName(name))

	}

	return b.String()
}

// placeHolder returns a string separated by comma
// n must be greater or equal than 1, or the function will panic
func placeHolder(n int) string {
	var builder strings.Builder
	builder.Grow((n-1)*2 + 1)
	for i := 0; i < n; i++ {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("?")
	}
	return builder.String()
}
