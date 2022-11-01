// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package cloudstorage

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestTableCol(t *testing.T) {
	testCases := []struct {
		name      string
		filedType byte
		flen      int
		decimal   int
		flag      uint
		expected  string
	}{
		{
			name:      "time",
			filedType: mysql.TypeDuration,
			flen:      math.MinInt,
			decimal:   5,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"TIME","ColumnScale":"5"}`,
		},
		{
			name:      "int(5) UNSIGNED",
			filedType: mysql.TypeLong,
			flen:      5,
			decimal:   math.MinInt,
			flag:      mysql.UnsignedFlag,
			expected:  `{"ColumnName":"","ColumnType":"INT UNSIGNED","ColumnPrecision":"5"}`,
		},
		{
			name:      "float(12,3)",
			filedType: mysql.TypeFloat,
			flen:      12,
			decimal:   3,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"FLOAT","ColumnPrecision":"12","ColumnScale":"3"}`,
		},
		{
			name:      "float",
			filedType: mysql.TypeFloat,
			flen:      12,
			decimal:   -1,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"FLOAT","ColumnPrecision":"12"}`,
		},
		{
			name:      "float",
			filedType: mysql.TypeFloat,
			flen:      5,
			decimal:   -1,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"FLOAT","ColumnPrecision":"5"}`,
		},
		{
			name:      "float(7,3)",
			filedType: mysql.TypeFloat,
			flen:      7,
			decimal:   3,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"FLOAT","ColumnPrecision":"7","ColumnScale":"3"}`,
		},
		{
			name:      "double(12,3)",
			filedType: mysql.TypeDouble,
			flen:      12,
			decimal:   3,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"DOUBLE","ColumnPrecision":"12","ColumnScale":"3"}`,
		},
		{
			name:      "double",
			filedType: mysql.TypeDouble,
			flen:      12,
			decimal:   -1,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"DOUBLE","ColumnPrecision":"12"}`,
		},
		{
			name:      "double",
			filedType: mysql.TypeDouble,
			flen:      5,
			decimal:   -1,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"DOUBLE","ColumnPrecision":"5"}`,
		},
		{
			name:      "double(7,3)",
			filedType: mysql.TypeDouble,
			flen:      7,
			decimal:   3,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"DOUBLE","ColumnPrecision":"7","ColumnScale":"3"}`,
		},
		{
			name:      "tinyint(5)",
			filedType: mysql.TypeTiny,
			flen:      5,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"TINYINT","ColumnPrecision":"5"}`,
		},
		{
			name:      "smallint(5)",
			filedType: mysql.TypeShort,
			flen:      5,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"SMALLINT","ColumnPrecision":"5"}`,
		},
		{
			name:      "mediumint(10)",
			filedType: mysql.TypeInt24,
			flen:      10,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"MEDIUMINT","ColumnPrecision":"10"}`,
		},
		{
			name:      "int(11)",
			filedType: mysql.TypeLong,
			flen:      math.MinInt,
			decimal:   math.MinInt,
			flag:      mysql.PriKeyFlag,
			expected:  `{"ColumnIsPk":"true", "ColumnName":"", "ColumnPrecision":"11", "ColumnType":"INT"}`,
		},
		{
			name:      "bigint(20)",
			filedType: mysql.TypeLonglong,
			flen:      20,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"BIGINT","ColumnPrecision":"20"}`,
		},
		{
			name:      "bit(5)",
			filedType: mysql.TypeBit,
			flen:      5,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"BIT","ColumnPrecision":"5"}`,
		},
		{
			name:      "varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
			filedType: mysql.TypeVarchar,
			flen:      128,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"VARCHAR","ColumnPrecision":"128"}`,
		},
		{
			name:      "char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
			filedType: mysql.TypeString,
			flen:      32,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"CHAR","ColumnPrecision":"32"}`,
		},
		{
			name:      "var_string(64)",
			filedType: mysql.TypeVarString,
			flen:      64,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"VAR_STRING","ColumnPrecision":"64"}`,
		},
		{
			name:      "blob",
			filedType: mysql.TypeBlob,
			flen:      100,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"BLOB","ColumnPrecision":"100"}`,
		},
		{
			name:      "tinyblob",
			filedType: mysql.TypeTinyBlob,
			flen:      120,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"TINYBLOB","ColumnPrecision":"120"}`,
		},
		{
			name:      "mediumblob",
			filedType: mysql.TypeMediumBlob,
			flen:      100,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"MEDIUMBLOB","ColumnPrecision":"100"}`,
		},
		{
			name:      "longblob",
			filedType: mysql.TypeLongBlob,
			flen:      5,
			decimal:   math.MinInt,
			flag:      0,
			expected:  `{"ColumnName":"","ColumnType":"LONGBLOB","ColumnPrecision":"5"}`,
		},
		{
			name:      "enum",
			filedType: mysql.TypeEnum,
			expected:  `{"ColumnName":"","ColumnType":"ENUM"}`,
		},
		{
			name:      "set",
			filedType: mysql.TypeSet,
			expected:  `{"ColumnName":"","ColumnType":"SET"}`,
		},
		{
			name:      "timestamp(2)",
			filedType: mysql.TypeTimestamp,
			flen:      8,
			decimal:   2,
			expected:  `{"ColumnName":"","ColumnType":"TIMESTAMP","ColumnScale":"2"}`,
		},
		{
			name:      "timestamp",
			filedType: mysql.TypeTimestamp,
			flen:      8,
			decimal:   0,
			expected:  `{"ColumnName":"","ColumnType":"TIMESTAMP"}`,
		},
		{
			name:      "datetime(2)",
			filedType: mysql.TypeDatetime,
			flen:      8,
			decimal:   2,
			expected:  `{"ColumnName":"","ColumnType":"DATETIME","ColumnScale":"2"}`,
		},
		{
			name:      "datetime",
			filedType: mysql.TypeDatetime,
			flen:      8,
			decimal:   0,
			expected:  `{"ColumnName":"","ColumnType":"DATETIME"}`,
		},
		{
			name:      "date",
			filedType: mysql.TypeDate,
			flen:      8,
			decimal:   2,
			expected:  `{"ColumnName":"","ColumnType":"DATE"}`,
		},
		{
			name:      "date",
			filedType: mysql.TypeDate,
			flen:      8,
			decimal:   0,
			expected:  `{"ColumnName":"","ColumnType":"DATE"}`,
		},
		{
			name:      "year(4)",
			filedType: mysql.TypeYear,
			flen:      4,
			decimal:   0,
			expected:  `{"ColumnName":"","ColumnType":"YEAR","ColumnPrecision":"4"}`,
		},
		{
			name:      "year(2)",
			filedType: mysql.TypeYear,
			flen:      2,
			decimal:   2,
			expected:  `{"ColumnName":"","ColumnType":"YEAR","ColumnPrecision":"2"}`,
		},
	}

	for _, tc := range testCases {
		ft := types.NewFieldType(tc.filedType)
		if tc.flen != math.MinInt {
			ft.SetFlen(tc.flen)
		}
		if tc.decimal != math.MinInt {
			ft.SetDecimal(tc.decimal)
		}
		if tc.flag != 0 {
			ft.SetFlag(tc.flag)
		}
		col := &timodel.ColumnInfo{FieldType: *ft}
		var tableCol TableCol
		tableCol.FromTiColumnInfo(col)
		encodedCol, err := json.Marshal(tableCol)
		require.Nil(t, err, tc.name)
		require.JSONEq(t, tc.expected, string(encodedCol), tc.name)

		_, err = tableCol.ToTiColumnInfo()
		require.Nil(t, err)
	}
}

func TestTableDetail(t *testing.T) {
	var columns []*timodel.ColumnInfo
	var def TableDetail

	tableInfo := &model.TableInfo{
		TableInfoVersion: 100,
		TableName: model.TableName{
			Schema:  "test",
			Table:   "table1",
			TableID: 20,
		},
	}
	ft := types.NewFieldType(mysql.TypeLong)
	ft.SetFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	col := &timodel.ColumnInfo{Name: timodel.NewCIStr("Id"), FieldType: *ft}
	columns = append(columns, col)

	ft = types.NewFieldType(mysql.TypeVarchar)
	ft.SetFlag(mysql.NotNullFlag)
	ft.SetFlen(128)
	col = &timodel.ColumnInfo{Name: timodel.NewCIStr("LastName"), FieldType: *ft}
	columns = append(columns, col)

	ft = types.NewFieldType(mysql.TypeVarchar)
	ft.SetFlen(64)
	col = &timodel.ColumnInfo{Name: timodel.NewCIStr("FirstName"), FieldType: *ft}
	columns = append(columns, col)

	ft = types.NewFieldType(mysql.TypeDatetime)
	col = &timodel.ColumnInfo{Name: timodel.NewCIStr("Birthday"), FieldType: *ft}
	columns = append(columns, col)

	tableInfo.TableInfo = &timodel.TableInfo{Columns: columns}
	def.FromTableInfo(tableInfo)
	encodedDef, err := json.MarshalIndent(def, "", "    ")
	require.Nil(t, err)
	fmt.Println(string(encodedDef))
	require.JSONEq(t, `{
		"Table": "table1",
		"Schema": "test",
		"Version": 100,
		"TableColumns": [
			{
				"ColumnName": "Id",
				"ColumnType": "INT",
				"ColumnPrecision": "11",
				"ColumnNullable": "false",
				"ColumnIsPk": "true"
			},
			{
				"ColumnName": "LastName",
				"ColumnType": "VARCHAR",
				"ColumnPrecision": "128",
				"ColumnNullable": "false"
			},
			{
				"ColumnName": "FirstName",
				"ColumnType": "VARCHAR",
				"ColumnPrecision": "64"
			},
			{
				"ColumnName": "Birthday",
				"ColumnType": "DATETIME"
			}
		],
		"TableColumnsTotal": 4
	}`, string(encodedDef))

	tableInfo, err = def.ToTableInfo()
	require.Nil(t, err)
	require.Len(t, tableInfo.Columns, 4)
}
