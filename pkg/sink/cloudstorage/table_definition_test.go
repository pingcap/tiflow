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
	"math"
	"math/rand"
	"testing"

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func generateTableDef() (TableDefinition, *model.TableInfo) {
	var columns []*timodel.ColumnInfo
	ft := types.NewFieldType(mysql.TypeLong)
	ft.SetFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	col := &timodel.ColumnInfo{
		Name:         pmodel.NewCIStr("Id"),
		FieldType:    *ft,
		DefaultValue: 10,
	}
	columns = append(columns, col)

	ft = types.NewFieldType(mysql.TypeVarchar)
	ft.SetFlag(mysql.NotNullFlag)
	ft.SetFlen(128)
	col = &timodel.ColumnInfo{
		Name:         pmodel.NewCIStr("LastName"),
		FieldType:    *ft,
		DefaultValue: "Default LastName",
	}
	columns = append(columns, col)

	ft = types.NewFieldType(mysql.TypeVarchar)
	ft.SetFlen(64)
	col = &timodel.ColumnInfo{
		Name:         pmodel.NewCIStr("FirstName"),
		FieldType:    *ft,
		DefaultValue: "Default FirstName",
	}
	columns = append(columns, col)

	ft = types.NewFieldType(mysql.TypeDatetime)
	col = &timodel.ColumnInfo{
		Name:         pmodel.NewCIStr("Birthday"),
		FieldType:    *ft,
		DefaultValue: 12345678,
	}
	columns = append(columns, col)

	tableInfo := &model.TableInfo{
		TableInfo: &timodel.TableInfo{Columns: columns},
		Version:   100,
		TableName: model.TableName{
			Schema:  "schema1",
			Table:   "table1",
			TableID: 20,
		},
	}

	var def TableDefinition
	def.FromTableInfo(tableInfo, tableInfo.Version, false)
	return def, tableInfo
}

func TestTableCol(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		filedType byte
		flen      int
		decimal   int
		flag      uint
		charset   string
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
			name:      "text",
			filedType: mysql.TypeBlob,
			flen:      100,
			decimal:   math.MinInt,
			flag:      0,
			charset:   charset.CharsetUTF8MB4,
			expected:  `{"ColumnName":"","ColumnType":"TEXT","ColumnPrecision":"100"}`,
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
		if len(tc.charset) != 0 {
			ft.SetCharset(tc.charset)
		}
		col := &timodel.ColumnInfo{FieldType: *ft}
		var tableCol TableCol
		tableCol.FromTiColumnInfo(col, false)
		encodedCol, err := json.Marshal(tableCol)
		require.Nil(t, err, tc.name)
		require.JSONEq(t, tc.expected, string(encodedCol), tc.name)

		_, err = tableCol.ToTiColumnInfo(100)
		require.NoError(t, err)
	}
}

func TestTableDefinition(t *testing.T) {
	t.Parallel()

	def, tableInfo := generateTableDef()
	encodedDef, err := json.MarshalIndent(def, "", "    ")
	require.NoError(t, err)
	require.JSONEq(t, `{
		"Table": "table1",
		"Schema": "schema1",
		"Version": 1,
		"TableVersion": 100,
		"Query": "",
		"Type": 0,
		"TableColumns": [
			{
				"ColumnName": "Id",
				"ColumnType": "INT",
				"ColumnPrecision": "11",
				"ColumnDefault":10,
				"ColumnNullable": "false",
				"ColumnIsPk": "true"
			},
			{
				"ColumnName": "LastName",
				"ColumnType": "VARCHAR",
				"ColumnDefault":"Default LastName",
				"ColumnPrecision": "128",
				"ColumnNullable": "false"
			},
			{
				"ColumnName": "FirstName",
				"ColumnDefault":"Default FirstName",
				"ColumnType": "VARCHAR",
				"ColumnPrecision": "64"
			},
			{
				"ColumnName": "Birthday",
				"ColumnDefault":1.2345678e+07,
				"ColumnType": "DATETIME"
			}
		],
		"TableColumnsTotal": 4
	}`, string(encodedDef))

	def = TableDefinition{}
	event := &model.DDLEvent{
		CommitTs:  tableInfo.Version,
		Type:      timodel.ActionAddColumn,
		Query:     "alter table schema1.table1 add Birthday date",
		TableInfo: tableInfo,
	}
	def.FromDDLEvent(event, false)
	encodedDef, err = json.MarshalIndent(def, "", "    ")
	require.NoError(t, err)
	require.JSONEq(t, `{
		"Table": "table1",
		"Schema": "schema1",
		"Version": 1,
		"TableVersion": 100,
		"Query": "alter table schema1.table1 add Birthday date",
		"Type": 5,
		"TableColumns": [
			{
				"ColumnName": "Id",
				"ColumnType": "INT",
				"ColumnPrecision": "11",
				"ColumnDefault":10,
				"ColumnNullable": "false",
				"ColumnIsPk": "true"
			},
			{
				"ColumnName": "LastName",
				"ColumnType": "VARCHAR",
				"ColumnDefault":"Default LastName",
				"ColumnPrecision": "128",
				"ColumnNullable": "false"
			},
			{
				"ColumnName": "FirstName",
				"ColumnDefault":"Default FirstName",
				"ColumnType": "VARCHAR",
				"ColumnPrecision": "64"
			},
			{
				"ColumnName": "Birthday",
				"ColumnDefault":1.2345678e+07,
				"ColumnType": "DATETIME"
			}
		],
		"TableColumnsTotal": 4
	}`, string(encodedDef))

	tableInfo, err = def.ToTableInfo()
	require.NoError(t, err)
	require.Len(t, tableInfo.Columns, 4)

	event, err = def.ToDDLEvent()
	require.NoError(t, err)
	require.Equal(t, timodel.ActionAddColumn, event.Type)
	require.Equal(t, uint64(100), event.CommitTs)
}

func TestTableDefinitionGenFilePath(t *testing.T) {
	t.Parallel()

	schemaDef := &TableDefinition{
		Schema:       "schema1",
		Version:      defaultTableDefinitionVersion,
		TableVersion: 100,
	}
	schemaPath, err := schemaDef.GenerateSchemaFilePath()
	require.NoError(t, err)
	require.Equal(t, "schema1/meta/schema_100_3233644819.json", schemaPath)

	def, _ := generateTableDef()
	tablePath, err := def.GenerateSchemaFilePath()
	require.NoError(t, err)
	require.Equal(t, "schema1/table1/meta/schema_100_3752767265.json", tablePath)
}

func TestTableDefinitionSum32(t *testing.T) {
	t.Parallel()

	def, _ := generateTableDef()
	checksum1, err := def.Sum32(nil)
	require.NoError(t, err)
	checksum2, err := def.Sum32(nil)
	require.NoError(t, err)
	require.Equal(t, checksum1, checksum2)

	n := len(def.Columns)
	newCol := make([]TableCol, n)
	copy(newCol, def.Columns)
	newDef := def
	newDef.Columns = newCol

	for i := 0; i < n; i++ {
		target := rand.Intn(n)
		newDef.Columns[i], newDef.Columns[target] = newDef.Columns[target], newDef.Columns[i]
		newChecksum, err := newDef.Sum32(nil)
		require.NoError(t, err)
		require.Equal(t, checksum1, newChecksum)
	}
}
