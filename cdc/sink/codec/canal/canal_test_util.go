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

package canal

import (
	mm "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/internal"
)

// testColumn combines model.Column and model.ColumnValue, for tests.
type testColumn struct {
	Name  string
	Type  byte
	Flag  model.ColumnFlagType
	Value interface{}
}

// testRowChangedEvent is just like model.RowChangedEvent.
type testRowChangedEvent struct {
	CommitTs   uint64
	Table      *model.TableName
	Columns    []*testColumn
	PreColumns []*testColumn
}

type testColumnTuple struct {
	column              *testColumn
	expectedMySQLType   string
	expectedJavaSQLType internal.JavaSQLType

	// expectedEncodedValue is expected by encoding
	expectedEncodedValue string

	// expectedDecodedValue is expected by decoding
	expectedDecodedValue interface{}
}

var (
	testColumnsTable = []*testColumnTuple{
		{
			&testColumn{Name: "tinyint", Type: mysql.TypeTiny, Value: int64(127)},
			"tinyint", internal.JavaSQLTypeTINYINT, "127", "127",
		},

		{
			&testColumn{
				Name: "tinyint unsigned", Type: mysql.TypeTiny, Value: uint64(127),
				Flag: model.UnsignedFlag,
			},
			"tinyint unsigned", internal.JavaSQLTypeTINYINT, "127", "127",
		},

		{
			&testColumn{
				Name: "tinyint unsigned 2", Type: mysql.TypeTiny, Value: uint64(128),
				Flag: model.UnsignedFlag,
			},
			"tinyint unsigned", internal.JavaSQLTypeSMALLINT, "128", "128",
		},

		{
			&testColumn{
				Name: "tinyint unsigned 3", Type: mysql.TypeTiny, Value: "0",
				Flag: model.UnsignedFlag,
			},
			"tinyint unsigned", internal.JavaSQLTypeTINYINT, "0", "0",
		},

		{
			&testColumn{
				Name: "tinyint unsigned 4", Type: mysql.TypeTiny, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
			"tinyint unsigned", internal.JavaSQLTypeTINYINT, "", nil,
		},

		{
			&testColumn{Name: "smallint", Type: mysql.TypeShort, Value: int64(32767)},
			"smallint", internal.JavaSQLTypeSMALLINT, "32767", "32767",
		},
		{
			&testColumn{
				Name: "smallint unsigned", Type: mysql.TypeShort, Value: uint64(32767),
				Flag: model.UnsignedFlag,
			},
			"smallint unsigned", internal.JavaSQLTypeSMALLINT, "32767", "32767",
		},
		{
			&testColumn{
				Name: "smallint unsigned 2", Type: mysql.TypeShort, Value: uint64(32768),
				Flag: model.UnsignedFlag,
			},
			"smallint unsigned", internal.JavaSQLTypeINTEGER, "32768", "32768",
		},
		{
			&testColumn{
				Name: "smallint unsigned 3", Type: mysql.TypeShort, Value: "0",
				Flag: model.UnsignedFlag,
			},
			"smallint unsigned", internal.JavaSQLTypeSMALLINT, "0", "0",
		},
		{
			&testColumn{
				Name: "smallint unsigned 4", Type: mysql.TypeShort, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
			"smallint unsigned", internal.JavaSQLTypeSMALLINT, "", nil,
		},

		{
			&testColumn{Name: "mediumint", Type: mysql.TypeInt24, Value: int64(8388607)},
			"mediumint", internal.JavaSQLTypeINTEGER, "8388607", "8388607",
		},
		{
			&testColumn{
				Name: "mediumint unsigned", Type: mysql.TypeInt24, Value: uint64(8388607),
				Flag: model.UnsignedFlag,
			},
			"mediumint unsigned", internal.JavaSQLTypeINTEGER, "8388607", "8388607",
		},
		{
			&testColumn{
				Name: "mediumint unsigned 2", Type: mysql.TypeInt24, Value: uint64(8388608),
				Flag: model.UnsignedFlag,
			},
			"mediumint unsigned", internal.JavaSQLTypeINTEGER, "8388608", "8388608",
		},
		{
			&testColumn{
				Name: "mediumint unsigned 3", Type: mysql.TypeInt24, Value: "0",
				Flag: model.UnsignedFlag,
			},
			"mediumint unsigned", internal.JavaSQLTypeINTEGER, "0", "0",
		},
		{
			&testColumn{
				Name: "mediumint unsigned 4", Type: mysql.TypeInt24, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
			"mediumint unsigned", internal.JavaSQLTypeINTEGER, "", nil,
		},

		{
			&testColumn{Name: "int", Type: mysql.TypeLong, Value: int64(2147483647)},
			"int", internal.JavaSQLTypeINTEGER, "2147483647", "2147483647",
		},
		{
			&testColumn{
				Name: "int unsigned", Type: mysql.TypeLong, Value: uint64(2147483647),
				Flag: model.UnsignedFlag,
			},
			"int unsigned", internal.JavaSQLTypeINTEGER, "2147483647", "2147483647",
		},
		{
			&testColumn{
				Name: "int unsigned 2", Type: mysql.TypeLong, Value: uint64(2147483648),
				Flag: model.UnsignedFlag,
			},
			"int unsigned", internal.JavaSQLTypeBIGINT, "2147483648", "2147483648",
		},
		{
			&testColumn{
				Name: "int unsigned 3", Type: mysql.TypeLong, Value: "0",
				Flag: model.UnsignedFlag,
			},
			"int unsigned", internal.JavaSQLTypeINTEGER, "0", "0",
		},
		{
			&testColumn{
				Name: "int unsigned 4", Type: mysql.TypeLong, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
			"int unsigned", internal.JavaSQLTypeINTEGER, "", nil,
		},

		{
			&testColumn{Name: "bigint", Type: mysql.TypeLonglong, Value: int64(9223372036854775807)},
			"bigint", internal.JavaSQLTypeBIGINT, "9223372036854775807", "9223372036854775807",
		},
		{
			&testColumn{
				Name: "bigint unsigned", Type: mysql.TypeLonglong, Value: uint64(9223372036854775807),
				Flag: model.UnsignedFlag,
			},
			"bigint unsigned", internal.JavaSQLTypeBIGINT, "9223372036854775807", "9223372036854775807",
		},
		{
			&testColumn{
				Name: "bigint unsigned 2", Type: mysql.TypeLonglong, Value: uint64(9223372036854775808),
				Flag: model.UnsignedFlag,
			},
			"bigint unsigned", internal.JavaSQLTypeDECIMAL, "9223372036854775808", "9223372036854775808",
		},
		{
			&testColumn{
				Name: "bigint unsigned 3", Type: mysql.TypeLonglong, Value: "0",
				Flag: model.UnsignedFlag,
			},
			"bigint unsigned", internal.JavaSQLTypeBIGINT, "0", "0",
		},
		{
			&testColumn{
				Name: "bigint unsigned 4", Type: mysql.TypeLonglong, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
			"bigint unsigned", internal.JavaSQLTypeBIGINT, "", nil,
		},

		{
			&testColumn{Name: "float", Type: mysql.TypeFloat, Value: 3.14},
			"float", internal.JavaSQLTypeREAL, "3.14", "3.14",
		},
		{
			&testColumn{Name: "double", Type: mysql.TypeDouble, Value: 2.71},
			"double", internal.JavaSQLTypeDOUBLE, "2.71", "2.71",
		},
		{
			&testColumn{Name: "decimal", Type: mysql.TypeNewDecimal, Value: "2333"},
			"decimal", internal.JavaSQLTypeDECIMAL, "2333", "2333",
		},

		{
			&testColumn{
				Name: "float unsigned", Type: mysql.TypeFloat, Value: 3.14,
				Flag: model.UnsignedFlag,
			},
			"float unsigned", internal.JavaSQLTypeREAL, "3.14", "3.14",
		},
		{
			&testColumn{
				Name: "double unsigned", Type: mysql.TypeDouble, Value: 2.71,
				Flag: model.UnsignedFlag,
			},
			"double unsigned", internal.JavaSQLTypeDOUBLE, "2.71", "2.71",
		},
		{
			&testColumn{
				Name: "decimal unsigned", Type: mysql.TypeNewDecimal, Value: "2333",
				Flag: model.UnsignedFlag,
			},
			"decimal unsigned", internal.JavaSQLTypeDECIMAL, "2333", "2333",
		},

		// for column value type in `[]uint8` and have `BinaryFlag`, expectedEncodedValue is dummy.
		{
			&testColumn{Name: "varchar", Type: mysql.TypeVarchar, Value: []uint8("测试Varchar")},
			"varchar", internal.JavaSQLTypeVARCHAR, "测试Varchar", "测试Varchar",
		},
		{
			&testColumn{Name: "char", Type: mysql.TypeString, Value: []uint8("测试String")},
			"char", internal.JavaSQLTypeCHAR, "测试String", "测试String",
		},
		{
			&testColumn{
				Name: "binary", Type: mysql.TypeString, Value: []uint8("测试Binary"),
				Flag: model.BinaryFlag,
			},
			"binary", internal.JavaSQLTypeBLOB, "测试Binary", "测试Binary",
		},
		{
			&testColumn{
				Name: "varbinary", Type: mysql.TypeVarchar, Value: []uint8("测试varbinary"),
				Flag: model.BinaryFlag,
			},
			"varbinary", internal.JavaSQLTypeBLOB, "测试varbinary", "测试varbinary",
		},

		{
			&testColumn{Name: "tinytext", Type: mysql.TypeTinyBlob, Value: []uint8("测试Tinytext")},
			"tinytext", internal.JavaSQLTypeCLOB, "测试Tinytext", "测试Tinytext",
		},
		{
			&testColumn{Name: "text", Type: mysql.TypeBlob, Value: []uint8("测试text")},
			"text", internal.JavaSQLTypeCLOB, "测试text", "测试text",
		},
		{
			&testColumn{
				Name: "mediumtext", Type: mysql.TypeMediumBlob,
				Value: []uint8("测试mediumtext"),
			},
			"mediumtext", internal.JavaSQLTypeCLOB, "测试mediumtext", "测试mediumtext",
		},
		{
			&testColumn{Name: "longtext", Type: mysql.TypeLongBlob, Value: []uint8("测试longtext")},
			"longtext", internal.JavaSQLTypeCLOB, "测试longtext", "测试longtext",
		},

		{
			&testColumn{
				Name: "tinyblob", Type: mysql.TypeTinyBlob, Value: []uint8("测试tinyblob"),
				Flag: model.BinaryFlag,
			},
			"tinyblob", internal.JavaSQLTypeBLOB, "测试tinyblob", "测试tinyblob",
		},
		{
			&testColumn{
				Name: "blob", Type: mysql.TypeBlob, Value: []uint8("测试blob"),
				Flag: model.BinaryFlag,
			},
			"blob", internal.JavaSQLTypeBLOB, "测试blob", "测试blob",
		},
		{
			&testColumn{
				Name: "mediumblob", Type: mysql.TypeMediumBlob, Value: []uint8("测试mediumblob"),
				Flag: model.BinaryFlag,
			},
			"mediumblob", internal.JavaSQLTypeBLOB, "测试mediumblob", "测试mediumblob",
		},
		{
			&testColumn{
				Name: "longblob", Type: mysql.TypeLongBlob, Value: []uint8("测试longblob"),
				Flag: model.BinaryFlag,
			},
			"longblob", internal.JavaSQLTypeBLOB, "测试longblob", "测试longblob",
		},

		{
			&testColumn{Name: "date", Type: mysql.TypeDate, Value: "2020-02-20"},
			"date", internal.JavaSQLTypeDATE, "2020-02-20", "2020-02-20",
		},
		{
			&testColumn{Name: "datetime", Type: mysql.TypeDatetime, Value: "2020-02-20 02:20:20"},
			"datetime", internal.JavaSQLTypeTIMESTAMP, "2020-02-20 02:20:20", "2020-02-20 02:20:20",
		},
		{
			&testColumn{Name: "timestamp", Type: mysql.TypeTimestamp, Value: "2020-02-20 10:20:20"},
			"timestamp", internal.JavaSQLTypeTIMESTAMP, "2020-02-20 10:20:20", "2020-02-20 10:20:20",
		},
		{
			&testColumn{Name: "time", Type: mysql.TypeDuration, Value: "02:20:20"},
			"time", internal.JavaSQLTypeTIME, "02:20:20", "02:20:20",
		},
		{
			&testColumn{Name: "year", Type: mysql.TypeYear, Value: "2020", Flag: model.UnsignedFlag},
			"year", internal.JavaSQLTypeVARCHAR, "2020", "2020",
		},

		{
			&testColumn{Name: "enum", Type: mysql.TypeEnum, Value: uint64(1)},
			"enum", internal.JavaSQLTypeINTEGER, "1", "1",
		},
		{
			&testColumn{Name: "set", Type: mysql.TypeSet, Value: uint64(3)},
			"set", internal.JavaSQLTypeBIT, "3", uint64(3),
		},
		{
			&testColumn{
				Name: "bit", Type: mysql.TypeBit, Value: uint64(65),
				Flag: model.UnsignedFlag | model.BinaryFlag,
			},
			"bit", internal.JavaSQLTypeBIT, "65", uint64(65),
		},
		{
			&testColumn{
				Name: "json", Type: mysql.TypeJSON, Value: "{\"key1\": \"value1\"}",
				Flag: model.BinaryFlag,
			},
			"json", internal.JavaSQLTypeVARCHAR, "{\"key1\": \"value1\"}", "{\"key1\": \"value1\"}",
		},
	}

	defaultCanalBatchTester = &struct {
		rowCases [][]*testRowChangedEvent
		ddlCases [][]*model.DDLEvent
	}{
		rowCases: [][]*testRowChangedEvent{
			{{
				CommitTs: 1,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns: []*testColumn{{
					Name:  "col1",
					Type:  mysql.TypeVarchar,
					Value: []byte("aa"),
				}},
			}},
			{
				{
					CommitTs: 1,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns: []*testColumn{{
						Name:  "col1",
						Type:  mysql.TypeVarchar,
						Value: []byte("aa"),
					}},
				},
				{
					CommitTs: 2,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns:  []*testColumn{{Name: "col1", Type: 1, Value: "bb"}},
				},
			},
		},
		ddlCases: [][]*model.DDLEvent{
			{{
				CommitTs: 1,
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: "a", Table: "b",
					},
				},
				Query: "create table a",
				Type:  1,
			}},
			{
				{
					CommitTs: 2,
					TableInfo: &model.TableInfo{
						TableName: model.TableName{
							Schema: "a", Table: "b",
						},
					},
					Query: "create table b",
					Type:  3,
				},
				{
					CommitTs: 3,
					TableInfo: &model.TableInfo{
						TableName: model.TableName{
							Schema: "a", Table: "b",
						},
					},
					Query: "create table c",
					Type:  3,
				},
			},
		},
	}

	testColumns = collectAllColumns(testColumnsTable)

	testCaseInsert = &testRowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		Columns:    testColumns,
		PreColumns: nil,
	}

	testCaseUpdate = &testRowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		Columns:    testColumns,
		PreColumns: testColumns,
	}

	testCaseDelete = &testRowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		Columns:    nil,
		PreColumns: testColumns,
	}

	testCaseDDL = &model.DDLEvent{
		CommitTs: 417318403368288260,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema: "cdc", Table: "person",
			},
		},
		Query: "create table person(id int, name varchar(32), tiny tinyint unsigned, comment text, primary key(id))",
		Type:  mm.ActionCreateTable,
	}
)

func collectAllColumns(groups []*testColumnTuple) []*testColumn {
	result := make([]*testColumn, 0, len(groups))
	for _, item := range groups {
		result = append(result, item.column)
	}
	return result
}

func collectExpectedDecodedValue(columns []*testColumnTuple) map[string]interface{} {
	result := make(map[string]interface{}, len(columns))
	for _, item := range columns {
		result[item.column.Name] = item.expectedDecodedValue
	}
	return result
}
