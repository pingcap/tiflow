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
	mm "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
)

type testColumnTuple struct {
	column  *model.Column
	colInfo rowcodec.ColInfo

	// expectedEncodedValue is expected by encoding
	expectedEncodedValue string

	// expectedDecodedValue is expected by decoding
	expectedDecodedValue interface{}
}

var (
	testColumnsTable = []*testColumnTuple{
		{
			&model.Column{Name: "tinyint", Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Type: mysql.TypeTiny, Value: int64(127)},
			rowcodec.ColInfo{ID: 1, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeTiny)},
			"127", "127",
		},

		{
			&model.Column{
				Name: "tinyint unsigned", Type: mysql.TypeTiny, Value: uint64(127),
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{ID: 2, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeTiny))},
			"127", "127",
		},

		{
			&model.Column{
				Name: "tinyint unsigned 2", Type: mysql.TypeTiny, Value: uint64(128),
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{ID: 3, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeTiny))},
			"128", "128",
		},

		{
			&model.Column{
				Name: "tinyint unsigned 3", Type: mysql.TypeTiny, Value: "0",
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{ID: 4, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeTiny))},
			"0", "0",
		},

		{
			&model.Column{
				Name: "tinyint unsigned 4", Type: mysql.TypeTiny, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
			rowcodec.ColInfo{ID: 5, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeTiny))},
			"", nil,
		},

		{
			&model.Column{Name: "smallint", Type: mysql.TypeShort, Value: int64(32767)},
			rowcodec.ColInfo{ID: 6, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeShort)},
			"32767", "32767",
		},
		{
			&model.Column{
				Name: "smallint unsigned", Type: mysql.TypeShort, Value: uint64(32767),
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{ID: 7, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeShort))},
			"32767", "32767",
		},
		{
			&model.Column{
				Name: "smallint unsigned 2", Type: mysql.TypeShort, Value: uint64(32768),
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{ID: 8, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeShort))},
			"32768", "32768",
		},
		{
			&model.Column{
				Name: "smallint unsigned 3", Type: mysql.TypeShort, Value: "0",
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{ID: 9, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeShort))},
			"0", "0",
		},
		{
			&model.Column{
				Name: "smallint unsigned 4", Type: mysql.TypeShort, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
			rowcodec.ColInfo{ID: 10, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeShort))},
			"", nil,
		},

		{
			&model.Column{Name: "mediumint", Type: mysql.TypeInt24, Value: int64(8388607)},
			rowcodec.ColInfo{ID: 11, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeInt24)},
			"8388607", "8388607",
		},
		{
			&model.Column{
				Name: "mediumint unsigned", Type: mysql.TypeInt24, Value: uint64(8388607),
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{ID: 12, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeInt24))},
			"8388607", "8388607",
		},
		{
			&model.Column{
				Name: "mediumint unsigned 2", Type: mysql.TypeInt24, Value: uint64(8388608),
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{ID: 13, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeInt24))},
			"8388608", "8388608",
		},
		{
			&model.Column{
				Name: "mediumint unsigned 3", Type: mysql.TypeInt24, Value: "0",
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{ID: 14, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeInt24))},
			"0", "0",
		},
		{
			&model.Column{
				Name: "mediumint unsigned 4", Type: mysql.TypeInt24, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
			rowcodec.ColInfo{ID: 15, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeInt24))},
			"", nil,
		},

		{
			&model.Column{Name: "int", Type: mysql.TypeLong, Value: int64(2147483647)},
			rowcodec.ColInfo{ID: 16, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeLong)},
			"2147483647", "2147483647",
		},
		{
			&model.Column{
				Name: "int unsigned", Type: mysql.TypeLong, Value: uint64(2147483647),
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{ID: 17, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeLong))},
			"2147483647", "2147483647",
		},
		{
			&model.Column{
				Name: "int unsigned 2", Type: mysql.TypeLong, Value: uint64(2147483648),
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{ID: 18, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeLong))},
			"2147483648", "2147483648",
		},
		{
			&model.Column{
				Name: "int unsigned 3", Type: mysql.TypeLong, Value: "0",
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{ID: 19, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeLong))},
			"0", "0",
		},
		{
			&model.Column{
				Name: "int unsigned 4", Type: mysql.TypeLong, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
			rowcodec.ColInfo{ID: 20, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeLong))},
			"", nil,
		},

		{
			&model.Column{Name: "bigint", Type: mysql.TypeLonglong, Value: int64(9223372036854775807)},
			rowcodec.ColInfo{ID: 21, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeLonglong)},
			"9223372036854775807", "9223372036854775807",
		},
		{
			&model.Column{
				Name: "bigint unsigned", Type: mysql.TypeLonglong, Value: uint64(9223372036854775807),
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{
				ID:            22,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            utils.SetUnsigned(types.NewFieldType(mysql.TypeLonglong)),
			},
			"9223372036854775807", "9223372036854775807",
		},
		{
			&model.Column{
				Name: "bigint unsigned 2", Type: mysql.TypeLonglong, Value: uint64(9223372036854775808),
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{
				ID:            23,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            utils.SetUnsigned(types.NewFieldType(mysql.TypeLonglong)),
			},
			"9223372036854775808", "9223372036854775808",
		},
		{
			&model.Column{
				Name: "bigint unsigned 3", Type: mysql.TypeLonglong, Value: "0",
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{
				ID:            24,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            utils.SetUnsigned(types.NewFieldType(mysql.TypeLonglong)),
			},
			"0", "0",
		},
		{
			&model.Column{
				Name: "bigint unsigned 4", Type: mysql.TypeLonglong, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
			rowcodec.ColInfo{
				ID:            25,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            utils.SetUnsigned(types.NewFieldType(mysql.TypeLonglong)),
			},
			"", nil,
		},

		{
			&model.Column{Name: "float", Type: mysql.TypeFloat, Value: 3.14},
			rowcodec.ColInfo{ID: 26, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeFloat)},
			"3.14", "3.14",
		},
		{
			&model.Column{Name: "double", Type: mysql.TypeDouble, Value: 2.71},
			rowcodec.ColInfo{ID: 27, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeDouble)},
			"2.71", "2.71",
		},
		{
			&model.Column{Name: "decimal", Type: mysql.TypeNewDecimal, Value: "2333"},
			rowcodec.ColInfo{ID: 28, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeNewDecimal)},
			"2333", "2333",
		},

		{
			&model.Column{
				Name: "float unsigned", Type: mysql.TypeFloat, Value: 3.14,
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{ID: 29, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeFloat))},
			"3.14", "3.14",
		},
		{
			&model.Column{
				Name: "double unsigned", Type: mysql.TypeDouble, Value: 2.71,
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{ID: 30, IsPKHandle: false, VirtualGenCol: false, Ft: utils.SetUnsigned(types.NewFieldType(mysql.TypeDouble))},
			"2.71", "2.71",
		},
		{
			&model.Column{
				Name: "decimal unsigned", Type: mysql.TypeNewDecimal, Value: "2333",
				Flag: model.UnsignedFlag,
			},
			rowcodec.ColInfo{
				ID:            31,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            utils.SetUnsigned(types.NewFieldType(mysql.TypeNewDecimal)),
			},
			"2333", "2333",
		},
		//
		// for column value type in `[]uint8` and have `BinaryFlag`, expectedEncodedValue is dummy.
		{
			&model.Column{Name: "varchar", Type: mysql.TypeVarchar, Value: []uint8("测试Varchar")},
			rowcodec.ColInfo{ID: 32, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeVarchar)},
			"测试Varchar", "测试Varchar",
		},
		{
			&model.Column{Name: "char", Type: mysql.TypeString, Value: []uint8("测试String")},
			rowcodec.ColInfo{ID: 33, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeString)},
			"测试String", "测试String",
		},
		{
			&model.Column{
				Name: "binary", Type: mysql.TypeString, Value: []uint8("测试Binary"),
				Flag: model.BinaryFlag,
			},
			rowcodec.ColInfo{
				ID:            34,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            utils.SetBinChsClnFlag(types.NewFieldType(mysql.TypeString)),
			},
			"测试Binary", "测试Binary",
		},
		{
			&model.Column{
				Name: "varbinary", Type: mysql.TypeVarchar, Value: []uint8("测试varbinary"),
				Flag: model.BinaryFlag,
			},
			rowcodec.ColInfo{
				ID:            35,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            utils.SetBinChsClnFlag(types.NewFieldType(mysql.TypeVarchar)),
			},
			"测试varbinary", "测试varbinary",
		},

		{
			&model.Column{Name: "tinytext", Type: mysql.TypeTinyBlob, Value: []uint8("测试Tinytext")},
			rowcodec.ColInfo{ID: 36, IsPKHandle: false, VirtualGenCol: false, Ft: utils.NewTextFieldType(mysql.TypeTinyBlob)},
			"测试Tinytext", "测试Tinytext",
		},

		{
			&model.Column{Name: "text", Type: mysql.TypeBlob, Value: []uint8("测试text")},
			rowcodec.ColInfo{ID: 37, IsPKHandle: false, VirtualGenCol: false, Ft: utils.NewTextFieldType(mysql.TypeBlob)},
			"测试text", "测试text",
		},
		{
			&model.Column{Name: "mediumtext", Type: mysql.TypeMediumBlob, Value: []uint8("测试mediumtext")},
			rowcodec.ColInfo{ID: 38, IsPKHandle: false, VirtualGenCol: false, Ft: utils.NewTextFieldType(mysql.TypeMediumBlob)},
			"测试mediumtext", "测试mediumtext",
		},
		{
			&model.Column{Name: "longtext", Type: mysql.TypeLongBlob, Value: []uint8("测试longtext")},
			rowcodec.ColInfo{ID: 39, IsPKHandle: false, VirtualGenCol: false, Ft: utils.NewTextFieldType(mysql.TypeLongBlob)},
			"测试longtext", "测试longtext",
		},

		{
			&model.Column{
				Name: "tinyblob", Type: mysql.TypeTinyBlob, Value: []uint8("测试tinyblob"),
				Flag: model.BinaryFlag,
			},
			rowcodec.ColInfo{ID: 40, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeTinyBlob)},
			"测试tinyblob", "测试tinyblob",
		},
		{
			&model.Column{
				Name: "blob", Type: mysql.TypeBlob, Value: []uint8("测试blob"),
				Flag: model.BinaryFlag,
			},
			rowcodec.ColInfo{ID: 41, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeBlob)},
			"测试blob", "测试blob",
		},
		{
			&model.Column{
				Name: "mediumblob", Type: mysql.TypeMediumBlob, Value: []uint8("测试mediumblob"),
				Flag: model.BinaryFlag,
			},
			rowcodec.ColInfo{ID: 42, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeMediumBlob)},
			"测试mediumblob", "测试mediumblob",
		},
		{
			&model.Column{
				Name: "longblob", Type: mysql.TypeLongBlob, Value: []uint8("测试longblob"),
				Flag: model.BinaryFlag,
			},
			rowcodec.ColInfo{ID: 43, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeLongBlob)},
			"测试longblob", "测试longblob",
		},

		{
			&model.Column{Name: "date", Type: mysql.TypeDate, Value: "2020-02-20"},
			rowcodec.ColInfo{ID: 44, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeDate)},
			"2020-02-20", "2020-02-20",
		},
		{
			&model.Column{Name: "datetime", Type: mysql.TypeDatetime, Value: "2020-02-20 02:20:20"},
			rowcodec.ColInfo{ID: 45, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeDatetime)},
			"2020-02-20 02:20:20", "2020-02-20 02:20:20",
		},
		{
			&model.Column{Name: "timestamp", Type: mysql.TypeTimestamp, Value: "2020-02-20 10:20:20"},
			rowcodec.ColInfo{ID: 46, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeTimestamp)},
			"2020-02-20 10:20:20", "2020-02-20 10:20:20",
		},
		{
			&model.Column{Name: "time", Type: mysql.TypeDuration, Value: "02:20:20"},
			rowcodec.ColInfo{ID: 47, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeDuration)},
			"02:20:20", "02:20:20",
		},
		{
			&model.Column{Name: "year", Type: mysql.TypeYear, Value: "2020", Flag: model.UnsignedFlag},
			rowcodec.ColInfo{ID: 48, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeYear)},
			"2020", "2020",
		},

		{
			&model.Column{Name: "enum", Type: mysql.TypeEnum, Value: uint64(1)},
			rowcodec.ColInfo{ID: 49, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeEnum)},
			"1", "1",
		},
		{
			&model.Column{Name: "set", Type: mysql.TypeSet, Value: uint64(2)},
			rowcodec.ColInfo{
				ID:            50,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            utils.SetElems(types.NewFieldType(mysql.TypeSet), []string{"a", "b", "c"}),
			},
			"2", uint64(2),
		},
		{
			&model.Column{
				Name: "bit", Type: mysql.TypeBit, Value: uint64(65),
				Flag: model.UnsignedFlag | model.BinaryFlag,
			},
			rowcodec.ColInfo{ID: 51, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeBit)},
			"65", uint64(65),
		},
		{
			&model.Column{
				Name: "json", Type: mysql.TypeJSON, Value: "{\"key1\": \"value1\"}",
				Flag: model.BinaryFlag,
			},
			rowcodec.ColInfo{ID: 52, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeJSON)},
			"{\"key1\": \"value1\"}", "{\"key1\": \"value1\"}",
		},
	}

	defaultCanalBatchTester = &struct {
		rowCases [][]*model.RowChangedEvent
		ddlCases [][]*model.DDLEvent
	}{
		rowCases: [][]*model.RowChangedEvent{
			{{
				CommitTs: 1,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns: []*model.Column{{
					Name:  "col1",
					Type:  mysql.TypeVarchar,
					Value: []byte("aa"),
				}},
				ColInfos: []rowcodec.ColInfo{{
					ID:            1,
					IsPKHandle:    false,
					VirtualGenCol: false,
					Ft:            types.NewFieldType(mysql.TypeVarchar),
				}},
			}},
			{
				{
					CommitTs: 1,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns: []*model.Column{{
						Name:  "col1",
						Type:  mysql.TypeVarchar,
						Value: []byte("aa"),
					}},
					ColInfos: []rowcodec.ColInfo{{
						ID:            1,
						IsPKHandle:    false,
						VirtualGenCol: false,
						Ft:            types.NewFieldType(mysql.TypeVarchar),
					}},
				},
				{
					CommitTs: 2,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
					ColInfos: []rowcodec.ColInfo{{
						ID:            1,
						IsPKHandle:    false,
						VirtualGenCol: false,
						Ft:            types.NewFieldType(mysql.TypeVarchar),
					}},
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

	testColumns, testColInfos = collectAllColumns(testColumnsTable)

	testCaseInsert = &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		Columns:    testColumns,
		PreColumns: nil,
		ColInfos:   testColInfos,
	}

	testCaseUpdate = &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		Columns:    testColumns,
		PreColumns: testColumns,
		ColInfos:   testColInfos,
	}

	testCaseDelete = &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		Columns:    nil,
		PreColumns: testColumns,
		ColInfos:   testColInfos,
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

func collectAllColumns(groups []*testColumnTuple) ([]*model.Column, []rowcodec.ColInfo) {
	columns := make([]*model.Column, 0, len(groups))
	colInfos := make([]rowcodec.ColInfo, 0, len(groups))
	for _, item := range groups {
		columns = append(columns, item.column)
		colInfos = append(colInfos, item.colInfo)
	}
	return columns, colInfos
}

func collectExpectedDecodedValue(columns []*testColumnTuple) map[string]interface{} {
	result := make(map[string]interface{}, len(columns))
	for _, item := range columns {
		result[item.column.Name] = item.expectedDecodedValue
	}
	return result
}
