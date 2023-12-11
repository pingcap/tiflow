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

//go:build intest
// +build intest

package canal

import (
	"testing"

	mm "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
)

type testColumnTuple struct {
	column *model.Column

	// expectedEncodedValue is expected by encoding
	expectedEncodedValue string

	// expectedDecodedValue is expected by decoding
	expectedDecodedValue interface{}
}

var (
	testColumnsTable = []*testColumnTuple{
		{
			&model.Column{Name: "t", Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Type: mysql.TypeTiny, Value: int64(127)},
			"127", "127",
		},

		{
			&model.Column{
				Name: "tu1", Type: mysql.TypeTiny, Value: uint64(127),
				Flag: model.UnsignedFlag,
			},
			"127", "127",
		},

		{
			&model.Column{
				Name: "tu2", Type: mysql.TypeTiny, Value: uint64(128),
				Flag: model.UnsignedFlag,
			},
			"128", "128",
		},

		{
			&model.Column{
				Name: "tu3", Type: mysql.TypeTiny, Value: "0",
				Flag: model.UnsignedFlag,
			},
			"0", "0",
		},

		{
			&model.Column{
				Name: "tu4", Type: mysql.TypeTiny, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
			"", nil,
		},

		{
			&model.Column{Name: "s", Type: mysql.TypeShort, Value: int64(32767)},
			"32767", "32767",
		},
		{
			&model.Column{
				Name: "su1", Type: mysql.TypeShort, Value: uint64(32767),
				Flag: model.UnsignedFlag,
			},
			"32767", "32767",
		},
		{
			&model.Column{
				Name: "su2", Type: mysql.TypeShort, Value: uint64(32768),
				Flag: model.UnsignedFlag,
			},
			"32768", "32768",
		},
		{
			&model.Column{
				Name: "su3", Type: mysql.TypeShort, Value: "0",
				Flag: model.UnsignedFlag,
			},
			"0", "0",
		},
		{
			&model.Column{
				Name: "su4", Type: mysql.TypeShort, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
			"", nil,
		},

		{
			&model.Column{Name: "m", Type: mysql.TypeInt24, Value: int64(8388607)},
			"8388607", "8388607",
		},
		{
			&model.Column{
				Name: "mu1", Type: mysql.TypeInt24, Value: uint64(8388607),
				Flag: model.UnsignedFlag,
			},
			"8388607", "8388607",
		},
		{
			&model.Column{
				Name: "mu2", Type: mysql.TypeInt24, Value: uint64(8388608),
				Flag: model.UnsignedFlag,
			},
			"8388608", "8388608",
		},
		{
			&model.Column{
				Name: "mu3", Type: mysql.TypeInt24, Value: "0",
				Flag: model.UnsignedFlag,
			},
			"0", "0",
		},
		{
			&model.Column{
				Name: "mu4", Type: mysql.TypeInt24, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
			"", nil,
		},

		{
			&model.Column{Name: "i", Type: mysql.TypeLong, Value: int64(2147483647)},
			"2147483647", "2147483647",
		},
		{
			&model.Column{
				Name: "iu1", Type: mysql.TypeLong, Value: uint64(2147483647),
				Flag: model.UnsignedFlag,
			},
			"2147483647", "2147483647",
		},
		{
			&model.Column{
				Name: "iu2", Type: mysql.TypeLong, Value: uint64(2147483648),
				Flag: model.UnsignedFlag,
			},
			"2147483648", "2147483648",
		},
		{
			&model.Column{
				Name: "iu3", Type: mysql.TypeLong, Value: "0",
				Flag: model.UnsignedFlag,
			},
			"0", "0",
		},
		{
			&model.Column{
				Name: "iu4", Type: mysql.TypeLong, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
			"", nil,
		},

		{
			&model.Column{Name: "bi", Type: mysql.TypeLonglong, Value: int64(9223372036854775807)},
			"9223372036854775807", "9223372036854775807",
		},
		{
			&model.Column{
				Name: "biu1", Type: mysql.TypeLonglong, Value: uint64(9223372036854775807),
				Flag: model.UnsignedFlag,
			},
			"9223372036854775807", "9223372036854775807",
		},
		{
			&model.Column{
				Name: "biu2", Type: mysql.TypeLonglong, Value: uint64(9223372036854775808),
				Flag: model.UnsignedFlag,
			},
			"9223372036854775808", "9223372036854775808",
		},
		{
			&model.Column{
				Name: "biu3", Type: mysql.TypeLonglong, Value: "0",
				Flag: model.UnsignedFlag,
			},
			"0", "0",
		},
		{
			&model.Column{
				Name: "biu4", Type: mysql.TypeLonglong, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
			"", nil,
		},

		{
			&model.Column{Name: "floatT", Type: mysql.TypeFloat, Value: 3.14},
			"3.14", "3.14",
		},
		{
			&model.Column{Name: "doubleT", Type: mysql.TypeDouble, Value: 2.71},
			"2.71", "2.71",
		},
		{
			&model.Column{Name: "decimalT", Type: mysql.TypeNewDecimal, Value: "2333"},
			"2333", "2333",
		},

		{
			&model.Column{
				Name: "float unsigned", Type: mysql.TypeFloat, Value: 3.14,
				Flag: model.UnsignedFlag,
			},
			"3.14", "3.14",
		},
		{
			&model.Column{
				Name: "double unsigned", Type: mysql.TypeDouble, Value: 2.71,
				Flag: model.UnsignedFlag,
			},
			"2.71", "2.71",
		},
		{
			&model.Column{
				Name: "decimal unsigned", Type: mysql.TypeNewDecimal, Value: "2333",
				Flag: model.UnsignedFlag,
			},
			"2333", "2333",
		},

		// for column value type in `[]uint8` and have `BinaryFlag`, expectedEncodedValue is dummy.
		{
			&model.Column{Name: "varcharT", Type: mysql.TypeVarchar, Value: []uint8("测试Varchar")},
			"测试Varchar", "测试Varchar",
		},
		{
			&model.Column{Name: "charT", Type: mysql.TypeString, Value: []uint8("测试String")},
			"测试String", "测试String",
		},
		{
			&model.Column{
				Name: "binaryT", Type: mysql.TypeString, Value: []uint8("测试Binary"),
				Flag: model.BinaryFlag,
			},
			"测试Binary", "测试Binary",
		},
		{
			&model.Column{
				Name: "varbinaryT", Type: mysql.TypeVarchar, Value: []uint8("测试varbinary"),
				Flag: model.BinaryFlag,
			},
			"测试varbinary", "测试varbinary",
		},

		{
			&model.Column{Name: "tinytextT", Type: mysql.TypeTinyBlob, Value: []uint8("测试Tinytext")},
			"测试Tinytext", "测试Tinytext",
		},

		{
			&model.Column{Name: "textT", Type: mysql.TypeBlob, Value: []uint8("测试text")},
			"测试text", "测试text",
		},
		{
			&model.Column{Name: "mediumtextT", Type: mysql.TypeMediumBlob, Value: []uint8("测试mediumtext")},
			"测试mediumtext", "测试mediumtext",
		},
		{
			&model.Column{Name: "longtextT", Type: mysql.TypeLongBlob, Value: []uint8("测试longtext")},
			"测试longtext", "测试longtext",
		},

		{
			&model.Column{
				Name: "tinyblobT", Type: mysql.TypeTinyBlob, Value: []uint8("测试tinyblob"),
				Flag: model.BinaryFlag,
			},
			"测试tinyblob", "测试tinyblob",
		},
		{
			&model.Column{
				Name: "blobT", Type: mysql.TypeBlob, Value: []uint8("测试blob"),
				Flag: model.BinaryFlag,
			},
			"测试blob", "测试blob",
		},
		{
			&model.Column{
				Name: "mediumblobT", Type: mysql.TypeMediumBlob, Value: []uint8("测试mediumblob"),
				Flag: model.BinaryFlag,
			},
			"测试mediumblob", "测试mediumblob",
		},
		{
			&model.Column{
				Name: "longblobT", Type: mysql.TypeLongBlob, Value: []uint8("测试longblob"),
				Flag: model.BinaryFlag,
			},
			"测试longblob", "测试longblob",
		},

		{
			&model.Column{Name: "dateT", Type: mysql.TypeDate, Value: "2020-02-20"},
			"2020-02-20", "2020-02-20",
		},
		{
			&model.Column{Name: "datetimeT", Type: mysql.TypeDatetime, Value: "2020-02-20 02:20:20"},
			"2020-02-20 02:20:20", "2020-02-20 02:20:20",
		},
		{
			&model.Column{Name: "timestampT", Type: mysql.TypeTimestamp, Value: "2020-02-20 10:20:20"},
			"2020-02-20 10:20:20", "2020-02-20 10:20:20",
		},
		{
			&model.Column{Name: "timeT", Type: mysql.TypeDuration, Value: "02:20:20"},
			"02:20:20", "02:20:20",
		},
		{
			&model.Column{Name: "yearT", Type: mysql.TypeYear, Value: "2020", Flag: model.UnsignedFlag},
			"2020", "2020",
		},

		{
			&model.Column{Name: "enumT", Type: mysql.TypeEnum, Value: uint64(1)},
			"1", "1",
		},
		{
			&model.Column{Name: "setT", Type: mysql.TypeSet, Value: uint64(2)},
			"2", uint64(2),
		},
		{
			&model.Column{
				Name: "bitT", Type: mysql.TypeBit, Value: uint64(65),
				Flag: model.UnsignedFlag | model.BinaryFlag,
			},
			"65", uint64(65),
		},
		{
			&model.Column{
				Name: "jsonT", Type: mysql.TypeJSON, Value: "{\"key1\": \"value1\"}",
				Flag: model.BinaryFlag,
			},
			"{\"key1\": \"value1\"}", "{\"key1\": \"value1\"}",
		},
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

func collectAllColumns(groups []*testColumnTuple) []*model.Column {
	columns := make([]*model.Column, 0, len(groups))
	for _, item := range groups {
		columns = append(columns, item.column)
	}
	return columns
}

func collectExpectedDecodedValue(columns []*testColumnTuple) map[string]interface{} {
	result := make(map[string]interface{}, len(columns))
	for _, item := range columns {
		result[item.column.Name] = item.expectedDecodedValue
	}
	return result
}

func newLargeEvent4Test(t *testing.T) (*model.RowChangedEvent, *model.RowChangedEvent, *model.RowChangedEvent) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(
    	t tinyint primary key,
		tu1 tinyint unsigned,
		tu2 tinyint unsigned,
		tu3 tinyint unsigned,
		tu4 tinyint unsigned,
		s smallint,
		su1 smallint unsigned,
		su2 smallint unsigned,
		su3 smallint unsigned,
		su4 smallint unsigned,
		m mediumint,
		mu1 mediumint unsigned,
		mu2 mediumint unsigned,
		mu3 mediumint unsigned,
		mu4 mediumint unsigned,
		i int,
		iu1 int unsigned,
		iu2 int unsigned,
		iu3 int unsigned,
		iu4 int unsigned,
		bi bigint,
		biu1 bigint unsigned,
		biu2 bigint unsigned,
		biu3 bigint unsigned,
		biu4 bigint unsigned,
		floatT float,
		doubleT double,
	 	decimalT decimal,
	 	floatTu float unsigned,
		doubleTu double unsigned,
	 	decimalTu decimal unsigned,
	 	varcharT varchar(255),
	 	charT char,
	 	binaryT binary,
	 	varbinaryT varbinary(255),
	 	tinytextT tinytext,
	 	textT text,
	 	mediumtextT mediumtext,
	 	longtextT longtext,
	 	tinyblobT tinyblob,
	 	blobT blob,
	 	mediumblobT mediumblob,
	 	longblobT longblob,
	 	dateT date,
	 	datetimeT datetime,
	 	timestampT timestamp,
	 	timeT time,
	 	yearT year,
	 	enumT enum('a', 'b', 'c'),
	 	setT set('a', 'b', 'c'),
	 	bitT bit(4),
	 	jsonT json)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)
	_, _, colInfo := tableInfo.GetRowColInfos()

	testColumns := collectAllColumns(testColumnsTable)

	insert := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "test",
			Table:  "t",
		},
		TableInfo:  tableInfo,
		Columns:    testColumns,
		PreColumns: nil,
		ColInfos:   colInfo,
	}

	update := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		TableInfo:  tableInfo,
		Columns:    testColumns,
		PreColumns: testColumns,
		ColInfos:   colInfo,
	}

	deleteE := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		TableInfo:  tableInfo,
		Columns:    nil,
		PreColumns: testColumns,
		ColInfos:   colInfo,
	}
	return insert, update, deleteE
}
