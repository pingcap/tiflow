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
	"testing"

	mm "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/internal"
)

type testColumnTuple struct {
	column              *model.Column
	expectedMySQLType   string
	expectedJavaSQLType internal.JavaSQLType
=======
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
)

type testColumnTuple struct {
	column *model.Column
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go

	// expectedEncodedValue is expected by encoding
	expectedEncodedValue string

	// expectedDecodedValue is expected by decoding
	expectedDecodedValue interface{}
}

var (
	testColumnsTable = []*testColumnTuple{
		{
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			&model.Column{Name: "tinyint", Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Type: mysql.TypeTiny, Value: int64(127)},
			"tinyint", internal.JavaSQLTypeTINYINT, "127", "127",
=======
			&model.Column{Name: "t", Flag: model.HandleKeyFlag | model.PrimaryKeyFlag, Type: mysql.TypeTiny, Value: int64(127)},
			"127", "127",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},

		{
			&model.Column{
				Name: "tu1", Type: mysql.TypeTiny, Value: uint64(127),
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"tinyint unsigned", internal.JavaSQLTypeTINYINT, "127", "127",
=======
			"127", "127",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},

		{
			&model.Column{
				Name: "tu2", Type: mysql.TypeTiny, Value: uint64(128),
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"tinyint unsigned", internal.JavaSQLTypeSMALLINT, "128", "128",
=======
			"128", "128",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},

		{
			&model.Column{
				Name: "tu3", Type: mysql.TypeTiny, Value: "0",
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"tinyint unsigned", internal.JavaSQLTypeTINYINT, "0", "0",
=======
			"0", "0",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},

		{
			&model.Column{
				Name: "tu4", Type: mysql.TypeTiny, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"tinyint unsigned", internal.JavaSQLTypeTINYINT, "", nil,
		},

		{
			&model.Column{Name: "smallint", Type: mysql.TypeShort, Value: int64(32767)},
			"smallint", internal.JavaSQLTypeSMALLINT, "32767", "32767",
=======
			"", nil,
		},

		{
			&model.Column{Name: "s", Type: mysql.TypeShort, Value: int64(32767)},
			"32767", "32767",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "su1", Type: mysql.TypeShort, Value: uint64(32767),
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"smallint unsigned", internal.JavaSQLTypeSMALLINT, "32767", "32767",
=======
			"32767", "32767",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "su2", Type: mysql.TypeShort, Value: uint64(32768),
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"smallint unsigned", internal.JavaSQLTypeINTEGER, "32768", "32768",
=======
			"32768", "32768",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "su3", Type: mysql.TypeShort, Value: "0",
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"smallint unsigned", internal.JavaSQLTypeSMALLINT, "0", "0",
=======
			"0", "0",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "su4", Type: mysql.TypeShort, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"smallint unsigned", internal.JavaSQLTypeSMALLINT, "", nil,
		},

		{
			&model.Column{Name: "mediumint", Type: mysql.TypeInt24, Value: int64(8388607)},
			"mediumint", internal.JavaSQLTypeINTEGER, "8388607", "8388607",
=======
			"", nil,
		},

		{
			&model.Column{Name: "m", Type: mysql.TypeInt24, Value: int64(8388607)},
			"8388607", "8388607",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "mu1", Type: mysql.TypeInt24, Value: uint64(8388607),
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"mediumint unsigned", internal.JavaSQLTypeINTEGER, "8388607", "8388607",
=======
			"8388607", "8388607",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "mu2", Type: mysql.TypeInt24, Value: uint64(8388608),
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"mediumint unsigned", internal.JavaSQLTypeINTEGER, "8388608", "8388608",
=======
			"8388608", "8388608",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "mu3", Type: mysql.TypeInt24, Value: "0",
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"mediumint unsigned", internal.JavaSQLTypeINTEGER, "0", "0",
=======
			"0", "0",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "mu4", Type: mysql.TypeInt24, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"mediumint unsigned", internal.JavaSQLTypeINTEGER, "", nil,
		},

		{
			&model.Column{Name: "int", Type: mysql.TypeLong, Value: int64(2147483647)},
			"int", internal.JavaSQLTypeINTEGER, "2147483647", "2147483647",
=======
			"", nil,
		},

		{
			&model.Column{Name: "i", Type: mysql.TypeLong, Value: int64(2147483647)},
			"2147483647", "2147483647",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "iu1", Type: mysql.TypeLong, Value: uint64(2147483647),
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"int unsigned", internal.JavaSQLTypeINTEGER, "2147483647", "2147483647",
=======
			"2147483647", "2147483647",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "iu2", Type: mysql.TypeLong, Value: uint64(2147483648),
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"int unsigned", internal.JavaSQLTypeBIGINT, "2147483648", "2147483648",
=======
			"2147483648", "2147483648",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "iu3", Type: mysql.TypeLong, Value: "0",
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"int unsigned", internal.JavaSQLTypeINTEGER, "0", "0",
=======
			"0", "0",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "iu4", Type: mysql.TypeLong, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"int unsigned", internal.JavaSQLTypeINTEGER, "", nil,
		},

		{
			&model.Column{Name: "bigint", Type: mysql.TypeLonglong, Value: int64(9223372036854775807)},
			"bigint", internal.JavaSQLTypeBIGINT, "9223372036854775807", "9223372036854775807",
=======
			"", nil,
		},

		{
			&model.Column{Name: "bi", Type: mysql.TypeLonglong, Value: int64(9223372036854775807)},
			"9223372036854775807", "9223372036854775807",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "biu1", Type: mysql.TypeLonglong, Value: uint64(9223372036854775807),
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"bigint unsigned", internal.JavaSQLTypeBIGINT, "9223372036854775807", "9223372036854775807",
=======
			"9223372036854775807", "9223372036854775807",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "biu2", Type: mysql.TypeLonglong, Value: uint64(9223372036854775808),
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"bigint unsigned", internal.JavaSQLTypeDECIMAL, "9223372036854775808", "9223372036854775808",
=======
			"9223372036854775808", "9223372036854775808",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "biu3", Type: mysql.TypeLonglong, Value: "0",
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"bigint unsigned", internal.JavaSQLTypeBIGINT, "0", "0",
=======
			"0", "0",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "biu4", Type: mysql.TypeLonglong, Value: nil,
				Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"bigint unsigned", internal.JavaSQLTypeBIGINT, "", nil,
		},

		{
			&model.Column{Name: "float", Type: mysql.TypeFloat, Value: 3.14},
			"float", internal.JavaSQLTypeREAL, "3.14", "3.14",
		},
		{
			&model.Column{Name: "double", Type: mysql.TypeDouble, Value: 2.71},
			"double", internal.JavaSQLTypeDOUBLE, "2.71", "2.71",
		},
		{
			&model.Column{Name: "decimal", Type: mysql.TypeNewDecimal, Value: "2333"},
			"decimal", internal.JavaSQLTypeDECIMAL, "2333", "2333",
=======
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
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},

		{
			&model.Column{
				Name: "float unsigned", Type: mysql.TypeFloat, Value: 3.14,
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"float unsigned", internal.JavaSQLTypeREAL, "3.14", "3.14",
=======
			"3.14", "3.14",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "double unsigned", Type: mysql.TypeDouble, Value: 2.71,
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"double unsigned", internal.JavaSQLTypeDOUBLE, "2.71", "2.71",
=======
			"2.71", "2.71",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "decimal unsigned", Type: mysql.TypeNewDecimal, Value: "2333",
				Flag: model.UnsignedFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"decimal unsigned", internal.JavaSQLTypeDECIMAL, "2333", "2333",
=======
			"2333", "2333",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},

		// for column value type in `[]uint8` and have `BinaryFlag`, expectedEncodedValue is dummy.
		{
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			&model.Column{Name: "varchar", Type: mysql.TypeVarchar, Value: []uint8("测试Varchar")},
			"varchar", internal.JavaSQLTypeVARCHAR, "测试Varchar", "测试Varchar",
		},
		{
			&model.Column{Name: "char", Type: mysql.TypeString, Value: []uint8("测试String")},
			"char", internal.JavaSQLTypeCHAR, "测试String", "测试String",
=======
			&model.Column{Name: "varcharT", Type: mysql.TypeVarchar, Value: []uint8("测试Varchar")},
			"测试Varchar", "测试Varchar",
		},
		{
			&model.Column{Name: "charT", Type: mysql.TypeString, Value: []uint8("测试String")},
			"测试String", "测试String",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "binaryT", Type: mysql.TypeString, Value: []uint8("测试Binary"),
				Flag: model.BinaryFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"binary", internal.JavaSQLTypeBLOB, "测试Binary", "测试Binary",
=======
			"测试Binary", "测试Binary",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "varbinaryT", Type: mysql.TypeVarchar, Value: []uint8("测试varbinary"),
				Flag: model.BinaryFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"varbinary", internal.JavaSQLTypeBLOB, "测试varbinary", "测试varbinary",
		},

		{
			&model.Column{Name: "tinytext", Type: mysql.TypeTinyBlob, Value: []uint8("测试Tinytext")},
			"tinytext", internal.JavaSQLTypeCLOB, "测试Tinytext", "测试Tinytext",
=======
			"测试varbinary", "测试varbinary",
		},

		{
			&model.Column{Name: "tinytextT", Type: mysql.TypeTinyBlob, Value: []uint8("测试Tinytext")},
			"测试Tinytext", "测试Tinytext",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			&model.Column{Name: "text", Type: mysql.TypeBlob, Value: []uint8("测试text")},
			"text", internal.JavaSQLTypeCLOB, "测试text", "测试text",
		},
		{
			&model.Column{
				Name: "mediumtext", Type: mysql.TypeMediumBlob,
				Value: []uint8("测试mediumtext"),
			},
			"mediumtext", internal.JavaSQLTypeCLOB, "测试mediumtext", "测试mediumtext",
		},
		{
			&model.Column{Name: "longtext", Type: mysql.TypeLongBlob, Value: []uint8("测试longtext")},
			"longtext", internal.JavaSQLTypeCLOB, "测试longtext", "测试longtext",
=======
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
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},

		{
			&model.Column{
				Name: "tinyblobT", Type: mysql.TypeTinyBlob, Value: []uint8("测试tinyblob"),
				Flag: model.BinaryFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"tinyblob", internal.JavaSQLTypeBLOB, "测试tinyblob", "测试tinyblob",
=======
			"测试tinyblob", "测试tinyblob",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "blobT", Type: mysql.TypeBlob, Value: []uint8("测试blob"),
				Flag: model.BinaryFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"blob", internal.JavaSQLTypeBLOB, "测试blob", "测试blob",
=======
			"测试blob", "测试blob",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "mediumblobT", Type: mysql.TypeMediumBlob, Value: []uint8("测试mediumblob"),
				Flag: model.BinaryFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"mediumblob", internal.JavaSQLTypeBLOB, "测试mediumblob", "测试mediumblob",
=======
			"测试mediumblob", "测试mediumblob",
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "longblobT", Type: mysql.TypeLongBlob, Value: []uint8("测试longblob"),
				Flag: model.BinaryFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"longblob", internal.JavaSQLTypeBLOB, "测试longblob", "测试longblob",
		},

		{
			&model.Column{Name: "date", Type: mysql.TypeDate, Value: "2020-02-20"},
			"date", internal.JavaSQLTypeDATE, "2020-02-20", "2020-02-20",
		},
		{
			&model.Column{Name: "datetime", Type: mysql.TypeDatetime, Value: "2020-02-20 02:20:20"},
			"datetime", internal.JavaSQLTypeTIMESTAMP, "2020-02-20 02:20:20", "2020-02-20 02:20:20",
		},
		{
			&model.Column{Name: "timestamp", Type: mysql.TypeTimestamp, Value: "2020-02-20 10:20:20"},
			"timestamp", internal.JavaSQLTypeTIMESTAMP, "2020-02-20 10:20:20", "2020-02-20 10:20:20",
		},
		{
			&model.Column{Name: "time", Type: mysql.TypeDuration, Value: "02:20:20"},
			"time", internal.JavaSQLTypeTIME, "02:20:20", "02:20:20",
		},
		{
			&model.Column{Name: "year", Type: mysql.TypeYear, Value: "2020", Flag: model.UnsignedFlag},
			"year", internal.JavaSQLTypeVARCHAR, "2020", "2020",
		},

		{
			&model.Column{Name: "enum", Type: mysql.TypeEnum, Value: uint64(1)},
			"enum", internal.JavaSQLTypeINTEGER, "1", "1",
		},
		{
			&model.Column{Name: "set", Type: mysql.TypeSet, Value: uint64(3)},
			"set", internal.JavaSQLTypeBIT, "3", uint64(3),
=======
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
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "bitT", Type: mysql.TypeBit, Value: uint64(65),
				Flag: model.UnsignedFlag | model.BinaryFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"bit", internal.JavaSQLTypeBIT, "65", uint64(65),
=======
			"65", uint64(65),
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
		},
		{
			&model.Column{
				Name: "jsonT", Type: mysql.TypeJSON, Value: "{\"key1\": \"value1\"}",
				Flag: model.BinaryFlag,
			},
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
			"json", internal.JavaSQLTypeVARCHAR, "{\"key1\": \"value1\"}", "{\"key1\": \"value1\"}",
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
				},
				{
					CommitTs: 2,
					Table:    &model.TableName{Schema: "a", Table: "b"},
					Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
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

	testCaseInsert = &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		Columns:    testColumns,
		PreColumns: nil,
	}

	testCaseUpdate = &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		Columns:    testColumns,
		PreColumns: testColumns,
	}

	testCaseDelete = &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		Columns:    nil,
		PreColumns: testColumns,
	}

=======
			"{\"key1\": \"value1\"}", "{\"key1\": \"value1\"}",
		},
	}

>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
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
<<<<<<< HEAD:cdc/sink/codec/canal/canal_test_util.go
	result := make([]*model.Column, 0, len(groups))
	for _, item := range groups {
		result = append(result, item.column)
	}
	return result
=======
	columns := make([]*model.Column, 0, len(groups))
	for _, item := range groups {
		columns = append(columns, item.column)
	}
	return columns
>>>>>>> 5921050d90 (codec(ticdc): canal-json decouple get value from java type and refactor unit test (#10123)):pkg/sink/codec/canal/canal_test_util.go
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
