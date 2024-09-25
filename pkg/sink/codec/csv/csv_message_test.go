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

package csv

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

type csvTestColumnTuple struct {
	col                  model.Column
	colInfo              rowcodec.ColInfo
	want                 interface{}
	BinaryEncodingMethod string
}

var csvTestColumnsGroup = [][]*csvTestColumnTuple{
	{
		{
			model.Column{Name: "tiny", Value: int64(1), Type: mysql.TypeTiny},
			rowcodec.ColInfo{
				ID:            1,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeTiny),
			},
			int64(1),
			config.BinaryEncodingBase64,
		},
		{
			model.Column{Name: "short", Value: int64(1), Type: mysql.TypeShort},
			rowcodec.ColInfo{
				ID:            2,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeShort),
			},
			int64(1),
			config.BinaryEncodingBase64,
		},
		{
			model.Column{Name: "int24", Value: int64(1), Type: mysql.TypeInt24},
			rowcodec.ColInfo{
				ID:            3,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeInt24),
			},
			int64(1),
			config.BinaryEncodingBase64,
		},
		{
			model.Column{Name: "long", Value: int64(1), Type: mysql.TypeLong},
			rowcodec.ColInfo{
				ID:            4,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeLong),
			},
			int64(1),
			config.BinaryEncodingBase64,
		},
		{
			model.Column{Name: "longlong", Value: int64(1), Type: mysql.TypeLonglong},
			rowcodec.ColInfo{
				ID:            5,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeLonglong),
			},
			int64(1),
			config.BinaryEncodingBase64,
		},
		{
			model.Column{
				Name:  "tinyunsigned",
				Value: uint64(1),
				Type:  mysql.TypeTiny,
				Flag:  model.UnsignedFlag,
			},
			rowcodec.ColInfo{
				ID:            6,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setFlag(types.NewFieldType(mysql.TypeTiny), uint(model.UnsignedFlag)),
			},
			uint64(1),
			config.BinaryEncodingBase64,
		},
		{
			model.Column{
				Name:  "shortunsigned",
				Value: uint64(1),
				Type:  mysql.TypeShort,
				Flag:  model.UnsignedFlag,
			},
			rowcodec.ColInfo{
				ID:            7,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setFlag(types.NewFieldType(mysql.TypeShort), uint(model.UnsignedFlag)),
			},
			uint64(1),
			config.BinaryEncodingBase64,
		},
		{
			model.Column{
				Name:  "int24unsigned",
				Value: uint64(1),
				Type:  mysql.TypeInt24,
				Flag:  model.UnsignedFlag,
			},
			rowcodec.ColInfo{
				ID:            8,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setFlag(types.NewFieldType(mysql.TypeInt24), uint(model.UnsignedFlag)),
			},
			uint64(1),
			config.BinaryEncodingBase64,
		},
		{
			model.Column{
				Name:  "longunsigned",
				Value: uint64(1),
				Type:  mysql.TypeLong,
				Flag:  model.UnsignedFlag,
			},
			rowcodec.ColInfo{
				ID:            9,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setFlag(types.NewFieldType(mysql.TypeLong), uint(model.UnsignedFlag)),
			},
			uint64(1),
			config.BinaryEncodingBase64,
		},
		{
			model.Column{
				Name:  "longlongunsigned",
				Value: uint64(1),
				Type:  mysql.TypeLonglong,
				Flag:  model.UnsignedFlag,
			},
			rowcodec.ColInfo{
				ID:            10,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft: setFlag(
					types.NewFieldType(mysql.TypeLonglong),
					uint(model.UnsignedFlag),
				),
			},
			uint64(1),
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			model.Column{Name: "float", Value: float64(3.14), Type: mysql.TypeFloat},
			rowcodec.ColInfo{
				ID:            11,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeFloat),
			},
			float64(3.14),
			config.BinaryEncodingBase64,
		},
		{
			model.Column{Name: "double", Value: float64(3.14), Type: mysql.TypeDouble},
			rowcodec.ColInfo{
				ID:            12,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeDouble),
			},
			float64(3.14),
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			model.Column{Name: "bit", Value: uint64(683), Type: mysql.TypeBit},
			rowcodec.ColInfo{
				ID:            13,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeBit),
			},
			uint64(683),
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			model.Column{Name: "decimal", Value: "129012.1230000", Type: mysql.TypeNewDecimal},
			rowcodec.ColInfo{
				ID:            14,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeNewDecimal),
			},
			"129012.1230000",
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			model.Column{Name: "tinytext", Value: []byte("hello world"), Type: mysql.TypeTinyBlob},
			rowcodec.ColInfo{
				ID:            15,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeBlob),
			},
			"hello world",
			config.BinaryEncodingBase64,
		},
		{
			model.Column{Name: "mediumtext", Value: []byte("hello world"), Type: mysql.TypeMediumBlob},
			rowcodec.ColInfo{
				ID:            16,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeMediumBlob),
			},
			"hello world",
			config.BinaryEncodingBase64,
		},
		{
			model.Column{Name: "text", Value: []byte("hello world"), Type: mysql.TypeBlob},
			rowcodec.ColInfo{
				ID:            17,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeBlob),
			},
			"hello world",
			config.BinaryEncodingBase64,
		},
		{
			model.Column{Name: "longtext", Value: []byte("hello world"), Type: mysql.TypeLongBlob},
			rowcodec.ColInfo{
				ID:            18,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeLongBlob),
			},
			"hello world",
			config.BinaryEncodingBase64,
		},
		{
			model.Column{Name: "varchar", Value: []byte("hello world"), Type: mysql.TypeVarchar},
			rowcodec.ColInfo{
				ID:            19,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeVarchar),
			},
			"hello world",
			config.BinaryEncodingBase64,
		},
		{
			model.Column{Name: "varstring", Value: []byte("hello world"), Type: mysql.TypeVarString},
			rowcodec.ColInfo{
				ID:            20,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeVarString),
			},
			"hello world",
			config.BinaryEncodingBase64,
		},
		{
			model.Column{Name: "string", Value: []byte("hello world"), Type: mysql.TypeString},
			rowcodec.ColInfo{
				ID:            21,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeString),
			},
			"hello world",
			config.BinaryEncodingBase64,
		},
		{
			model.Column{Name: "json", Value: `{"key": "value"}`, Type: mysql.TypeJSON},
			rowcodec.ColInfo{
				ID:            31,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeJSON),
			},
			`{"key": "value"}`,
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			model.Column{
				Name:  "tinyblob",
				Value: []byte("hello world"),
				Type:  mysql.TypeTinyBlob,
				Flag:  model.BinaryFlag,
			},
			rowcodec.ColInfo{
				ID:            22,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setBinChsClnFlag(types.NewFieldType(mysql.TypeTinyBlob)),
			},
			"aGVsbG8gd29ybGQ=",
			config.BinaryEncodingBase64,
		},
		{
			model.Column{
				Name:  "mediumblob",
				Value: []byte("hello world"),
				Type:  mysql.TypeMediumBlob,
				Flag:  model.BinaryFlag,
			},
			rowcodec.ColInfo{
				ID:            23,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setBinChsClnFlag(types.NewFieldType(mysql.TypeMediumBlob)),
			},
			"aGVsbG8gd29ybGQ=",
			config.BinaryEncodingBase64,
		},
		{
			model.Column{
				Name:  "blob",
				Value: []byte("hello world"),
				Type:  mysql.TypeBlob,
				Flag:  model.BinaryFlag,
			},
			rowcodec.ColInfo{
				ID:            24,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setBinChsClnFlag(types.NewFieldType(mysql.TypeBlob)),
			},
			"aGVsbG8gd29ybGQ=",
			config.BinaryEncodingBase64,
		},
		{
			model.Column{
				Name:  "longblob",
				Value: []byte("hello world"),
				Type:  mysql.TypeLongBlob,
				Flag:  model.BinaryFlag,
			},
			rowcodec.ColInfo{
				ID:            25,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setBinChsClnFlag(types.NewFieldType(mysql.TypeLongBlob)),
			},
			"aGVsbG8gd29ybGQ=",
			config.BinaryEncodingBase64,
		},
		{
			model.Column{
				Name:  "varbinary",
				Value: []byte("hello world"),
				Type:  mysql.TypeVarchar,
				Flag:  model.BinaryFlag,
			},
			rowcodec.ColInfo{
				ID:            26,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setBinChsClnFlag(types.NewFieldType(mysql.TypeVarchar)),
			},
			"aGVsbG8gd29ybGQ=",
			config.BinaryEncodingBase64,
		},
		{
			model.Column{
				Name:  "varbinary1",
				Value: []byte("hello world"),
				Type:  mysql.TypeVarString,
				Flag:  model.BinaryFlag,
			},
			rowcodec.ColInfo{
				ID:            27,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setBinChsClnFlag(types.NewFieldType(mysql.TypeVarString)),
			},
			"aGVsbG8gd29ybGQ=",
			config.BinaryEncodingBase64,
		},
		{
			model.Column{
				Name:  "binary",
				Value: []byte("hello world"),
				Type:  mysql.TypeString,
				Flag:  model.BinaryFlag,
			},
			rowcodec.ColInfo{
				ID:            28,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setBinChsClnFlag(types.NewFieldType(mysql.TypeString)),
			},
			"aGVsbG8gd29ybGQ=",
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			model.Column{
				Name:  "tinyblob",
				Value: []byte("hello world"),
				Type:  mysql.TypeTinyBlob,
				Flag:  model.BinaryFlag,
			},
			rowcodec.ColInfo{
				ID:            22,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setBinChsClnFlag(types.NewFieldType(mysql.TypeTinyBlob)),
			},
			"68656c6c6f20776f726c64",
			config.BinaryEncodingHex,
		},
		{
			model.Column{
				Name:  "mediumblob",
				Value: []byte("hello world"),
				Type:  mysql.TypeMediumBlob,
				Flag:  model.BinaryFlag,
			},
			rowcodec.ColInfo{
				ID:            23,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setBinChsClnFlag(types.NewFieldType(mysql.TypeMediumBlob)),
			},
			"68656c6c6f20776f726c64",
			config.BinaryEncodingHex,
		},
		{
			model.Column{
				Name:  "blob",
				Value: []byte("hello world"),
				Type:  mysql.TypeBlob,
				Flag:  model.BinaryFlag,
			},
			rowcodec.ColInfo{
				ID:            24,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setBinChsClnFlag(types.NewFieldType(mysql.TypeBlob)),
			},
			"68656c6c6f20776f726c64",
			config.BinaryEncodingHex,
		},
		{
			model.Column{
				Name:  "longblob",
				Value: []byte("hello world"),
				Type:  mysql.TypeLongBlob,
				Flag:  model.BinaryFlag,
			},
			rowcodec.ColInfo{
				ID:            25,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setBinChsClnFlag(types.NewFieldType(mysql.TypeLongBlob)),
			},
			"68656c6c6f20776f726c64",
			config.BinaryEncodingHex,
		},
		{
			model.Column{
				Name:  "varbinary",
				Value: []byte("hello world"),
				Type:  mysql.TypeVarchar,
				Flag:  model.BinaryFlag,
			},
			rowcodec.ColInfo{
				ID:            26,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setBinChsClnFlag(types.NewFieldType(mysql.TypeVarchar)),
			},
			"68656c6c6f20776f726c64",
			config.BinaryEncodingHex,
		},
		{
			model.Column{
				Name:  "varbinary1",
				Value: []byte("hello world"),
				Type:  mysql.TypeVarString,
				Flag:  model.BinaryFlag,
			},
			rowcodec.ColInfo{
				ID:            27,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setBinChsClnFlag(types.NewFieldType(mysql.TypeVarString)),
			},
			"68656c6c6f20776f726c64",
			config.BinaryEncodingHex,
		},
		{
			model.Column{
				Name:  "binary",
				Value: []byte("hello world"),
				Type:  mysql.TypeString,
				Flag:  model.BinaryFlag,
			},
			rowcodec.ColInfo{
				ID:            28,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setBinChsClnFlag(types.NewFieldType(mysql.TypeString)),
			},
			"68656c6c6f20776f726c64",
			config.BinaryEncodingHex,
		},
	},
	{
		{
			model.Column{Name: "enum", Value: uint64(1), Type: mysql.TypeEnum},
			rowcodec.ColInfo{
				ID:            29,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setElems(types.NewFieldType(mysql.TypeEnum), []string{"a,", "b"}),
			},
			"a,",
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			model.Column{Name: "set", Value: uint64(9), Type: mysql.TypeSet},
			rowcodec.ColInfo{
				ID:            30,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            setElems(types.NewFieldType(mysql.TypeSet), []string{"a", "b", "c", "d"}),
			},
			"a,d",
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			model.Column{Name: "date", Value: "2000-01-01", Type: mysql.TypeDate},
			rowcodec.ColInfo{
				ID:            32,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeDate),
			},
			"2000-01-01",
			config.BinaryEncodingBase64,
		},
		{
			model.Column{Name: "datetime", Value: "2015-12-20 23:58:58", Type: mysql.TypeDatetime},
			rowcodec.ColInfo{
				ID:            33,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeDatetime),
			},
			"2015-12-20 23:58:58",
			config.BinaryEncodingBase64,
		},
		{
			model.Column{Name: "timestamp", Value: "1973-12-30 15:30:00", Type: mysql.TypeTimestamp},
			rowcodec.ColInfo{
				ID:            34,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeTimestamp),
			},
			"1973-12-30 15:30:00",
			config.BinaryEncodingBase64,
		},
		{
			model.Column{Name: "time", Value: "23:59:59", Type: mysql.TypeDuration},
			rowcodec.ColInfo{
				ID:            35,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeDuration),
			},
			"23:59:59",
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			model.Column{Name: "year", Value: int64(1970), Type: mysql.TypeYear},
			rowcodec.ColInfo{
				ID:            36,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeYear),
			},
			int64(1970),
			config.BinaryEncodingBase64,
		},
	},
	{
		{
			model.Column{Name: "vectorfloat32", Value: util.Must(types.ParseVectorFloat32("[1,2,3,4,5]")), Type: mysql.TypeTiDBVectorFloat32},
			rowcodec.ColInfo{
				ID:            37,
				IsPKHandle:    false,
				VirtualGenCol: false,
				Ft:            types.NewFieldType(mysql.TypeTiDBVectorFloat32),
			},
			"[1,2,3,4,5]",
			config.BinaryEncodingBase64,
		},
	},
}

func setBinChsClnFlag(ft *types.FieldType) *types.FieldType {
	types.SetBinChsClnFlag(ft)
	return ft
}

//nolint:unparam
func setFlag(ft *types.FieldType, flag uint) *types.FieldType {
	ft.SetFlag(flag)
	return ft
}

func setElems(ft *types.FieldType, elems []string) *types.FieldType {
	ft.SetElems(elems)
	return ft
}

func TestFormatWithQuotes(t *testing.T) {
	config := &common.Config{
		Quote: "\"",
	}

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "string does not contain quote mark",
			input:    "a,b,c",
			expected: `"a,b,c"`,
		},
		{
			name:     "string contains quote mark",
			input:    `"a,b,c`,
			expected: `"""a,b,c"`,
		},
		{
			name:     "empty string",
			input:    "",
			expected: `""`,
		},
	}
	for _, tc := range testCases {
		csvMessage := newCSVMessage(config)
		strBuilder := new(strings.Builder)
		csvMessage.formatWithQuotes(tc.input, strBuilder)
		require.Equal(t, tc.expected, strBuilder.String(), tc.name)
	}
}

func TestFormatWithEscape(t *testing.T) {
	testCases := []struct {
		name     string
		config   *common.Config
		input    string
		expected string
	}{
		{
			name:     "string does not contain CR/LF/backslash/delimiter",
			config:   &common.Config{Delimiter: ","},
			input:    "abcdef",
			expected: "abcdef",
		},
		{
			name:     "string contains CRLF",
			config:   &common.Config{Delimiter: ","},
			input:    "abc\r\ndef",
			expected: "abc\\r\\ndef",
		},
		{
			name:     "string contains backslash",
			config:   &common.Config{Delimiter: ","},
			input:    `abc\def`,
			expected: `abc\\def`,
		},
		{
			name:     "string contains a single character delimiter",
			config:   &common.Config{Delimiter: ","},
			input:    "abc,def",
			expected: `abc\,def`,
		},
		{
			name:     "string contains multi-character delimiter",
			config:   &common.Config{Delimiter: "***"},
			input:    "abc***def",
			expected: `abc\*\*\*def`,
		},
		{
			name:     "string contains CR, LF, backslash and delimiter",
			config:   &common.Config{Delimiter: "?"},
			input:    `abc\def?ghi\r\n`,
			expected: `abc\\def\?ghi\\r\\n`,
		},
	}

	for _, tc := range testCases {
		csvMessage := newCSVMessage(tc.config)
		strBuilder := new(strings.Builder)
		csvMessage.formatWithEscapes(tc.input, strBuilder)
		require.Equal(t, tc.expected, strBuilder.String())
	}
}

func TestCSVMessageEncode(t *testing.T) {
	type fields struct {
		config     *common.Config
		opType     operation
		tableName  string
		schemaName string
		commitTs   uint64
		preColumns []any
		columns    []any
		HandleKey  kv.Handle
	}
	testCases := []struct {
		name   string
		fields fields
		want   []byte
	}{
		{
			name: "csv encode with typical configurations",
			fields: fields{
				config: &common.Config{
					Delimiter:       ",",
					Quote:           "\"",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: true,
				},
				opType:     operationInsert,
				tableName:  "table1",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{123, "hello,world"},
			},
			want: []byte("\"I\",\"table1\",\"test\",435661838416609281,123,\"hello,world\"\n"),
		},
		{
			name: "csv encode values containing single-character delimter string, without quote mark",
			fields: fields{
				config: &common.Config{
					Delimiter:       "!",
					Quote:           "",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: true,
				},
				opType:     operationUpdate,
				tableName:  "table2",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a!b!c", "def"},
			},
			want: []byte(`U!table2!test!435661838416609281!a\!b\!c!def` + "\n"),
		},
		{
			name: "csv encode values containing single-character delimter string, without quote mark, update with old value",
			fields: fields{
				config: &common.Config{
					Delimiter:       "!",
					Quote:           "",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: true,
					OutputOldValue:  true,
					OutputHandleKey: true,
				},
				opType:     operationUpdate,
				tableName:  "table2",
				schemaName: "test",
				commitTs:   435661838416609281,
				preColumns: []any{"a!b!c", "abc"},
				columns:    []any{"a!b!c", "def"},
				HandleKey:  kv.IntHandle(1),
			},
			want: []byte(`D!table2!test!435661838416609281!true!1!a\!b\!c!abc` + "\n" +
				`I!table2!test!435661838416609281!true!1!a\!b\!c!def` + "\n"),
		},
		{
			name: "csv encode values containing single-character delimter string, without quote mark, update with old value",
			fields: fields{
				config: &common.Config{
					Delimiter:       "!",
					Quote:           "",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: true,
					OutputOldValue:  true,
				},
				opType:     operationInsert,
				tableName:  "table2",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a!b!c", "def"},
			},
			want: []byte(`I!table2!test!435661838416609281!false!a\!b\!c!def` + "\n"),
		},
		{
			name: "csv encode values containing single-character delimter string, with quote mark",
			fields: fields{
				config: &common.Config{
					Delimiter:       ",",
					Quote:           "\"",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: true,
				},
				opType:     operationUpdate,
				tableName:  "table3",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a,b,c", "def", "2022-08-31 17:07:00"},
			},
			want: []byte(`"U","table3","test",435661838416609281,"a,b,c","def","2022-08-31 17:07:00"` + "\n"),
		},
		{
			name: "csv encode values containing multi-character delimiter string, without quote mark",
			fields: fields{
				config: &common.Config{
					Delimiter:       "[*]",
					Quote:           "",
					Terminator:      "\r\n",
					NullString:      "\\N",
					IncludeCommitTs: false,
				},
				opType:     operationDelete,
				tableName:  "table4",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a[*]b[*]c", "def"},
			},
			want: []byte(`D[*]table4[*]test[*]a\[\*\]b\[\*\]c[*]def` + "\r\n"),
		},
		{
			name: "csv encode with values containing multi-character delimiter string, with quote mark",
			fields: fields{
				config: &common.Config{
					Delimiter:       "[*]",
					Quote:           "'",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: false,
				},
				opType:     operationInsert,
				tableName:  "table5",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a[*]b[*]c", "def", nil, 12345.678},
			},
			want: []byte(`'I'[*]'table5'[*]'test'[*]'a[*]b[*]c'[*]'def'[*]\N[*]12345.678` + "\n"),
		},
		{
			name: "csv encode with values containing backslash and LF, without quote mark",
			fields: fields{
				config: &common.Config{
					Delimiter:       ",",
					Quote:           "",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: true,
				},
				opType:     operationUpdate,
				tableName:  "table6",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a\\b\\c", "def\n"},
			},
			want: []byte(`U,table6,test,435661838416609281,a\\b\\c,def\n` + "\n"),
		},
		{
			name: "csv encode with values containing backslash and CR, with quote mark",
			fields: fields{
				config: &common.Config{
					Delimiter:       ",",
					Quote:           "'",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: false,
				},
				opType:     operationInsert,
				tableName:  "table7",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"\\", "\\\r", "\\\\"},
			},
			want: []byte("'I','table7','test','\\','\\\r','\\\\'" + "\n"),
		},
		{
			name: "csv encode with values containing unicode characters",
			fields: fields{
				config: &common.Config{
					Delimiter:       "\t",
					Quote:           "\"",
					Terminator:      "\n",
					NullString:      "\\N",
					IncludeCommitTs: true,
				},
				opType:     operationDelete,
				tableName:  "table8",
				schemaName: "test",
				commitTs:   435661838416609281,
				columns:    []any{"a\tb", 123.456, "你好，世界"},
			},
			want: []byte("\"D\"\t\"table8\"\t\"test\"\t435661838416609281\t\"a\tb\"\t123.456\t\"你好，世界\"\n"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := &csvMessage{
				config:     tc.fields.config,
				opType:     tc.fields.opType,
				tableName:  tc.fields.tableName,
				schemaName: tc.fields.schemaName,
				commitTs:   tc.fields.commitTs,
				columns:    tc.fields.columns,
				preColumns: tc.fields.preColumns,
				newRecord:  true,
				HandleKey:  tc.fields.HandleKey,
			}

			require.Equal(t, tc.want, c.encode())
		})
	}
}

func TestConvertToCSVType(t *testing.T) {
	for _, group := range csvTestColumnsGroup {
		for _, c := range group {
			cfg := &common.Config{BinaryEncodingMethod: c.BinaryEncodingMethod}
			col := model.Column2ColumnDataXForTest(&c.col)
			val, _ := fromColValToCsvVal(cfg, col, c.colInfo.Ft)
			require.Equal(t, c.want, val, c.col.Name)
		}
	}
}

func TestRowChangeEventConversion(t *testing.T) {
	for idx, group := range csvTestColumnsGroup {
		row := &model.RowChangedEvent{}
		cols := make([]*model.Column, 0)
		colInfos := make([]rowcodec.ColInfo, 0)
		for _, c := range group {
			cols = append(cols, &c.col)
			colInfos = append(colInfos, c.colInfo)
		}
		tidbTableInfo := model.BuildTiDBTableInfo(fmt.Sprintf("table%d", idx), cols, nil)
		model.AddExtraColumnInfo(tidbTableInfo, colInfos)
		row.TableInfo = model.WrapTableInfo(100, "test", 100, tidbTableInfo)

		if idx%3 == 0 { // delete operation
			row.PreColumns = model.Columns2ColumnDatas(cols, row.TableInfo)
		} else if idx%3 == 1 { // insert operation
			row.Columns = model.Columns2ColumnDatas(cols, row.TableInfo)
		} else { // update operation
			row.PreColumns = model.Columns2ColumnDatas(cols, row.TableInfo)
			row.Columns = model.Columns2ColumnDatas(cols, row.TableInfo)
		}
		csvMsg, err := rowChangedEvent2CSVMsg(&common.Config{
			Delimiter:            "\t",
			Quote:                "\"",
			Terminator:           "\n",
			NullString:           "\\N",
			IncludeCommitTs:      true,
			BinaryEncodingMethod: group[0].BinaryEncodingMethod,
		}, row)
		require.NotNil(t, csvMsg)
		require.Nil(t, err)

		row2, err := csvMsg2RowChangedEvent(&common.Config{
			BinaryEncodingMethod: group[0].BinaryEncodingMethod,
		}, csvMsg, row.TableInfo)
		require.Nil(t, err)
		require.NotNil(t, row2)
	}
}

func TestCSVMessageDecode(t *testing.T) {
	// datums := make([][]types.Datum, 0, 4)
	testCases := []struct {
		row              []types.Datum
		expectedCommitTs uint64
		expectedColsCnt  int
		expectedErr      string
	}{
		{
			row: []types.Datum{
				types.NewStringDatum("I"),
				types.NewStringDatum("employee"),
				types.NewStringDatum("hr"),
				types.NewStringDatum("433305438660591626"),
				types.NewStringDatum("101"),
				types.NewStringDatum("Smith"),
				types.NewStringDatum("Bob"),
				types.NewStringDatum("2014-06-04"),
				types.NewDatum(nil),
			},
			expectedCommitTs: 433305438660591626,
			expectedColsCnt:  5,
			expectedErr:      "",
		},
		{
			row: []types.Datum{
				types.NewStringDatum("U"),
				types.NewStringDatum("employee"),
				types.NewStringDatum("hr"),
				types.NewStringDatum("433305438660591627"),
				types.NewStringDatum("101"),
				types.NewStringDatum("Smith"),
				types.NewStringDatum("Bob"),
				types.NewStringDatum("2015-10-08"),
				types.NewStringDatum("Los Angeles"),
			},
			expectedCommitTs: 433305438660591627,
			expectedColsCnt:  5,
			expectedErr:      "",
		},
		{
			row: []types.Datum{
				types.NewStringDatum("D"),
				types.NewStringDatum("employee"),
				types.NewStringDatum("hr"),
			},
			expectedCommitTs: 0,
			expectedColsCnt:  0,
			expectedErr:      "the csv row should have at least four columns",
		},
		{
			row: []types.Datum{
				types.NewStringDatum("D"),
				types.NewStringDatum("employee"),
				types.NewStringDatum("hr"),
				types.NewStringDatum("hello world"),
			},
			expectedCommitTs: 0,
			expectedColsCnt:  0,
			expectedErr:      "the 4th column(hello world) of csv row should be a valid commit-ts",
		},
	}
	for _, tc := range testCases {
		csvMsg := newCSVMessage(&common.Config{
			Delimiter:       ",",
			Quote:           "\"",
			Terminator:      "\n",
			NullString:      "\\N",
			IncludeCommitTs: true,
		})
		err := csvMsg.decode(tc.row)
		if tc.expectedErr != "" {
			require.Contains(t, err.Error(), tc.expectedErr)
		} else {
			require.Nil(t, err)
			require.Equal(t, tc.expectedCommitTs, csvMsg.commitTs)
			require.Equal(t, tc.expectedColsCnt, len(csvMsg.columns))
		}
	}
}
