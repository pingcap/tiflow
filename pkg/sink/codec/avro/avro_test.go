// Copyright 2020 PingCAP, Inc.
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

package avro

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
	"github.com/pingcap/tiflow/pkg/uuid"
	"github.com/stretchr/testify/require"
)

type avroTestColumnTuple struct {
	col            model.Column
	colInfo        rowcodec.ColInfo
	expectedSchema interface{}
	expectedData   interface{}
	expectedType   string
}

var avroTestColumns = []*avroTestColumnTuple{
	{
		model.Column{Name: "tiny", Value: int64(1), Type: mysql.TypeTiny},
		rowcodec.ColInfo{
			ID:            1,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeTiny),
		},
		avroSchema{Type: "int", Parameters: map[string]string{"tidb_type": "INT"}},
		int32(1), "int",
	},
	{
		model.Column{Name: "short", Value: int64(1), Type: mysql.TypeShort},
		rowcodec.ColInfo{
			ID:            2,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeShort),
		},
		avroSchema{Type: "int", Parameters: map[string]string{"tidb_type": "INT"}},
		int32(1), "int",
	},
	{
		model.Column{Name: "int24", Value: int64(1), Type: mysql.TypeInt24},
		rowcodec.ColInfo{
			ID:            3,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeInt24),
		},
		avroSchema{Type: "int", Parameters: map[string]string{"tidb_type": "INT"}},
		int32(1), "int",
	},
	{
		model.Column{Name: "long", Value: int64(1), Type: mysql.TypeLong},
		rowcodec.ColInfo{
			ID:            4,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeLong),
		},
		avroSchema{Type: "int", Parameters: map[string]string{"tidb_type": "INT"}},
		int32(1), "int",
	},
	{
		model.Column{Name: "longlong", Value: int64(1), Type: mysql.TypeLonglong},
		rowcodec.ColInfo{
			ID:            5,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeLonglong),
		},
		avroSchema{Type: "long", Parameters: map[string]string{"tidb_type": "BIGINT"}},
		int64(1), "long",
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
			Ft:            utils.SetUnsigned(types.NewFieldType(mysql.TypeTiny)),
		},
		avroSchema{Type: "int", Parameters: map[string]string{"tidb_type": "INT UNSIGNED"}},
		int32(1), "int",
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
			Ft:            utils.SetUnsigned(types.NewFieldType(mysql.TypeShort)),
		},
		avroSchema{Type: "int", Parameters: map[string]string{"tidb_type": "INT UNSIGNED"}},
		int32(1), "int",
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
			Ft:            utils.SetUnsigned(types.NewFieldType(mysql.TypeInt24)),
		},
		avroSchema{Type: "int", Parameters: map[string]string{"tidb_type": "INT UNSIGNED"}},
		int32(1), "int",
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
			Ft:            utils.SetUnsigned(types.NewFieldType(mysql.TypeLong)),
		},
		avroSchema{Type: "long", Parameters: map[string]string{"tidb_type": "INT UNSIGNED"}},
		int64(1), "long",
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
			Ft:            utils.SetUnsigned(types.NewFieldType(mysql.TypeLonglong)),
		},
		avroSchema{Type: "long", Parameters: map[string]string{"tidb_type": "BIGINT UNSIGNED"}},
		int64(1), "long",
	},
	{
		model.Column{Name: "float", Value: float32(3.14), Type: mysql.TypeFloat},
		rowcodec.ColInfo{
			ID:            11,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeFloat),
		},
		avroSchema{Type: "float", Parameters: map[string]string{"tidb_type": "FLOAT"}},
		float32(3.14), "float",
	},
	{
		model.Column{Name: "double", Value: float64(3.14), Type: mysql.TypeDouble},
		rowcodec.ColInfo{
			ID:            12,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeDouble),
		},
		avroSchema{Type: "double", Parameters: map[string]string{"tidb_type": "DOUBLE"}},
		float64(3.14), "double",
	},
	{
		model.Column{Name: "bit", Value: uint64(683), Type: mysql.TypeBit},
		rowcodec.ColInfo{
			ID:            13,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeBit),
		},
		avroSchema{Type: "bytes", Parameters: map[string]string{"tidb_type": "BIT", "length": "1"}},
		[]byte("\x02\xab"), "bytes",
	},
	{
		model.Column{Name: "decimal", Value: "129012.1230000", Type: mysql.TypeNewDecimal},
		rowcodec.ColInfo{
			ID:            14,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeNewDecimal),
		},
		avroLogicalTypeSchema{
			avroSchema: avroSchema{
				Type:       "bytes",
				Parameters: map[string]string{"tidb_type": "DECIMAL"},
			},
			LogicalType: "decimal",
			Precision:   10,
			Scale:       0,
		},
		big.NewRat(129012123, 1000), "bytes.decimal",
	},
	{
		model.Column{Name: "tinytext", Value: []byte("hello world"), Type: mysql.TypeTinyBlob},
		rowcodec.ColInfo{
			ID:            15,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeBlob),
		},
		avroSchema{Type: "string", Parameters: map[string]string{"tidb_type": "TEXT"}},
		"hello world", "string",
	},
	{
		model.Column{Name: "mediumtext", Value: []byte("hello world"), Type: mysql.TypeMediumBlob},
		rowcodec.ColInfo{
			ID:            16,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeMediumBlob),
		},
		avroSchema{Type: "string", Parameters: map[string]string{"tidb_type": "TEXT"}},
		"hello world", "string",
	},
	{
		model.Column{Name: "text", Value: []byte("hello world"), Type: mysql.TypeBlob},
		rowcodec.ColInfo{
			ID:            17,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeBlob),
		},
		avroSchema{Type: "string", Parameters: map[string]string{"tidb_type": "TEXT"}},
		"hello world", "string",
	},
	{
		model.Column{Name: "longtext", Value: []byte("hello world"), Type: mysql.TypeLongBlob},
		rowcodec.ColInfo{
			ID:            18,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeLongBlob),
		},
		avroSchema{Type: "string", Parameters: map[string]string{"tidb_type": "TEXT"}},
		"hello world", "string",
	},
	{
		model.Column{Name: "varchar", Value: []byte("hello world"), Type: mysql.TypeVarchar},
		rowcodec.ColInfo{
			ID:            19,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeVarchar),
		},
		avroSchema{Type: "string", Parameters: map[string]string{"tidb_type": "TEXT"}},
		"hello world", "string",
	},
	{
		model.Column{Name: "varstring", Value: []byte("hello world"), Type: mysql.TypeVarString},
		rowcodec.ColInfo{
			ID:            20,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeVarString),
		},
		avroSchema{Type: "string", Parameters: map[string]string{"tidb_type": "TEXT"}},
		"hello world", "string",
	},
	{
		model.Column{Name: "string", Value: []byte("hello world"), Type: mysql.TypeString},
		rowcodec.ColInfo{
			ID:            21,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeString),
		},
		avroSchema{Type: "string", Parameters: map[string]string{"tidb_type": "TEXT"}},
		"hello world", "string",
	},
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
			Ft:            utils.SetBinChsClnFlag(types.NewFieldType(mysql.TypeTinyBlob)),
		},
		avroSchema{Type: "bytes", Parameters: map[string]string{"tidb_type": "BLOB"}},
		[]byte("hello world"), "bytes",
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
			Ft:            utils.SetBinChsClnFlag(types.NewFieldType(mysql.TypeMediumBlob)),
		},
		avroSchema{Type: "bytes", Parameters: map[string]string{"tidb_type": "BLOB"}},
		[]byte("hello world"), "bytes",
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
			Ft:            utils.SetBinChsClnFlag(types.NewFieldType(mysql.TypeBlob)),
		},
		avroSchema{Type: "bytes", Parameters: map[string]string{"tidb_type": "BLOB"}},
		[]byte("hello world"), "bytes",
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
			Ft:            utils.SetBinChsClnFlag(types.NewFieldType(mysql.TypeLongBlob)),
		},
		avroSchema{Type: "bytes", Parameters: map[string]string{"tidb_type": "BLOB"}},
		[]byte("hello world"), "bytes",
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
			Ft:            utils.SetBinChsClnFlag(types.NewFieldType(mysql.TypeVarchar)),
		},
		avroSchema{Type: "bytes", Parameters: map[string]string{"tidb_type": "BLOB"}},
		[]byte("hello world"), "bytes",
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
			Ft:            utils.SetBinChsClnFlag(types.NewFieldType(mysql.TypeVarString)),
		},
		avroSchema{Type: "bytes", Parameters: map[string]string{"tidb_type": "BLOB"}},
		[]byte("hello world"), "bytes",
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
			Ft:            utils.SetBinChsClnFlag(types.NewFieldType(mysql.TypeString)),
		},
		avroSchema{Type: "bytes", Parameters: map[string]string{"tidb_type": "BLOB"}},
		[]byte("hello world"), "bytes",
	},
	{
		model.Column{Name: "enum", Value: uint64(1), Type: mysql.TypeEnum},
		rowcodec.ColInfo{
			ID:            29,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            utils.SetElems(types.NewFieldType(mysql.TypeEnum), []string{"a", "b"}),
		},
		avroSchema{
			Type:       "string",
			Parameters: map[string]string{"tidb_type": "ENUM", "allowed": "a,b"},
		},
		"a", "string",
	},
	{
		model.Column{Name: "set", Value: uint64(1), Type: mysql.TypeSet},
		rowcodec.ColInfo{
			ID:            30,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            utils.SetElems(types.NewFieldType(mysql.TypeSet), []string{"a", "b"}),
		},
		avroSchema{
			Type:       "string",
			Parameters: map[string]string{"tidb_type": "SET", "allowed": "a,b"},
		},
		"a", "string",
	},
	{
		model.Column{Name: "json", Value: `{"key": "value"}`, Type: mysql.TypeJSON},
		rowcodec.ColInfo{
			ID:            31,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeJSON),
		},
		avroSchema{Type: "string", Parameters: map[string]string{"tidb_type": "JSON"}},
		`{"key": "value"}`, "string",
	},
	{
		model.Column{Name: "date", Value: "2000-01-01", Type: mysql.TypeDate},
		rowcodec.ColInfo{
			ID:            32,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeDate),
		},
		avroSchema{Type: "string", Parameters: map[string]string{"tidb_type": "DATE"}},
		"2000-01-01", "string",
	},
	{
		model.Column{Name: "datetime", Value: "2015-12-20 23:58:58", Type: mysql.TypeDatetime},
		rowcodec.ColInfo{
			ID:            33,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeDatetime),
		},
		avroSchema{Type: "string", Parameters: map[string]string{"tidb_type": "DATETIME"}},
		"2015-12-20 23:58:58", "string",
	},
	{
		model.Column{Name: "timestamp", Value: "1973-12-30 15:30:00", Type: mysql.TypeTimestamp},
		rowcodec.ColInfo{
			ID:            34,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeTimestamp),
		},
		avroSchema{Type: "string", Parameters: map[string]string{"tidb_type": "TIMESTAMP"}},
		"1973-12-30 15:30:00", "string",
	},
	{
		model.Column{Name: "time", Value: "23:59:59", Type: mysql.TypeDuration},
		rowcodec.ColInfo{
			ID:            35,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeDuration),
		},
		avroSchema{Type: "string", Parameters: map[string]string{"tidb_type": "TIME"}},
		"23:59:59", "string",
	},
	{
		model.Column{Name: "year", Value: int64(1970), Type: mysql.TypeYear},
		rowcodec.ColInfo{
			ID:            36,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeYear),
		},
		avroSchema{Type: "int", Parameters: map[string]string{"tidb_type": "YEAR"}},
		int32(1970), "int",
	},
}

func TestColumnToAvroSchema(t *testing.T) {
	for _, v := range avroTestColumns {
		encoder := NewAvroEncoder("namespace", nil, &common.Config{
			AvroDecimalHandlingMode:        "precise",
			AvroBigintUnsignedHandlingMode: "long",
		})
		schema, err := encoder.(*BatchEncoder).columnToAvroSchema(&v.col, v.colInfo.Ft)
		require.NoError(t, err)
		require.Equal(t, v.expectedSchema, schema)
		if v.col.Name == "decimal" {
			encoder := NewAvroEncoder("namespace", nil, &common.Config{
				AvroDecimalHandlingMode:        "string",
				AvroBigintUnsignedHandlingMode: "long",
			})
			schema, err := encoder.(*BatchEncoder).columnToAvroSchema(&v.col, v.colInfo.Ft)
			require.NoError(t, err)
			require.Equal(
				t,
				avroSchema{Type: "string", Parameters: map[string]string{"tidb_type": "DECIMAL"}},
				schema,
			)
		}
		if v.col.Name == "longlongunsigned" {
			encoder := NewAvroEncoder("namespace", nil, &common.Config{
				AvroDecimalHandlingMode:        "precise",
				AvroBigintUnsignedHandlingMode: "string",
			})
			schema, err := encoder.(*BatchEncoder).columnToAvroSchema(&v.col, v.colInfo.Ft)
			require.NoError(t, err)
			require.Equal(
				t,
				avroSchema{
					Type:       "string",
					Parameters: map[string]string{"tidb_type": "BIGINT UNSIGNED"},
				},
				schema,
			)
		}
	}
}

func TestColumnToAvroData(t *testing.T) {
	t.Parallel()

	for _, v := range avroTestColumns {
		encoder := NewAvroEncoder("namespace", nil, &common.Config{
			AvroDecimalHandlingMode:        "precise",
			AvroBigintUnsignedHandlingMode: "long",
		})
		data, str, err := encoder.(*BatchEncoder).columnToAvroData(&v.col, v.colInfo.Ft)
		require.NoError(t, err)
		require.Equal(t, v.expectedData, data)
		require.Equal(t, v.expectedType, str)
		if v.col.Name == "decimal" {
			encoder := NewAvroEncoder("namespace", nil, &common.Config{
				AvroDecimalHandlingMode:        "string",
				AvroBigintUnsignedHandlingMode: "long",
			})
			data, str, err := encoder.(*BatchEncoder).columnToAvroData(&v.col, v.colInfo.Ft)
			require.NoError(t, err)
			require.Equal(t, "129012.1230000", data)
			require.Equal(t, "string", str)
		}
		if v.col.Name == "longlongunsigned" {
			encoder := NewAvroEncoder("namespace", nil, &common.Config{
				AvroDecimalHandlingMode:        "precise",
				AvroBigintUnsignedHandlingMode: "string",
			})
			data, str, err := encoder.(*BatchEncoder).columnToAvroData(&v.col, v.colInfo.Ft)
			require.NoError(t, err)
			require.Equal(t, "1", data)
			require.Equal(t, "string", str)
		}
	}
}

func indentJSON(j string) string {
	var buf bytes.Buffer
	_ = json.Indent(&buf, []byte(j), "", "  ")
	return buf.String()
}

func newLargeEvent() *model.RowChangedEvent {
	cols := make([]*model.Column, 0)
	colInfos := make([]rowcodec.ColInfo, 0)

	cols = append(
		cols,
		&model.Column{
			Name:  "id",
			Value: int64(1),
			Type:  mysql.TypeLong,
			Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
		},
	)
	colInfos = append(
		colInfos,
		rowcodec.ColInfo{
			ID:            1000,
			IsPKHandle:    true,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeLong),
		},
	)

	for _, v := range avroTestColumns {
		cols = append(cols, &v.col)
		colInfos = append(colInfos, v.colInfo)

		colNew := v.col
		colNew.Name = colNew.Name + "nullable"
		colNew.Value = nil
		colNew.Flag.SetIsNullable()

		colInfoNew := v.colInfo
		colInfoNew.ID += int64(len(avroTestColumns))

		cols = append(cols, &colNew)
		colInfos = append(colInfos, colInfoNew)
	}

	nameToIDMap := make(map[string]int64, len(cols))
	for i, col := range cols {
		nameToIDMap[col.Name] = colInfos[i].ID
	}
	tidbTableInfo := model.BuildTiDBTableInfoImpl(
		"avroencode",
		cols,
		[][]int{{0}},
		model.NewNameBasedColumnIDAllocator(nameToIDMap))
	model.AddExtraColumnInfo(tidbTableInfo, colInfos)
	tableInfo := model.WrapTableInfo(100, "testdb", 100, tidbTableInfo)
	return &model.RowChangedEvent{
		CommitTs:  417318403368288260,
		TableInfo: tableInfo,
		Columns:   model.Columns2ColumnDatas(cols, tableInfo),
	}
}

func TestRowToAvroSchemaEnableChecksum(t *testing.T) {
	t.Parallel()

	event := newLargeEvent()
	columns := event.GetColumns()
	colInfos := event.TableInfo.GetColInfosForRowChangedEvent()
	input := &avroEncodeInput{
		columns,
		colInfos,
	}

	rand.New(rand.NewSource(time.Now().Unix())).Shuffle(len(input.columns), func(i, j int) {
		input.columns[i], input.columns[j] = input.columns[j], input.columns[i]
		input.colInfos[i], input.colInfos[j] = input.colInfos[j], input.colInfos[i]
	})

	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true
	codecConfig.EnableRowChecksum = true
	codecConfig.AvroDecimalHandlingMode = "string"
	codecConfig.AvroBigintUnsignedHandlingMode = "string"

	encoder := NewAvroEncoder(model.DefaultNamespace, nil, codecConfig)

	schema, err := encoder.(*BatchEncoder).value2AvroSchema(&event.TableInfo.TableName, input)
	require.NoError(t, err)
	require.Equal(t, expectedSchemaWithExtensionEnableChecksum, indentJSON(schema))
	_, err = goavro.NewCodec(schema)
	require.NoError(t, err)

	require.True(t, sort.IsSorted(input))
}

func TestRowToAvroSchema(t *testing.T) {
	t.Parallel()

	event := newLargeEvent()
	columns := event.GetColumns()
	colInfos := event.TableInfo.GetColInfosForRowChangedEvent()
	input := &avroEncodeInput{
		columns,
		colInfos,
	}

	codecConfig := common.NewConfig(config.ProtocolAvro)
	encoder := NewAvroEncoder(model.DefaultNamespace, nil, codecConfig)

	schema, err := encoder.(*BatchEncoder).value2AvroSchema(&event.TableInfo.TableName, input)
	require.NoError(t, err)
	require.Equal(t, expectedSchemaWithoutExtension, indentJSON(schema))
	_, err = goavro.NewCodec(schema)
	require.NoError(t, err)

	codecConfig.EnableTiDBExtension = true
	encoder = NewAvroEncoder(model.DefaultNamespace, nil, codecConfig)

	schema, err = encoder.(*BatchEncoder).value2AvroSchema(&event.TableInfo.TableName, input)
	require.NoError(t, err)
	require.Equal(t, expectedSchemaWithExtension, indentJSON(schema))
	_, err = goavro.NewCodec(schema)
	require.NoError(t, err)
}

func TestRowToAvroData(t *testing.T) {
	t.Parallel()

	event := newLargeEvent()
	columns := event.GetColumns()
	colInfos := event.TableInfo.GetColInfosForRowChangedEvent()
	input := &avroEncodeInput{
		columns,
		colInfos,
	}

	codecConfig := common.NewConfig(config.ProtocolAvro)
	encoder := NewAvroEncoder(model.DefaultNamespace, nil, codecConfig)

	data, err := encoder.(*BatchEncoder).columns2AvroData(input)
	require.NoError(t, err)

	for _, col := range input.columns {
		_, exists := data[col.Name]
		require.True(t, exists)
	}
}

func TestAvroEncode4EnableChecksum(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true
	codecConfig.EnableRowChecksum = true
	codecConfig.AvroDecimalHandlingMode = "string"
	codecConfig.AvroBigintUnsignedHandlingMode = "string"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
	defer TeardownEncoderAndSchemaRegistry4Testing()
	require.NoError(t, err)
	require.NotNil(t, encoder)

	event := newLargeEvent()
	topic := "default"
	bin, err := encoder.encodeValue(ctx, "default", event)
	require.NoError(t, err)

	cid, data, err := extractConfluentSchemaIDAndBinaryData(bin)
	require.NoError(t, err)

	avroValueCodec, err := encoder.schemaM.Lookup(ctx, topic, schemaID{confluentSchemaID: cid})
	require.NoError(t, err)

	res, _, err := avroValueCodec.NativeFromBinary(data)
	require.NoError(t, err)
	require.NotNil(t, res)

	m, ok := res.(map[string]interface{})
	require.True(t, ok)

	_, found := m[tidbRowLevelChecksum]
	require.True(t, found)

	_, found = m[tidbCorrupted]
	require.True(t, found)

	_, found = m[tidbChecksumVersion]
	require.True(t, found)
}

func TestAvroEncode(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
	defer TeardownEncoderAndSchemaRegistry4Testing()
	require.NoError(t, err)
	require.NotNil(t, encoder)

	event := newLargeEvent()
	topic := "default"
	bin, err := encoder.encodeKey(ctx, topic, event)
	require.NoError(t, err)

	cid, data, err := extractConfluentSchemaIDAndBinaryData(bin)
	require.NoError(t, err)

	avroKeyCodec, err := encoder.schemaM.Lookup(ctx, topic, schemaID{confluentSchemaID: cid})
	require.NoError(t, err)

	res, _, err := avroKeyCodec.NativeFromBinary(data)
	require.NoError(t, err)
	require.NotNil(t, res)
	for k := range res.(map[string]interface{}) {
		if k == "_tidb_commit_ts" || k == "_tidb_op" || k == "_tidb_commit_physical_time" {
			require.Fail(t, "key shall not include extension fields")
		}
	}
	require.Equal(t, int32(1), res.(map[string]interface{})["id"])

	bin, err = encoder.encodeValue(ctx, topic, event)
	require.NoError(t, err)

	cid, data, err = extractConfluentSchemaIDAndBinaryData(bin)
	require.NoError(t, err)

	avroValueCodec, err := encoder.schemaM.Lookup(ctx, topic, schemaID{confluentSchemaID: cid})
	require.NoError(t, err)

	res, _, err = avroValueCodec.NativeFromBinary(data)
	require.NoError(t, err)
	require.NotNil(t, res)

	for k, v := range res.(map[string]interface{}) {
		if k == "_tidb_op" {
			require.Equal(t, "c", v.(string))
		}
		if k == "float" {
			require.Equal(t, float32(3.14), v)
		}
	}
}

func TestAvroEnvelope(t *testing.T) {
	t.Parallel()
	cManager := &confluentSchemaManager{}
	gManager := &glueSchemaManager{}
	avroCodec, err := goavro.NewCodec(`
       {
         "type": "record",
         "name": "testdb.avroenvelope",
         "fields" : [
           {"name": "id", "type": "int", "default": 0}
         ]
       }`)

	require.NoError(t, err)

	testNativeData := make(map[string]interface{})
	testNativeData["id"] = 7

	bin, err := avroCodec.BinaryFromNative(nil, testNativeData)
	require.NoError(t, err)

	// test confluent schema message
	header, err := cManager.getMsgHeader(8)
	require.NoError(t, err)
	res := avroEncodeResult{
		data:   bin,
		header: header,
	}

	evlp, err := res.toEnvelope()
	require.NoError(t, err)
	require.Equal(t, header, evlp[0:5])

	parsed, _, err := avroCodec.NativeFromBinary(evlp[5:])
	require.NoError(t, err)
	require.NotNil(t, parsed)

	id, exists := parsed.(map[string]interface{})["id"]
	require.True(t, exists)
	require.Equal(t, int32(7), id)

	// test glue schema message
	uuidGenerator := uuid.NewGenerator()
	uuidS := uuidGenerator.NewString()
	header, err = gManager.getMsgHeader(uuidS)
	require.NoError(t, err)
	res = avroEncodeResult{
		data:   bin,
		header: header,
	}
	evlp, err = res.toEnvelope()
	require.NoError(t, err)
	require.Equal(t, header, evlp[0:18])

	parsed, _, err = avroCodec.NativeFromBinary(evlp[18:])
	require.NoError(t, err)
	require.NotNil(t, parsed)
	id, exists = parsed.(map[string]interface{})["id"]
	require.True(t, exists)
	require.Equal(t, int32(7), id)
}

func TestSanitizeName(t *testing.T) {
	t.Parallel()

	require.Equal(t, "normalColumnName123", sanitizeName("normalColumnName123"))
	require.Equal(
		t,
		"_1ColumnNameStartWithNumber",
		sanitizeName("1ColumnNameStartWithNumber"),
	)
	require.Equal(t, "A_B", sanitizeName("A.B"))
	require.Equal(t, "columnNameWith__", sanitizeName("columnNameWith中文"))
}

func TestGetAvroNamespace(t *testing.T) {
	t.Parallel()

	require.Equal(
		t,
		"normalNamespace.normalSchema",
		getAvroNamespace("normalNamespace", "normalSchema"),
	)
	require.Equal(
		t,
		"_1Namespace._1Schema",
		getAvroNamespace("1Namespace", "1Schema"),
	)
	require.Equal(
		t,
		"N_amespace.S_chema",
		getAvroNamespace("N-amespace", "S.chema"),
	)
}

func TestArvoAppendRowChangedEventWithCallback(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
	defer TeardownEncoderAndSchemaRegistry4Testing()
	require.NoError(t, err)
	require.NotNil(t, encoder)

	// Empty build makes sure that the callback build logic not broken.
	msgs := encoder.Build()
	require.Len(t, msgs, 0, "no message should be built and no panic")

	cols := []*model.Column{{
		Name: "col1",
		Type: mysql.TypeVarchar,
		Flag: model.HandleKeyFlag | model.PrimaryKeyFlag,
	}}
	tableInfo := model.BuildTableInfo("a", "b", cols, [][]int{{0}})
	row := &model.RowChangedEvent{
		CommitTs:  1,
		TableInfo: tableInfo,
		Columns: model.Columns2ColumnDatas([]*model.Column{{
			Name:  "col1",
			Value: []byte("aa"),
		}}, tableInfo),
	}

	expected := 0
	count := 0
	for i := 0; i < 5; i++ {
		expected += i
		bit := i
		err := encoder.AppendRowChangedEvent(ctx, "", row, func() {
			count += bit
		})
		require.NoError(t, err)

		msgs = encoder.Build()
		require.Len(t, msgs, 1, "one message should be built")

		msgs[0].Callback()
		require.Equal(t, expected, count, "expected one callback be called")
	}
}
