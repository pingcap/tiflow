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
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func setupEncoderAndSchemaRegistry(config *common.Config) (*BatchEncoder, error) {
	startHTTPInterceptForTestingRegistry()

	keyManager, err := NewAvroSchemaManager(
		context.Background(),
		nil,
		"http://127.0.0.1:8081",
		"-key",
	)
	if err != nil {
		return nil, err
	}

	valueManager, err := NewAvroSchemaManager(
		context.Background(),
		nil,
		"http://127.0.0.1:8081",
		"-value",
	)
	if err != nil {
		return nil, err
	}

	return &BatchEncoder{
		namespace:          model.DefaultNamespace,
		valueSchemaManager: valueManager,
		keySchemaManager:   keyManager,
		result:             make([]*common.Message, 0, 1),
		config:             config,
	}, nil
}

func teardownEncoderAndSchemaRegistry() {
	stopHTTPInterceptForTestingRegistry()
}

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
			Ft:            common.SetUnsigned(types.NewFieldType(mysql.TypeTiny)),
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
			Ft:            common.SetUnsigned(types.NewFieldType(mysql.TypeShort)),
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
			Ft:            common.SetUnsigned(types.NewFieldType(mysql.TypeInt24)),
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
			Ft:            common.SetUnsigned(types.NewFieldType(mysql.TypeLong)),
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
			Ft:            common.SetUnsigned(types.NewFieldType(mysql.TypeLonglong)),
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
			Ft:            common.SetBinChsClnFlag(types.NewFieldType(mysql.TypeTinyBlob)),
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
			Ft:            common.SetBinChsClnFlag(types.NewFieldType(mysql.TypeMediumBlob)),
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
			Ft:            common.SetBinChsClnFlag(types.NewFieldType(mysql.TypeBlob)),
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
			Ft:            common.SetBinChsClnFlag(types.NewFieldType(mysql.TypeLongBlob)),
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
			Ft:            common.SetBinChsClnFlag(types.NewFieldType(mysql.TypeVarchar)),
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
			Ft:            common.SetBinChsClnFlag(types.NewFieldType(mysql.TypeVarString)),
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
			Ft:            common.SetBinChsClnFlag(types.NewFieldType(mysql.TypeString)),
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
			Ft:            common.SetElems(types.NewFieldType(mysql.TypeEnum), []string{"a,", "b"}),
		},
		avroSchema{
			Type:       "string",
			Parameters: map[string]string{"tidb_type": "ENUM", "allowed": "a\\,,b"},
		},
		"a,", "string",
	},
	{
		model.Column{Name: "set", Value: uint64(1), Type: mysql.TypeSet},
		rowcodec.ColInfo{
			ID:            30,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            common.SetElems(types.NewFieldType(mysql.TypeSet), []string{"a,", "b"}),
		},
		avroSchema{
			Type:       "string",
			Parameters: map[string]string{"tidb_type": "SET", "allowed": "a\\,,b"},
		},
		"a,", "string",
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
		schema, err := columnToAvroSchema(&v.col, v.colInfo.Ft, "precise", "long")
		require.NoError(t, err)
		require.Equal(t, v.expectedSchema, schema)
		if v.col.Name == "decimal" {
			schema, err := columnToAvroSchema(&v.col, v.colInfo.Ft, "string", "long")
			require.NoError(t, err)
			require.Equal(
				t,
				avroSchema{Type: "string", Parameters: map[string]string{"tidb_type": "DECIMAL"}},
				schema,
			)
		}
		if v.col.Name == "longlongunsigned" {
			schema, err := columnToAvroSchema(&v.col, v.colInfo.Ft, "precise", "string")
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
		data, str, err := columnToAvroData(&v.col, v.colInfo.Ft, "precise", "long")
		require.NoError(t, err)
		require.Equal(t, v.expectedData, data)
		require.Equal(t, v.expectedType, str)
		if v.col.Name == "decimal" {
			data, str, err := columnToAvroData(&v.col, v.colInfo.Ft, "string", "long")
			require.NoError(t, err)
			require.Equal(t, "129012.1230000", data)
			require.Equal(t, "string", str)
		}
		if v.col.Name == "longlongunsigned" {
			data, str, err := columnToAvroData(&v.col, v.colInfo.Ft, "precise", "string")
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

func TestRowToAvroSchemaEnableChecksum(t *testing.T) {
	t.Parallel()

	table := model.TableName{
		Schema: "testdb",
		Table:  "rowtoavroschema",
	}
	namespace := getAvroNamespace(model.DefaultNamespace, &table)
	cols := make([]*model.Column, 0)
	colInfos := make([]rowcodec.ColInfo, 0)

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

	input := &avroEncodeInput{
		cols,
		colInfos,
	}

	rand.New(rand.NewSource(time.Now().Unix())).Shuffle(len(input.columns), func(i, j int) {
		input.columns[i], input.columns[j] = input.columns[j], input.columns[i]
		input.colInfos[i], input.colInfos[j] = input.colInfos[j], input.colInfos[i]
	})

	schema, err := rowToAvroSchema(
		namespace,
		table.Table,
		input,
		true,
		true,
		"string",
		"string",
	)
	require.NoError(t, err)
	require.Equal(t, expectedSchemaWithExtensionEnableChecksum, indentJSON(schema))
	_, err = goavro.NewCodec(schema)
	require.NoError(t, err)

	require.True(t, sort.IsSorted(input))
}

func TestRowToAvroSchema(t *testing.T) {
	t.Parallel()

	table := model.TableName{
		Schema: "testdb",
		Table:  "rowtoavroschema",
	}
	namespace := getAvroNamespace(model.DefaultNamespace, &table)
	cols := make([]*model.Column, 0)
	colInfos := make([]rowcodec.ColInfo, 0)

	for _, v := range avroTestColumns {
		cols = append(cols, &v.col)
		colInfos = append(colInfos, v.colInfo)
		colNew := v.col
		colNew.Name = colNew.Name + "nullable"
		colNew.Value = nil
		colNew.Flag.SetIsNullable()
		cols = append(cols, &colNew)
		colInfos = append(colInfos, v.colInfo)
	}

	schema, err := rowToAvroSchema(
		namespace,
		table.Table,
		&avroEncodeInput{
			cols,
			colInfos,
		},
		false,
		false,
		"precise",
		"long",
	)
	require.NoError(t, err)
	require.Equal(t, expectedSchemaWithoutExtension, indentJSON(schema))
	_, err = goavro.NewCodec(schema)
	require.NoError(t, err)

	schema, err = rowToAvroSchema(
		namespace,
		table.Table,
		&avroEncodeInput{
			cols,
			colInfos,
		},
		true,
		false,
		"precise",
		"long",
	)
	require.NoError(t, err)
	require.Equal(t, expectedSchemaWithExtension, indentJSON(schema))
	_, err = goavro.NewCodec(schema)
	require.NoError(t, err)
}

func TestRowToAvroData(t *testing.T) {
	t.Parallel()

	cols := make([]*model.Column, 0)
	colInfos := make([]rowcodec.ColInfo, 0)

	for _, v := range avroTestColumns {
		cols = append(cols, &v.col)
		colInfos = append(colInfos, v.colInfo)
		colNew := v.col
		colNew.Name = colNew.Name + "nullable"
		colNew.Value = nil
		colNew.Flag.SetIsNullable()
		cols = append(cols, &colNew)
		colInfos = append(colInfos, v.colInfo)
	}

	data, err := rowToAvroData(&avroEncodeInput{cols, colInfos}, 417318403368288260, "c", false, "precise", "long")
	require.NoError(t, err)
	_, exists := data["_tidb_commit_ts"]
	require.False(t, exists)
	_, exists = data["_tidb_op"]
	require.False(t, exists)
	_, exists = data["_tidb_commit_physical_time"]
	require.False(t, exists)

	data, err = rowToAvroData(&avroEncodeInput{cols, colInfos}, 417318403368288260, "c", true, "precise", "long")
	require.NoError(t, err)
	v, exists := data["_tidb_commit_ts"]
	require.True(t, exists)
	require.Equal(t, int64(417318403368288260), v.(int64))
	v, exists = data["_tidb_commit_physical_time"]
	require.True(t, exists)
	require.Equal(t, int64(1591943372224), v.(int64))
	v, exists = data["_tidb_op"]
	require.True(t, exists)
	require.Equal(t, "c", v.(string))
}

func TestAvroEncode(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true
	codecConfig.AvroBigintUnsignedHandlingMode = "long"
	codecConfig.AvroDecimalHandlingMode = "precise"
	encoder, err := setupEncoderAndSchemaRegistry(codecConfig)
	require.NoError(t, err)
	defer teardownEncoderAndSchemaRegistry()

	cols := make([]*model.Column, 0)
	colInfos := make([]rowcodec.ColInfo, 0)

	cols = append(
		cols,
		&model.Column{
			Name:  "id",
			Value: int64(1),
			Type:  mysql.TypeLong,
			Flag:  model.HandleKeyFlag,
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
		cols = append(cols, &colNew)
		colInfos = append(colInfos, v.colInfo)
	}

	event := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "testdb",
			Table:  "avroencode",
		},
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema: "testdb",
				Table:  "avroencode",
			},
		},
		Columns:  cols,
		ColInfos: colInfos,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	keyCols, keyColInfos := event.HandleKeyColInfos()
	namespace := getAvroNamespace(encoder.namespace, event.Table)

	keySchema, err := rowToAvroSchema(
		namespace,
		event.Table.Table,
		&avroEncodeInput{
			keyCols,
			keyColInfos,
		},
		false,
		false,
		"precise",
		"long",
	)
	require.NoError(t, err)
	avroKeyCodec, err := goavro.NewCodec(keySchema)
	require.NoError(t, err)

	r, err := encoder.avroEncode(ctx, event, "default", true)
	require.NoError(t, err)
	res, _, err := avroKeyCodec.NativeFromBinary(r.data)
	require.NoError(t, err)
	require.NotNil(t, res)
	for k := range res.(map[string]interface{}) {
		if k == "_tidb_commit_ts" || k == "_tidb_op" || k == "_tidb_commit_physical_time" {
			require.Fail(t, "key shall not include extension fields")
		}
	}

	valueSchema, err := rowToAvroSchema(
		namespace,
		event.Table.Table,
		&avroEncodeInput{
			cols, colInfos,
		},
		true,
		false,
		"precise",
		"long",
	)
	require.NoError(t, err)
	avroValueCodec, err := goavro.NewCodec(valueSchema)
	require.NoError(t, err)

	r, err = encoder.avroEncode(ctx, event, "default", false)
	require.NoError(t, err)
	res, _, err = avroValueCodec.NativeFromBinary(r.data)
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

	res := avroEncodeResult{
		data:       bin,
		registryID: 7,
	}

	evlp, err := res.toEnvelope()
	require.NoError(t, err)

	require.Equal(t, magicByte, evlp[0])
	require.Equal(t, []byte{0, 0, 0, 7}, evlp[1:5])

	parsed, _, err := avroCodec.NativeFromBinary(evlp[5:])
	require.NoError(t, err)
	require.NotNil(t, parsed)

	id, exists := parsed.(map[string]interface{})["id"]
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
		getAvroNamespace(
			"normalNamespace",
			&model.TableName{Schema: "normalSchema", Table: "normalTable"},
		),
	)
	require.Equal(
		t,
		"_1Namespace._1Schema",
		getAvroNamespace("1Namespace", &model.TableName{Schema: "1Schema", Table: "normalTable"}),
	)
	require.Equal(
		t,
		"N_amespace.S_chema",
		getAvroNamespace("N-amespace", &model.TableName{Schema: "S.chema", Table: "normalTable"}),
	)
}

func TestArvoAppendRowChangedEventWithCallback(t *testing.T) {
	t.Parallel()

	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true
	codecConfig.AvroBigintUnsignedHandlingMode = "long"
	codecConfig.AvroDecimalHandlingMode = "precise"

	encoder, err := setupEncoderAndSchemaRegistry(codecConfig)
	require.NoError(t, err)
	defer teardownEncoderAndSchemaRegistry()

	// Empty build makes sure that the callback build logic not broken.
	msgs := encoder.Build()
	require.Len(t, msgs, 0, "no message should be built and no panic")

	row := &model.RowChangedEvent{
		CommitTs:  1,
		Table:     &model.TableName{Schema: "a", Table: "b"},
		TableInfo: &model.TableInfo{TableName: model.TableName{Schema: "a", Table: "b"}},
		Columns: []*model.Column{{
			Name:  "col1",
			Type:  mysql.TypeVarchar,
			Value: []byte("aa"),
		}},
		ColInfos: []rowcodec.ColInfo{{
			ID:            1000,
			IsPKHandle:    true,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeVarchar),
		}},
	}

	ctx := context.Background()
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

func TestEncodeCheckpoint(t *testing.T) {
	t.Parallel()

	config := &common.Config{
		EnableTiDBExtension: true,
		AvroEnableWatermark: true,
	}

	encoder := &BatchEncoder{
		namespace: model.DefaultNamespace,
		result:    make([]*common.Message, 0, 1),
		config:    config,
	}

	message, err := encoder.EncodeCheckpointEvent(446266400629063682)
	require.NoError(t, err)
	require.NotNil(t, message)

	topic := "test-topic"
	decoder := NewDecoder(config, nil, topic)
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, exist, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, exist)
	require.Equal(t, model.MessageTypeResolved, messageType)

	obtained, err := decoder.NextResolvedEvent()
	require.NoError(t, err)
	require.Equal(t, uint64(446266400629063682), obtained)
}

func TestEncodeDDLEvent(t *testing.T) {
	t.Parallel()

	config := &common.Config{
		EnableTiDBExtension: true,
		AvroEnableWatermark: true,
	}

	encoder := &BatchEncoder{
		namespace: model.DefaultNamespace,
		result:    make([]*common.Message, 0, 1),
		config:    config,
	}

	message, err := encoder.EncodeDDLEvent(&model.DDLEvent{
		StartTs:  1020,
		CommitTs: 1030,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:      "test",
				Table:       "t1",
				TableID:     0,
				IsPartition: false,
			},
		},
		Type:  timodel.ActionAddColumn,
		Query: "ALTER TABLE test.t1 ADD COLUMN a int",
	})
	require.NoError(t, err)
	require.NotNil(t, message)

	topic := "test-topic"
	decoder := NewDecoder(config, nil, topic)
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, exist, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, exist)
	require.Equal(t, model.MessageTypeDDL, messageType)

	decodedEvent, err := decoder.NextDDLEvent()
	require.NoError(t, err)
	require.NotNil(t, decodedEvent)
	require.Equal(t, uint64(1030), decodedEvent.CommitTs)
	require.Equal(t, timodel.ActionAddColumn, decodedEvent.Type)
	require.Equal(t, "ALTER TABLE test.t1 ADD COLUMN a int", decodedEvent.Query)
	require.Equal(t, "test", decodedEvent.TableInfo.TableName.Schema)
	require.Equal(t, "t1", decodedEvent.TableInfo.TableName.Table)
	require.Equal(t, int64(0), decodedEvent.TableInfo.TableName.TableID)
	require.False(t, decodedEvent.TableInfo.TableName.IsPartition)
}
