// Copyright 2023 PingCAP, Inc.
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

package simple

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"sort"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/simple/avro"
)

//go:embed message.json
var avroSchemaBytes []byte

type marshaller interface {
	// MarshalCheckpoint marshals the checkpoint ts into bytes.
	MarshalCheckpoint(ts uint64) ([]byte, error)

	// MarshalDDLEvent marshals the DDL event into bytes.
	MarshalDDLEvent(event *model.DDLEvent) ([]byte, error)

	// MarshalRowChangedEvent marshals the row changed event into bytes.
	MarshalRowChangedEvent(event *model.RowChangedEvent, config *common.Config,
		handleKeyOnly bool, claimCheckFileName string) ([]byte, error)

	// Unmarshal the bytes into the given value.
	Unmarshal(data []byte, v any) error
}

func newMarshaller(format common.EncodingFormatType) (marshaller, error) {
	var (
		result marshaller
		//err    error
	)
	switch format {
	case common.EncodingFormatJSON:
		result = newJSONMarshaller()
	case common.EncodingFormatAvro:
		result = newAvroGoGenMarshaller()
		//result, err = newAvroMarshaller(string(avroSchemaBytes))
		//if err != nil {
		//	return nil, errors.Trace(err)
		//}
	default:
		return nil, errors.New("unknown encoding format")
	}
	return result, nil
}

type avroGoGenMarshaller struct{}

func newAvroGoGenMarshaller() *avroGoGenMarshaller {
	return &avroGoGenMarshaller{}
}

// MarshalCheckpoint implement the marshaller interface
func (a *avroGoGenMarshaller) MarshalCheckpoint(ts uint64) ([]byte, error) {
	watermark := avro.Watermark{
		Version:  defaultVersion,
		Type:     string(WatermarkType),
		CommitTs: int64(ts),
		BuildTs:  time.Now().UnixMilli(),
	}

	payload := avro.UnionWatermarkBootstrapDDLDML{
		Watermark: watermark,
		UnionType: avro.UnionWatermarkBootstrapDDLDMLTypeEnumWatermark,
	}
	m := avro.Message{Payload: payload}

	var buf bytes.Buffer
	err := m.Serialize(&buf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return buf.Bytes(), nil
}

func newGoGenAvroTableSchema(tableInfo *model.TableInfo) avro.TableSchema {
	sort.SliceStable(tableInfo.Columns, func(i, j int) bool {
		return tableInfo.Columns[i].ID < tableInfo.Columns[j].ID
	})

	columnsSchema := make([]avro.ColumnSchema, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		mysqlType := avro.DataType{
			MysqlType: types.TypeToStr(col.GetType(), col.GetCharset()),
			Charset:   col.GetCharset(),
			Collate:   col.GetCollate(),
			Length:    int64(col.GetFlen()),
		}
		switch col.GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
			mysql.TypeFloat, mysql.TypeDouble, mysql.TypeBit, mysql.TypeYear:
			mysqlType.Unsigned = &avro.UnionNullBool{
				Bool:      mysql.HasUnsignedFlag(col.GetFlag()),
				UnionType: avro.UnionNullBoolTypeEnumBool,
			}
			mysqlType.Zerofill = &avro.UnionNullBool{
				Bool:      mysql.HasZerofillFlag(col.GetFlag()),
				UnionType: avro.UnionNullBoolTypeEnumBool,
			}
		case mysql.TypeEnum, mysql.TypeSet:
			mysqlType.Elements = &avro.UnionNullArrayString{
				ArrayString: col.GetElems(),
				UnionType:   avro.UnionNullArrayStringTypeEnumArrayString,
			}
		case mysql.TypeNewDecimal:
			mysqlType.Decimal = &avro.UnionNullInt{
				Int:       int32(col.GetDecimal()),
				UnionType: avro.UnionNullIntTypeEnumInt,
			}
		default:
		}

		column := avro.ColumnSchema{
			Name:     col.Name.O,
			DataType: mysqlType,
			Nullable: !mysql.HasNotNullFlag(col.GetFlag()),
			Default:  nil,
		}

		defaultValue := entry.GetColumnDefaultValue(col)
		if defaultValue != nil {
			column.Default = &avro.UnionNullString{
				String:    defaultValue.(string),
				UnionType: avro.UnionNullStringTypeEnumString,
			}
		}
		columnsSchema = append(columnsSchema, column)
	}

	indexesSchema := make([]avro.IndexSchema, 0, len(tableInfo.Indices))

	pkInIndexes := false
	for _, idx := range tableInfo.Indices {
		index := avro.IndexSchema{
			Name:    idx.Name.O,
			Unique:  idx.Unique,
			Primary: idx.Primary,
		}
		columns := make([]string, 0, len(idx.Columns))
		for _, col := range idx.Columns {
			columns = append(columns, col.Name.O)
			colInfo := tableInfo.Columns[col.Offset]
			// An index is not null when all columns of aer not null
			if !mysql.HasNotNullFlag(colInfo.GetFlag()) {
				index.Nullable = true
			}
		}
		index.Columns = columns
		if idx.Primary {
			pkInIndexes = true
		}
		indexesSchema = append(indexesSchema, index)
	}

	if !pkInIndexes {
		pkColumns := tableInfo.GetPrimaryKeyColumnNames()
		if len(pkColumns) > 0 {
			index := avro.IndexSchema{
				Name:     "PRIMARY",
				Unique:   true,
				Primary:  true,
				Nullable: false,
				Columns:  pkColumns,
			}
			indexesSchema = append(indexesSchema, index)
		}
	}

	return avro.TableSchema{
		Database: tableInfo.TableName.Schema,
		Table:    tableInfo.TableName.Table,
		TableID:  tableInfo.ID,
		Version:  int64(tableInfo.UpdateTS),
		Columns:  columnsSchema,
		Indexes:  indexesSchema,
	}
}

// MarshalDDLEvent implement the marshaller interface
func (a *avroGoGenMarshaller) MarshalDDLEvent(event *model.DDLEvent) ([]byte, error) {
	var payload avro.UnionWatermarkBootstrapDDLDML
	if event.IsBootstrap {
		tableSchema := newGoGenAvroTableSchema(event.TableInfo)
		b := avro.Bootstrap{
			Version:     defaultVersion,
			Type:        string(BootstrapType),
			BuildTs:     time.Now().UnixMilli(),
			TableSchema: tableSchema,
		}

		payload = avro.UnionWatermarkBootstrapDDLDML{
			Bootstrap: b,
			UnionType: avro.UnionWatermarkBootstrapDDLDMLTypeEnumBootstrap,
		}
	} else {
		ddlType, err := avro.NewDDLTypeValue(getDDLType(event.Type))
		if err != nil {
			return nil, errors.Trace(err)
		}

		var (
			tableSchema    *avro.UnionNullTableSchema
			preTableSchema *avro.UnionNullTableSchema
		)

		if event.TableInfo != nil && event.TableInfo.TableInfo != nil {
			schema := newGoGenAvroTableSchema(event.TableInfo)
			tableSchema = &avro.UnionNullTableSchema{
				TableSchema: schema,
				UnionType:   avro.UnionNullTableSchemaTypeEnumTableSchema,
			}
		}

		if event.PreTableInfo != nil && event.PreTableInfo.TableInfo != nil {
			schema := newGoGenAvroTableSchema(event.PreTableInfo)
			preTableSchema = &avro.UnionNullTableSchema{
				TableSchema: schema,
				UnionType:   avro.UnionNullTableSchemaTypeEnumTableSchema,
			}
		}

		ddl := avro.DDL{
			Version:        defaultVersion,
			Type:           ddlType,
			Sql:            event.Query,
			CommitTs:       int64(event.CommitTs),
			BuildTs:        time.Now().UnixMilli(),
			TableSchema:    tableSchema,
			PreTableSchema: preTableSchema,
		}
		payload = avro.UnionWatermarkBootstrapDDLDML{
			DDL:       ddl,
			UnionType: avro.UnionWatermarkBootstrapDDLDMLTypeEnumDDL,
		}
	}

	m := avro.Message{Payload: payload}
	var buf bytes.Buffer
	err := m.Serialize(&buf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return buf.Bytes(), nil
}

// MarshalRowChangedEvent implement the marshaller interface
func (a *avroGoGenMarshaller) MarshalRowChangedEvent(
	event *model.RowChangedEvent, config *common.Config,
	handleKeyOnly bool, claimCheckFileName string,
) ([]byte, error) {
	return nil, nil
}

// Unmarshal implement the marshaller interface
func (a *avroGoGenMarshaller) Unmarshal(data []byte, v any) error {
	reader := bytes.NewReader(data)
	avroMessage, err := avro.DeserializeMessage(reader)
	if err != nil {
		return errors.Trace(err)
	}

	err = newMessageFromAvroGoGenMessage(&avroMessage, v.(*message))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

type jsonMarshaller struct{}

func newJSONMarshaller() *jsonMarshaller {
	return &jsonMarshaller{}
}

// MarshalCheckpoint implement the marshaller interface
func (m *jsonMarshaller) MarshalCheckpoint(ts uint64) ([]byte, error) {
	msg := newResolvedMessage(ts)
	result, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.WrapError(errors.ErrEncodeFailed, err)
	}
	return result, nil
}

// MarshalDDLEvent implement the marshaller interface
func (m *jsonMarshaller) MarshalDDLEvent(event *model.DDLEvent) ([]byte, error) {
	var (
		msg *message
		err error
	)
	if event.IsBootstrap {
		msg, err = newBootstrapMessage(event)
	} else {
		msg, err = newDDLMessage(event)
	}
	if err != nil {
		return nil, err
	}
	value, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.WrapError(errors.ErrEncodeFailed, err)
	}
	return value, nil
}

// MarshalRowChangedEvent implement the marshaller interface
func (m *jsonMarshaller) MarshalRowChangedEvent(
	event *model.RowChangedEvent, config *common.Config,
	handleKeyOnly bool, claimCheckFileName string,
) ([]byte, error) {
	msg, err := newDMLMessage(event, config, handleKeyOnly, claimCheckFileName)
	if err != nil {
		return nil, err
	}

	value, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.WrapError(errors.ErrEncodeFailed, err)
	}
	return value, nil
}

// Unmarshal implement the marshaller interface
func (m *jsonMarshaller) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

type avroMarshaller struct {
	codec *goavro.Codec
}

func newAvroMarshaller(schema string) (*avroMarshaller, error) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &avroMarshaller{
		codec: codec,
	}, nil
}

// Marshal implement the marshaller interface
func (m *avroMarshaller) Marshal(v any) ([]byte, error) {
	return m.codec.BinaryFromNative(nil, v)
}

// MarshalCheckpoint implement the marshaller interface
func (m *avroMarshaller) MarshalCheckpoint(ts uint64) ([]byte, error) {
	msg := newResolvedMessageMap(ts)
	result, err := m.codec.BinaryFromNative(nil, msg)
	if err != nil {
		return nil, errors.WrapError(errors.ErrEncodeFailed, err)
	}
	return result, nil
}

// MarshalDDLEvent implement the marshaller interface
func (m *avroMarshaller) MarshalDDLEvent(event *model.DDLEvent) ([]byte, error) {
	var msg map[string]interface{}
	if event.IsBootstrap {
		msg = newBootstrapMessageMap(event.TableInfo)
	} else {
		msg = newDDLMessageMap(event)
	}
	value, err := m.codec.BinaryFromNative(nil, msg)
	if err != nil {
		return nil, errors.WrapError(errors.ErrEncodeFailed, err)
	}
	return value, nil
}

// MarshalRowChangedEvent implement the marshaller interface
func (m *avroMarshaller) MarshalRowChangedEvent(
	event *model.RowChangedEvent, config *common.Config,
	handleKeyOnly bool, claimCheckFileName string,
) ([]byte, error) {
	msg, err := newDMLMessageMap(event, config, handleKeyOnly, claimCheckFileName)
	if err != nil {
		return nil, err
	}
	value, err := m.codec.BinaryFromNative(nil, msg)
	if err != nil {
		return nil, errors.WrapError(errors.ErrEncodeFailed, err)
	}

	recycleMap(msg)
	return value, nil
}

// Unmarshal implement the marshaller interface
func (m *avroMarshaller) Unmarshal(data []byte, v any) error {
	native, _, err := m.codec.NativeFromBinary(data)
	if err != nil {
		return errors.Trace(err)
	}
	err = newMessageFromAvroNative(native, v.(*message))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
