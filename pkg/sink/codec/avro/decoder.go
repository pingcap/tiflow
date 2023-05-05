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
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"go.uber.org/zap"
)

type decoder struct {
	*Options
	topic string
	sc    *stmtctx.StatementContext

	keySchemaM   *SchemaManager
	valueSchemaM *SchemaManager

	key   []byte
	value []byte
}

// NewDecoder return an avro decoder
func NewDecoder(
	o *Options,
	keySchemaM *SchemaManager,
	valueSchemaM *SchemaManager,
	topic string,
	tz *time.Location,
) codec.RowEventDecoder {
	return &decoder{
		Options:      o,
		topic:        topic,
		keySchemaM:   keySchemaM,
		valueSchemaM: valueSchemaM,
		sc:           &stmtctx.StatementContext{TimeZone: tz},
	}
}

func (d *decoder) AddKeyValue(key, value []byte) error {
	if d.key != nil || d.value != nil {
		return errors.New("key or value is not nil")
	}
	d.key = key
	d.value = value
	return nil
}

func (d *decoder) HasNext() (model.MessageType, bool, error) {
	if d.key == nil || d.value == nil {
		return model.MessageTypeUnknown, false, nil
	}
	eventType, err := extractEventType(d.value)
	if err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}
	return eventType, true, nil
}

// NextRowChangedEvent returns the next row changed event if exists
func (d *decoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	ctx := context.Background()
	key, err := d.decodeKey(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	valueMap, rawSchema, err := d.decodeValue(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	schema := make(map[string]interface{})
	if err := json.Unmarshal([]byte(rawSchema), &schema); err != nil {
		return nil, errors.Trace(err)
	}

	fields, ok := schema["fields"].([]interface{})
	if !ok {
		return nil, errors.New("schema fields should be a map")
	}

	columns := make([]*model.Column, 0, len(valueMap))
	for _, value := range fields {
		field, ok := value.(map[string]interface{})
		if !ok {
			return nil, errors.New("schema field should be a map")
		}

		// `tidbOp` is the first extension field in the schema, so we can break here.
		colName := field["name"].(string)
		if colName == tidbOp {
			break
		}

		var holder map[string]interface{}
		switch ty := field["type"].(type) {
		case []interface{}:
			holder = ty[1].(map[string]interface{})["connect.parameters"].(map[string]interface{})
		case map[string]interface{}:
			holder = ty["connect.parameters"].(map[string]interface{})
		default:
			log.Panic("type info is anything else", zap.Any("typeInfo", field["type"]))
		}
		tidbType := holder["tidb_type"].(string)

		mysqlType, flag := mysqlAndFlagTypeFromTiDBType(tidbType)
		if _, ok := key[colName]; ok {
			flag.SetIsHandleKey()
		}

		value, ok := valueMap[colName]
		if !ok {
			return nil, errors.New("value not found")
		}

		switch value.(type) {
		// for nullable columns, the value is encoded as a map with one pair.
		// key is the encoded type, value is the encoded value, only care about the value here.
		case map[string]interface{}:
			for _, v := range value.(map[string]interface{}) {
				value = v
			}
		}

		switch mysqlType {
		case mysql.TypeEnum:
			// enum type is encoded as string,
			// we need to convert it to int by the order of the enum values definition.
			allowed := strings.Split(holder["allowed"].(string), ",")
			valueStr := value.(string)
			enum, err := types.ParseEnum(allowed, valueStr, "")
			if err != nil {
				return nil, errors.Trace(err)
			}
			value = enum.Value
		case mysql.TypeSet:
			// set type is encoded as string,
			// we need to convert it to the binary format.
			elems := strings.Split(holder["allowed"].(string), ",")
			valueStr := value.(string)
			s, err := types.ParseSet(elems, valueStr, "")
			if err != nil {
				return nil, errors.Trace(err)
			}
			value = s.Value
		}

		col := &model.Column{
			Name:  colName,
			Type:  mysqlType,
			Flag:  flag,
			Value: value,
		}
		columns = append(columns, col)
	}

	o, ok := valueMap[tidbOp]
	if !ok {
		return nil, errors.New("operation not found")
	}
	operation := o.(string)

	o, ok = valueMap[tidbCommitTs]
	if !ok {
		return nil, errors.New("commit ts not found")
	}
	commitTs := o.(int64)

	o, ok = valueMap[tidbRowLevelChecksum]
	if ok {
		checksum := o.(string)
		expected, err := strconv.ParseUint(checksum, 10, 64)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if o, ok := valueMap[tidbCorrupted]; ok {
			corrupted := o.(bool)
			if corrupted {
				log.Warn("row data is corrupted",
					zap.String("topic", d.topic),
					zap.String("checksum", checksum))
			}
		}

		if err := d.verifyChecksum(columns, expected); err != nil {
			return nil, errors.Trace(err)
		}
	}

	// "namespace.schema"
	namespace := schema["namespace"].(string)
	schemaName := strings.Split(namespace, ".")[1]
	tableName := schema["name"].(string)

	event := new(model.RowChangedEvent)
	event.CommitTs = uint64(commitTs)
	event.Table = &model.TableName{
		Schema: schemaName,
		Table:  tableName,
	}
	switch operation {
	case insertOperation:
		event.Columns = columns
	case updateOperation:
		event.PreColumns = columns
	default:
		log.Panic("unsupported operation type", zap.String("operation", operation))
	}
	return event, nil
}

// NextResolvedEvent returns the next resolved event if exists
func (d *decoder) NextResolvedEvent() (uint64, error) {
	if len(d.value) == 0 {
		return 0, errors.New("value should not be empty")
	}
	ts := binary.BigEndian.Uint64(d.value[1:])
	return ts, nil
}

// NextDDLEvent returns the next DDL event if exists
func (d *decoder) NextDDLEvent() (*model.DDLEvent, error) {
	if len(d.value) == 0 {
		return nil, errors.New("value should not be empty")
	}
	if d.value[0] != ddlByte {
		return nil, fmt.Errorf("first byte is not the ddl byte, but got: %+v", d.value[0])
	}

	result := new(model.DDLEvent)
	data := d.value[1:]

	err := json.Unmarshal(data, &result)
	if err != nil {
		return nil, errors.WrapError(errors.ErrDecodeFailed, err)
	}
	return result, nil
}

// return the schema ID and the encoded binary data
// schemaID can be used to fetch the corresponding schema from schema registry,
// which should be used to decode the binary data.
func extractSchemaIDAndBinaryData(data []byte) (int, []byte, error) {
	if len(data) < 5 {
		return 0, nil, errors.ErrAvroInvalidMessage.FastGenByArgs()
	}
	if data[0] != magicByte {
		return 0, nil, errors.ErrAvroInvalidMessage.FastGenByArgs()
	}
	return int(binary.BigEndian.Uint32(data[1:5])), data[5:], nil
}

func (d *decoder) decodeKey(ctx context.Context) (map[string]interface{}, error) {
	schemaID, binary, err := extractSchemaIDAndBinaryData(d.key)
	if err != nil {
		return nil, err
	}

	codec, err := d.keySchemaM.Lookup(ctx, d.topic, schemaID)
	if err != nil {
		return nil, err
	}
	native, _, err := codec.NativeFromBinary(binary)
	if err != nil {
		return nil, err
	}

	result, ok := native.(map[string]interface{})
	if !ok {
		return nil, errors.New("raw avro message is not a map")
	}
	d.key = nil

	return result, nil
}

func (d *decoder) decodeValue(ctx context.Context) (map[string]interface{}, string, error) {
	schemaID, binary, err := extractSchemaIDAndBinaryData(d.value)
	if err != nil {
		return nil, "", err
	}

	codec, err := d.valueSchemaM.Lookup(ctx, d.topic, schemaID)
	if err != nil {
		return nil, "", err
	}
	native, _, err := codec.NativeFromBinary(binary)
	if err != nil {
		return nil, "", err
	}

	result, ok := native.(map[string]interface{})
	if !ok {
		return nil, "", errors.New("raw avro message is not a map")
	}
	d.value = nil

	return result, codec.Schema(), nil
}

func extractEventType(data []byte) (model.MessageType, error) {
	if len(data) < 1 {
		return model.MessageTypeUnknown, errors.ErrAvroInvalidMessage.FastGenByArgs()
	}
	switch data[0] {
	case magicByte:
		return model.MessageTypeRow, nil
	case ddlByte:
		return model.MessageTypeDDL, nil
	case checkpointByte:
		return model.MessageTypeResolved, nil
	}
	return model.MessageTypeUnknown, errors.ErrAvroInvalidMessage.FastGenByArgs()
}

func (d *decoder) verifyChecksum(columns []*model.Column, expected uint64) error {
	calculator := rowcodec.RowData{
		Cols: make([]rowcodec.ColData, 0, len(columns)),
		Data: make([]byte, 0),
	}
	for _, col := range columns {
		info := &timodel.ColumnInfo{
			FieldType: *types.NewFieldType(col.Type),
		}

		data, err := d.buildDatum(col.Value, col.Type)
		if err != nil {
			return errors.Trace(err)
		}
		calculator.Cols = append(calculator.Cols, rowcodec.ColData{
			ColumnInfo: info,
			Datum:      &data,
		})
	}
	checksum, err := calculator.Checksum()
	if err != nil {
		return errors.Trace(err)
	}

	if uint64(checksum) != expected {
		log.Error("checksum mismatch",
			zap.Uint64("expected", expected),
			zap.Uint32("actual", checksum))
		return errors.New("checksum mismatch")
	}

	log.Info("checksum passed",
		zap.Uint64("expected", expected), zap.Uint32("actual", checksum))

	return nil
}

func (d *decoder) buildDatum(value interface{}, typ byte) (types.Datum, error) {
	if value == nil {
		return types.NewDatum(value), nil
	}
	switch typ {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong,
		mysql.TypeLonglong, mysql.TypeInt24, mysql.TypeYear:
		switch a := value.(type) {
		case int32:
			return types.NewIntDatum(int64(a)), nil
		case uint32:
			return types.NewUintDatum(uint64(a)), nil
		case int64:
			return types.NewIntDatum(a), nil
		case uint64:
			return types.NewUintDatum(a), nil
		case string:
			v, err := strconv.ParseUint(a, 10, 64)
			if err != nil {
				return types.Datum{}, errors.Trace(err)
			}
			return types.NewUintDatum(v), nil
		default:
			log.Panic("unknown golang type for the mysql Types",
				zap.Any("mysqlType", typ), zap.Any("value", value))
		}
	case mysql.TypeTimestamp:
		mysqlTime, err := types.ParseTimestamp(d.sc, value.(string))
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		return types.NewTimeDatum(mysqlTime), nil
	case mysql.TypeDatetime:
		mysqlTime, err := types.ParseDatetime(d.sc, value.(string))
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		return types.NewTimeDatum(mysqlTime), nil
	case mysql.TypeDate, mysql.TypeNewDate:
		mysqlTime, err := types.ParseDate(d.sc, value.(string))
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		return types.NewTimeDatum(mysqlTime), nil
	case mysql.TypeDuration:
		duration, ok, err := types.ParseDuration(d.sc, value.(string), 0)
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		if ok {
			return types.Datum{}, errors.New("parse duration failed")
		}
		return types.NewDurationDatum(duration), nil
	case mysql.TypeNewDecimal:
		dec := new(types.MyDecimal)
		err := dec.FromString([]byte(value.(string)))
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		return types.NewDecimalDatum(dec), nil
	case mysql.TypeEnum:
		e := types.Enum{
			Name:  "",
			Value: value.(uint64),
		}
		return types.NewMysqlEnumDatum(e), nil
	case mysql.TypeSet:
		s := types.Set{
			Name:  "",
			Value: value.(uint64),
		}
		return types.NewMysqlSetDatum(s, ""), nil
	case mysql.TypeJSON:
		// original json string convert to map, and then marshal it to binary,
		// to follow the tidb json logic.
		var m map[string]interface{}
		err := json.Unmarshal([]byte(value.(string)), &m)
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}

		binary, err := json.Marshal(m)
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		var bj types.BinaryJSON
		err = bj.UnmarshalJSON(binary)
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}

		return types.NewJSONDatum(bj), nil
	case mysql.TypeBit:
		return types.NewBinaryLiteralDatum(value.([]byte)), nil
	default:
	}
	return types.NewDatum(value), nil
}
