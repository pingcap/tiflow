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
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"go.uber.org/zap"
)

type decoder struct {
	config *common.Config
	topic  string
	sc     *stmtctx.StatementContext

	keySchemaM   *SchemaManager
	valueSchemaM *SchemaManager

	key   []byte
	value []byte
}

// NewDecoder return an avro decoder
func NewDecoder(
	config *common.Config,
	keySchemaM *SchemaManager,
	valueSchemaM *SchemaManager,
	topic string,
	tz *time.Location,
) codec.RowEventDecoder {
	return &decoder{
		config:       config,
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
	if d.key == nil && d.value == nil {
		return model.MessageTypeUnknown, false, nil
	}

	// it must a row event.
	if d.key != nil {
		return model.MessageTypeRow, true, nil
	}
	if len(d.value) < 1 {
		return model.MessageTypeUnknown, false, errors.ErrAvroInvalidMessage.FastGenByArgs()
	}
	switch d.value[0] {
	case magicByte:
		return model.MessageTypeRow, true, nil
	case ddlByte:
		return model.MessageTypeDDL, true, nil
	case checkpointByte:
		return model.MessageTypeResolved, true, nil
	}
	return model.MessageTypeUnknown, false, errors.ErrAvroInvalidMessage.FastGenByArgs()
}

// NextRowChangedEvent returns the next row changed event if exists
func (d *decoder) NextRowChangedEvent() (*model.RowChangedEvent, bool, error) {
	var (
		valueMap  map[string]interface{}
		rawSchema string
		err       error
	)

	ctx := context.Background()
	key, rawSchema, err := d.decodeKey(ctx)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	isDelete := len(d.value) == 0
	if isDelete {
		valueMap = key
	} else {
		valueMap, rawSchema, err = d.decodeValue(ctx)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
	}

	schema := make(map[string]interface{})
	if err := json.Unmarshal([]byte(rawSchema), &schema); err != nil {
		return nil, false, errors.Trace(err)
	}

	fields, ok := schema["fields"].([]interface{})
	if !ok {
		return nil, false, errors.New("schema fields should be a map")
	}

	columns := make([]*model.Column, 0, len(valueMap))
	for _, value := range fields {
		field, ok := value.(map[string]interface{})
		if !ok {
			return nil, false, errors.New("schema field should be a map")
		}

		// `tidbOp` is the first extension field in the schema, so we can break here.
		colName := field["name"].(string)
		if colName == tidbOp {
			break
		}

		var holder map[string]interface{}
		switch ty := field["type"].(type) {
		case []interface{}:
			if m, ok := ty[0].(map[string]interface{}); ok {
				holder = m["connect.parameters"].(map[string]interface{})
			} else if m, ok := ty[1].(map[string]interface{}); ok {
				holder = m["connect.parameters"].(map[string]interface{})
			} else {
				log.Panic("type info is anything else", zap.Any("typeInfo", field["type"]))
			}
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
			return nil, false, errors.New("value not found")
		}

		switch t := value.(type) {
		// for nullable columns, the value is encoded as a map with one pair.
		// key is the encoded type, value is the encoded value, only care about the value here.
		case map[string]interface{}:
			for _, v := range t {
				value = v
			}
		}

		switch mysqlType {
		case mysql.TypeEnum:
			// enum type is encoded as string,
			// we need to convert it to int by the order of the enum values definition.
			allowed := strings.Split(holder["allowed"].(string), ",")
			switch t := value.(type) {
			case string:
				enum, err := types.ParseEnum(allowed, t, "")
				if err != nil {
					return nil, false, errors.Trace(err)
				}
				value = enum.Value
			case nil:
				value = nil
			}
		case mysql.TypeSet:
			// set type is encoded as string,
			// we need to convert it to the binary format.
			elems := strings.Split(holder["allowed"].(string), ",")
			switch t := value.(type) {
			case string:
				s, err := types.ParseSet(elems, t, "")
				if err != nil {
					return nil, false, errors.Trace(err)
				}
				value = s.Value
			case nil:
				value = nil
			}
		}

		col := &model.Column{
			Name:  colName,
			Type:  mysqlType,
			Flag:  flag,
			Value: value,
		}
		columns = append(columns, col)
	}

	var commitTs int64
	if !isDelete {
		o, ok := valueMap[tidbCommitTs]
		if !ok {
			return nil, false, errors.New("commit ts not found")
		}
		commitTs = o.(int64)

		o, ok = valueMap[tidbRowLevelChecksum]
		if ok {
			var expected uint64
			checksum := o.(string)
			if checksum != "" {
				expected, err = strconv.ParseUint(checksum, 10, 64)
				if err != nil {
					return nil, false, errors.Trace(err)
				}
				if o, ok := valueMap[tidbCorrupted]; ok {
					corrupted := o.(bool)
					if corrupted {
						log.Warn("row data is corrupted",
							zap.String("topic", d.topic),
							zap.String("checksum", checksum))
						for _, col := range columns {
							log.Info("data corrupted, print each column for debugging",
								zap.String("name", col.Name),
								zap.Any("type", col.Type),
								zap.Any("charset", col.Charset),
								zap.Any("flag", col.Flag),
								zap.Any("value", col.Value),
								zap.Any("default", col.Default),
							)
						}
					}
				}

				if err := d.verifyChecksum(columns, expected); err != nil {
					return nil, false, errors.Trace(err)
				}
			}
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
	event.Columns = columns

	return event, false, nil
}

// NextResolvedEvent returns the next resolved event if exists
func (d *decoder) NextResolvedEvent() (uint64, error) {
	if len(d.value) == 0 {
		return 0, errors.New("value should not be empty")
	}
	ts := binary.BigEndian.Uint64(d.value[1:])
	d.value = nil
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

	data := d.value[1:]
	var baseDDLEvent ddlEvent
	err := json.Unmarshal(data, &baseDDLEvent)
	if err != nil {
		return nil, errors.WrapError(errors.ErrDecodeFailed, err)
	}
	d.value = nil

	result := new(model.DDLEvent)
	result.TableInfo = new(model.TableInfo)
	result.CommitTs = baseDDLEvent.CommitTs
	result.TableInfo.TableName = model.TableName{
		Schema: baseDDLEvent.Schema,
		Table:  baseDDLEvent.Table,
	}
	result.Type = baseDDLEvent.Type
	result.Query = baseDDLEvent.Query

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

func (d *decoder) decodeKey(ctx context.Context) (map[string]interface{}, string, error) {
	schemaID, binary, err := extractSchemaIDAndBinaryData(d.key)
	if err != nil {
		return nil, "", err
	}

	codec, err := d.keySchemaM.Lookup(ctx, d.topic, schemaID)
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
	d.key = nil

	return result, codec.Schema(), nil
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
