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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
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

	schemaM SchemaManager

	key   []byte
	value []byte
}

// NewDecoder return an avro decoder
func NewDecoder(
	config *common.Config,
	schemaM SchemaManager,
	topic string,
	tz *time.Location,
) codec.RowEventDecoder {
	return &decoder{
		config:  config,
		topic:   topic,
		schemaM: schemaM,
		sc:      stmtctx.NewStmtCtxWithTimeZone(tz),
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
		return model.MessageTypeUnknown, false, errors.ErrAvroInvalidMessage.FastGenByArgs(d.value)
	}
	switch d.value[0] {
	case magicByte:
		return model.MessageTypeRow, true, nil
	case ddlByte:
		return model.MessageTypeDDL, true, nil
	case checkpointByte:
		return model.MessageTypeResolved, true, nil
	}
	return model.MessageTypeUnknown, false, errors.ErrAvroInvalidMessage.FastGenByArgs(d.value)
}

// NextRowChangedEvent returns the next row changed event if exists
func (d *decoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	var (
		valueMap    map[string]interface{}
		valueSchema map[string]interface{}
		err         error
	)

	ctx := context.Background()
	keyMap, keySchema, err := d.decodeKey(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// for the delete event, only have key part, it holds primary key or the unique key columns.
	// for the insert / update, extract the value part, it holds all columns.
	isDelete := len(d.value) == 0
	if isDelete {
		// delete event only have key part, treat it as the value part also.
		valueMap = keyMap
		valueSchema = keySchema
	} else {
		valueMap, valueSchema, err = d.decodeValue(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	event, err := assembleEvent(keyMap, valueMap, valueSchema, isDelete)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Delete event only has Primary Key Columns, but the checksum is calculated based on the whole row columns,
	// checksum verification cannot be done here, so skip it.
	if isDelete {
		return event, nil
	}

	expectedChecksum, found, err := extractExpectedChecksum(valueMap)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if isCorrupted(valueMap) {
		log.Warn("row data is corrupted",
			zap.String("topic", d.topic), zap.Uint64("checksum", expectedChecksum))
		for _, col := range event.Columns {
			log.Info("data corrupted, print each column for debugging",
				zap.String("name", col.Name),
				zap.Any("type", col.Type),
				zap.Any("charset", col.Charset),
				zap.Any("flag", col.Flag),
				zap.Any("value", col.Value),
				zap.Any("default", col.Default))
		}

	}

	if found {
		if err := common.VerifyChecksum(event.Columns, uint32(expectedChecksum)); err != nil {
			return nil, errors.Trace(err)
		}
	}

	return event, nil
}

// assembleEvent return a row changed event
// keyMap hold primary key or unique key columns
// valueMap hold all columns information
// schema is corresponding to the valueMap, it can be used to decode the valueMap to construct columns.
func assembleEvent(keyMap, valueMap, schema map[string]interface{}, isDelete bool) (*model.RowChangedEvent, error) {
	fields, ok := schema["fields"].([]interface{})
	if !ok {
		return nil, errors.New("schema fields should be a map")
	}

	columns := make([]*model.Column, 0, len(valueMap))
	// fields is ordered by the column id, so iterate over it to build columns
	// it's also the order to calculate the checksum.
	for _, item := range fields {
		field, ok := item.(map[string]interface{})
		if !ok {
			return nil, errors.New("schema field should be a map")
		}

		// `tidbOp` is the first extension field in the schema,
		// it's not real columns, so break here.
		colName := field["name"].(string)
		if colName == tidbOp {
			break
		}

		// query the field to get `tidbType`, and get the mysql type from it.
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

		mysqlType := mysqlTypeFromTiDBType(tidbType)

		flag := flagFromTiDBType(tidbType)
		if _, ok := keyMap[colName]; ok {
			flag.SetIsHandleKey()
		}

		value, ok := valueMap[colName]
		if !ok {
			return nil, errors.New("value not found")
		}
		value, err := getColumnValue(value, holder, mysqlType)
		if err != nil {
			return nil, errors.Trace(err)
		}

		col := &model.Column{
			Name:  colName,
			Type:  mysqlType,
			Flag:  flag,
			Value: value,
		}
		columns = append(columns, col)
	}

	// "namespace.schema"
	namespace := schema["namespace"].(string)
	schemaName := strings.Split(namespace, ".")[1]
	tableName := schema["name"].(string)

	var commitTs int64
	if !isDelete {
		o, ok := valueMap[tidbCommitTs]
		if !ok {
			return nil, errors.New("commit ts not found")
		}
		commitTs = o.(int64)
	}

	event := new(model.RowChangedEvent)
	event.CommitTs = uint64(commitTs)
	event.TableInfo = &model.TableInfo{
		TableName: model.TableName{
			Schema: schemaName,
			Table:  tableName,
		},
	}

	if isDelete {
		event.PreColumns = columns
	} else {
		event.Columns = columns
	}

	return event, nil
}

func isCorrupted(valueMap map[string]interface{}) bool {
	o, ok := valueMap[tidbCorrupted]
	if !ok {
		return false
	}

	corrupted := o.(bool)
	return corrupted
}

// extract the checksum from the received value map
// return true if the checksum found, and return error if the checksum is not valid
func extractExpectedChecksum(valueMap map[string]interface{}) (uint64, bool, error) {
	o, ok := valueMap[tidbRowLevelChecksum]
	if !ok {
		return 0, false, nil
	}
	checksum := o.(string)
	if checksum == "" {
		return 0, false, nil
	}
	result, err := strconv.ParseUint(checksum, 10, 64)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	return result, true, nil
}

// value is an interface, need to convert it to the real value with the help of type info.
// holder has the value's column info.
func getColumnValue(value interface{}, holder map[string]interface{}, mysqlType byte) (interface{}, error) {
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
				return nil, errors.Trace(err)
			}
			value = enum.Value
		case nil:
			value = nil
		}
	case mysql.TypeSet:
		// set type is encoded as string,
		// we need to convert it to int by the order of the set values definition.
		elems := strings.Split(holder["allowed"].(string), ",")
		switch t := value.(type) {
		case string:
			s, err := types.ParseSet(elems, t, "")
			if err != nil {
				return nil, errors.Trace(err)
			}
			value = s.Value
		case nil:
			value = nil
		}
	}
	return value, nil
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
func extractConfluentSchemaIDAndBinaryData(data []byte) (int, []byte, error) {
	if len(data) < 5 {
		return 0, nil, errors.ErrAvroInvalidMessage.
			FastGenByArgs("an avro message using confluent schema registry should have at least 5 bytes")
	}
	if data[0] != magicByte {
		return 0, nil, errors.ErrAvroInvalidMessage.
			FastGenByArgs("magic byte is not match, it should be 0")
	}
	id, err := getConfluentSchemaIDFromHeader(data[0:5])
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	return int(id), data[5:], nil
}

func extractGlueSchemaIDAndBinaryData(data []byte) (string, []byte, error) {
	if len(data) < 18 {
		return "", nil, errors.ErrAvroInvalidMessage.
			FastGenByArgs("an avro message using glue schema registry should have at least 18 bytes")
	}
	if data[0] != headerVersionByte {
		return "", nil, errors.ErrAvroInvalidMessage.
			FastGenByArgs("header version byte is not match, it should be %d", headerVersionByte)
	}
	if data[1] != compressionDefaultByte {
		return "", nil, errors.ErrAvroInvalidMessage.
			FastGenByArgs("compression byte is not match, it should be %d", compressionDefaultByte)
	}
	id, err := getGlueSchemaIDFromHeader(data[0:18])
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	return id, data[18:], nil
}

func decodeRawBytes(
	ctx context.Context, schemaM SchemaManager, data []byte, topic string,
) (map[string]interface{}, map[string]interface{}, error) {
	var schemaID schemaID
	var binary []byte
	var err error
	var cid int
	var gid string

	switch schemaM.RegistryType() {
	case common.SchemaRegistryTypeConfluent:
		cid, binary, err = extractConfluentSchemaIDAndBinaryData(data)
		if err != nil {
			return nil, nil, err
		}
		schemaID.confluentSchemaID = cid
	case common.SchemaRegistryTypeGlue:
		gid, binary, err = extractGlueSchemaIDAndBinaryData(data)
		if err != nil {
			return nil, nil, err
		}
		schemaID.glueSchemaID = gid
	default:
		return nil, nil, errors.New("unknown schema registry type")
	}

	codec, err := schemaM.Lookup(ctx, topic, schemaID)
	if err != nil {
		return nil, nil, err
	}

	native, _, err := codec.NativeFromBinary(binary)
	if err != nil {
		return nil, nil, err
	}

	result, ok := native.(map[string]interface{})
	if !ok {
		return nil, nil, errors.New("raw avro message is not a map")
	}

	schema := make(map[string]interface{})
	if err := json.Unmarshal([]byte(codec.Schema()), &schema); err != nil {
		return nil, nil, errors.Trace(err)
	}

	return result, schema, nil
}

func (d *decoder) decodeKey(ctx context.Context) (map[string]interface{}, map[string]interface{}, error) {
	data := d.key
	d.key = nil
	return decodeRawBytes(ctx, d.schemaM, data, d.topic)
}

func (d *decoder) decodeValue(ctx context.Context) (map[string]interface{}, map[string]interface{}, error) {
	data := d.value
	d.value = nil
	return decodeRawBytes(ctx, d.schemaM, data, d.topic)
}
