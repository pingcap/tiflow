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
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"go.uber.org/zap"
)

type decoder struct {
	topic string

	key   []byte
	value []byte

	*Options

	keySchemaM   *schemaManager
	valueSchemaM *schemaManager

	nextEvent *model.RowChangedEvent
}

func NewDecoder(key, value []byte,
	o *Options,
	keySchemaM *schemaManager,
	valueSchemaM *schemaManager,
	topic string,
) codec.RowEventDecoder {
	return &decoder{
		key:     key,
		value:   value,
		Options: o,

		keySchemaM:   keySchemaM,
		valueSchemaM: valueSchemaM,

		topic: topic,
	}
}

func (d *decoder) HasNext() (model.MessageType, bool, error) {
	if d.key == nil {
		return model.MessageTypeUnknown, false, nil
	}

	ctx := context.Background()
	key, err := d.decodeKey(ctx)
	if err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}

	valueMap, rawSchema, err := d.decodeValue(ctx)
	if err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}

	schema := make(map[string]interface{})
	if err := json.Unmarshal([]byte(rawSchema), &schema); err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}

	fields, ok := schema["fields"].([]interface{})
	if !ok {
		return model.MessageTypeUnknown, false, errors.New("schema fields should be a map")
	}

	columns := make([]*model.Column, 0, len(valueMap))
	for _, value := range fields {
		field, ok := value.(map[string]interface{})
		if !ok {
			return model.MessageTypeRow, false, errors.New("schema field should be a map")
		}

		// `tidbOp` is the first extension field in the schema, so we can break here.
		colName := field["name"].(string)
		if colName == tidbOp {
			break
		}
		types := field["type"].(map[string]interface{})
		tidbType := types["connect.parameters"].(map[string]interface{})["tidb_type"].(string)
		mysqlType, flag := mysqlTypeFromTiDBType(tidbType)
		if _, ok := key[colName]; ok {
			// todo: handle key or primary key ?
			flag.SetIsHandleKey()
		}
		col := &model.Column{
			Name:    colName,
			Type:    mysqlType,
			Charset: "",
			Flag:    flag,
			Value:   value,
			Default: nil,
		}
		columns = append(columns, col)
	}

	o, ok := valueMap[tidbOp]
	if !ok {
		return model.MessageTypeRow, false, errors.New("operation not found")
	}
	operation := o.(string)

	o, ok = valueMap[tidbCommitTs]
	if !ok {
		return model.MessageTypeRow, false, errors.New("commit ts not found")
	}
	commitTs := o.(int64)

	if d.Options.enableRowChecksum {
		o, ok := valueMap[tidbRowLevelChecksum]
		if !ok {
			return model.MessageTypeRow, false, errors.New("cannot found row level checksum")
		}
		checksum := o.(int64)
		if verifyChecksum(columns, uint64(checksum)) {
			return model.MessageTypeRow, false, errors.New("row level checksum mismatch")
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

	d.nextEvent = event
	return model.MessageTypeRow, true, nil
}

func verifyChecksum(columns []*model.Column, expected uint64) bool {
	return false
}

// NextResolvedEvent returns the next resolved event if exists
func (d *decoder) NextResolvedEvent() (uint64, error) {
	return 0, nil
}

// NextRowChangedEvent returns the next row changed event if exists
func (d *decoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if d.nextEvent != nil {
		result := d.nextEvent
		d.nextEvent = nil
		return result, nil
	}

	log.Info("next event not found")
	return nil, errors.New("next event not found")
}

// NextDDLEvent returns the next DDL event if exists
func (d *decoder) NextDDLEvent() (*model.DDLEvent, error) {
	return nil, nil
}

func GetSchemaIDAndBinaryData(data []byte) (int, []byte, error) {
	if len(data) < 5 {
		return 0, nil, errors.ErrAvroInvalidMessage.FastGenByArgs()
	}
	if data[0] != magicByte {
		return 0, nil, errors.ErrAvroInvalidMessage.FastGenByArgs()
	}
	return int(binary.BigEndian.Uint32(data[1:5])), data[5:], nil
}

func (d *decoder) decodeKey(ctx context.Context) (map[string]interface{}, error) {
	schemaID, binary, err := GetSchemaIDAndBinaryData(d.key)
	if err != nil {
		return nil, err
	}

	codec, schemaID, err := d.keySchemaM.Lookup(ctx, d.topic, schemaID)
	if err != nil {
		return nil, err
	}
	native, _, err := codec.NativeFromBinary(binary)
	if err != nil {
		return nil, err
	}

	result, ok := native.(map[string]interface{})
	if !ok {
		log.Error("raw avro message is not a map")
		return nil, errors.New("raw avro message is not a map")
	}
	return result, nil
}

func (d *decoder) decodeValue(ctx context.Context) (map[string]interface{}, string, error) {
	schemaID, binary, err := GetSchemaIDAndBinaryData(d.value)
	if err != nil {
		return nil, "", err
	}

	codec, schemaID, err := d.valueSchemaM.Lookup(ctx, d.topic, schemaID)
	if err != nil {
		return nil, "", err
	}
	native, _, err := codec.NativeFromBinary(binary)
	if err != nil {
		return nil, "", err
	}

	result, ok := native.(map[string]interface{})
	if !ok {
		log.Error("raw avro message is not a map")
		return nil, "", errors.New("raw avro message is not a map")
	}
	return result, codec.Schema(), nil
}
