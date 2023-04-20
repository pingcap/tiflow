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

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
)

type decoder struct {
	topic string

	key   []byte
	value []byte

	enableTiDBExtension    bool
	enableRowLevelChecksum bool

	keySchemaM   *schemaManager
	valueSchemaM *schemaManager

	nextKey   interface{}
	nextValue interface{}
}

func NewDecoder(key, value []byte,
	enableTiDBExtension bool,
	enableRowLevelChecksum bool,
	keySchemaM *schemaManager,
	valueSchemaM *schemaManager,
) codec.RowEventDecoder {
	return &decoder{
		key:                    key,
		value:                  value,
		enableTiDBExtension:    enableTiDBExtension,
		enableRowLevelChecksum: enableRowLevelChecksum,

		keySchemaM:   keySchemaM,
		valueSchemaM: valueSchemaM,
	}
}

func (d *decoder) HasNext() (model.MessageType, bool, error) {
	if d.key == nil {
		return model.MessageTypeUnknown, false, nil
	}

	schemaID, err := GetSchemaIDFromEnvelope(d.key)
	if err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}

	ctx := context.Background()
	keyCodec, schemaID, err := d.keySchemaM.Lookup(ctx, d.topic, schemaID)
	if err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}

	binary, err := GetBinaryDataFromEnvelope(d.key)
	if err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}

	key, _, err := keyCodec.NativeFromBinary(binary)
	if err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}
	d.nextKey = key

	schemaID, err = GetSchemaIDFromEnvelope(d.value)
	if err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}
	valCodec, schemaID, err := d.valueSchemaM.Lookup(ctx, d.topic, schemaID)
	if err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}

	binary, err = GetBinaryDataFromEnvelope(d.value)
	if err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}

	value, _, err := valCodec.NativeFromBinary(binary)
	if err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}

	d.nextValue = value
	return model.MessageTypeRow, true, nil
}

// NextResolvedEvent returns the next resolved event if exists
func (d *decoder) NextResolvedEvent() (uint64, error) {
	return 0, nil
}

// NextRowChangedEvent returns the next row changed event if exists
func (d *decoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	rawKey, ok := d.nextKey.(map[string]interface{})
	if !ok {
		log.Error("raw avro message is not a map")
		return nil, errors.New("raw avro message is not a map")
	}

	result := new(model.RowChangedEvent)
	for name, col := range rawKey {

	}

	return result, nil
}

// NextDDLEvent returns the next DDL event if exists
func (d *decoder) NextDDLEvent() (*model.DDLEvent, error) {
	return nil, nil
}

func GetSchemaIDFromEnvelope(data []byte) (int, error) {
	if len(data) < 5 {
		return 0, errors.ErrAvroInvalidMessage.FastGenByArgs()
	}
	if data[0] != magicByte {
		return 0, errors.ErrAvroInvalidMessage.FastGenByArgs()
	}
	return int(binary.BigEndian.Uint32(data[1:5])), nil
}

func GetBinaryDataFromEnvelope(data []byte) ([]byte, error) {
	if len(data) < 5 {
		return nil, errors.ErrAvroInvalidMessage.FastGenByArgs()
	}
	if data[0] != magicByte {
		return nil, errors.ErrAvroInvalidMessage.FastGenByArgs()
	}
	return data[5:], nil
}

type builder struct {
}

func NewDecoderBuilder() *builder {
	return &builder{}
}

func (b *builder) Build() codec.RowEventDecoder {
	return &decoder{
		topic:                  "",
		key:                    nil,
		value:                  nil,
		enableTiDBExtension:    false,
		enableRowLevelChecksum: false,
		keySchemaM:             nil,
		valueSchemaM:           nil,
	}
}
