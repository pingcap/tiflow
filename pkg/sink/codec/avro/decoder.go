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
}

func NewDecoder(key, value []byte,
	enableTiDBExtension bool,
	enableRowLevelChecksum bool,
) codec.RowEventDecoder {
	return &decoder{
		key:                    key,
		value:                  value,
		enableTiDBExtension:    enableTiDBExtension,
		enableRowLevelChecksum: enableRowLevelChecksum,
	}
}

func (d *decoder) HasNext() (model.MessageType, bool, error) {
	if d.key == nil {
		return model.MessageTypeUnknown, false, nil
	}

	ctx := context.Background()
	schemaID, err := GetSchemaIDFromEnvelope(d.key)
	if err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}

	keyCodec, schemaID, err := d.keySchemaM.Lookup(ctx, d.topic, schemaID)
	if err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}

	binary, err := GetBinaryDataFromEnvelope(d.key)
	if err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}

	result, remained, err := keyCodec.NativeFromBinary(binary)
	if err != nil {
		return model.MessageTypeUnknown, false, errors.Trace(err)
	}

	return model.MessageTypeRow, true, nil
}

// NextResolvedEvent returns the next resolved event if exists
func (d *decoder) NextResolvedEvent() (uint64, error) {

}

// NextRowChangedEvent returns the next row changed event if exists
func (d *decoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	return nil, nil
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
