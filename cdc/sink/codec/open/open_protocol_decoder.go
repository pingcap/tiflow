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

package open

import (
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/cdc/sink/codec/internal"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// BatchMixedDecoder decodes the byte of a batch into the original messages.
type BatchMixedDecoder struct {
	mixedBytes []byte
	nextKey    *internal.MessageKey
	nextKeyLen uint64
}

// HasNext implements the EventBatchDecoder interface
func (b *BatchMixedDecoder) HasNext() (model.MessageType, bool, error) {
	if !b.hasNext() {
		return 0, false, nil
	}
	if err := b.decodeNextKey(); err != nil {
		return 0, false, err
	}
	return b.nextKey.Type, true, nil
}

// NextResolvedEvent implements the EventBatchDecoder interface
func (b *BatchMixedDecoder) NextResolvedEvent() (uint64, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return 0, err
		}
	}
	b.mixedBytes = b.mixedBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MessageTypeResolved {
		return 0, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found resolved event message")
	}
	valueLen := binary.BigEndian.Uint64(b.mixedBytes[:8])
	b.mixedBytes = b.mixedBytes[valueLen+8:]
	resolvedTs := b.nextKey.Ts
	b.nextKey = nil
	return resolvedTs, nil
}

// NextRowChangedEvent implements the EventBatchDecoder interface
func (b *BatchMixedDecoder) NextRowChangedEvent() (*model.DetailedRowChangedEvent, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return nil, err
		}
	}
	b.mixedBytes = b.mixedBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MessageTypeRow {
		return nil, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found row event message")
	}
	valueLen := binary.BigEndian.Uint64(b.mixedBytes[:8])
	value := b.mixedBytes[8 : valueLen+8]
	b.mixedBytes = b.mixedBytes[valueLen+8:]
	rowMsg := new(messageRow)
	if err := rowMsg.decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	rowEvent := msgToRowChange(b.nextKey, rowMsg)
	b.nextKey = nil
	return rowEvent, nil
}

// NextDDLEvent implements the EventBatchDecoder interface
func (b *BatchMixedDecoder) NextDDLEvent() (*model.DDLEvent, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return nil, err
		}
	}
	b.mixedBytes = b.mixedBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MessageTypeDDL {
		return nil, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found ddl event message")
	}
	valueLen := binary.BigEndian.Uint64(b.mixedBytes[:8])
	value := b.mixedBytes[8 : valueLen+8]
	b.mixedBytes = b.mixedBytes[valueLen+8:]
	ddlMsg := new(messageDDL)
	if err := ddlMsg.decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	ddlEvent := msgToDDLEvent(b.nextKey, ddlMsg)
	b.nextKey = nil
	return ddlEvent, nil
}

func (b *BatchMixedDecoder) hasNext() bool {
	return len(b.mixedBytes) > 0
}

func (b *BatchMixedDecoder) decodeNextKey() error {
	keyLen := binary.BigEndian.Uint64(b.mixedBytes[:8])
	key := b.mixedBytes[8 : keyLen+8]
	// drop value bytes
	msgKey := new(internal.MessageKey)
	err := msgKey.Decode(key)
	if err != nil {
		return errors.Trace(err)
	}
	b.nextKey = msgKey
	b.nextKeyLen = keyLen
	return nil
}

// BatchDecoder decodes the byte of a batch into the original messages.
type BatchDecoder struct {
	keyBytes   []byte
	valueBytes []byte
	nextKey    *internal.MessageKey
	nextKeyLen uint64
}

// HasNext implements the EventBatchDecoder interface
func (b *BatchDecoder) HasNext() (model.MessageType, bool, error) {
	if !b.hasNext() {
		return 0, false, nil
	}
	if err := b.decodeNextKey(); err != nil {
		return 0, false, err
	}
	return b.nextKey.Type, true, nil
}

// NextResolvedEvent implements the EventBatchDecoder interface
func (b *BatchDecoder) NextResolvedEvent() (uint64, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return 0, err
		}
	}
	b.keyBytes = b.keyBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MessageTypeResolved {
		return 0, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found resolved event message")
	}
	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	b.valueBytes = b.valueBytes[valueLen+8:]
	resolvedTs := b.nextKey.Ts
	b.nextKey = nil
	return resolvedTs, nil
}

// NextRowChangedEvent implements the EventBatchDecoder interface
func (b *BatchDecoder) NextRowChangedEvent() (*model.DetailedRowChangedEvent, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return nil, err
		}
	}
	b.keyBytes = b.keyBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MessageTypeRow {
		return nil, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found row event message")
	}
	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	value := b.valueBytes[8 : valueLen+8]
	b.valueBytes = b.valueBytes[valueLen+8:]
	rowMsg := new(messageRow)
	if err := rowMsg.decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	rowEvent := msgToRowChange(b.nextKey, rowMsg)
	b.nextKey = nil
	return rowEvent, nil
}

// NextDDLEvent implements the EventBatchDecoder interface
func (b *BatchDecoder) NextDDLEvent() (*model.DDLEvent, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return nil, err
		}
	}
	b.keyBytes = b.keyBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MessageTypeDDL {
		return nil, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found ddl event message")
	}
	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	value := b.valueBytes[8 : valueLen+8]
	b.valueBytes = b.valueBytes[valueLen+8:]
	ddlMsg := new(messageDDL)
	if err := ddlMsg.decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	ddlEvent := msgToDDLEvent(b.nextKey, ddlMsg)
	b.nextKey = nil
	return ddlEvent, nil
}

func (b *BatchDecoder) hasNext() bool {
	return len(b.keyBytes) > 0 && len(b.valueBytes) > 0
}

func (b *BatchDecoder) decodeNextKey() error {
	keyLen := binary.BigEndian.Uint64(b.keyBytes[:8])
	key := b.keyBytes[8 : keyLen+8]
	msgKey := new(internal.MessageKey)
	err := msgKey.Decode(key)
	if err != nil {
		return errors.Trace(err)
	}
	b.nextKey = msgKey
	b.nextKeyLen = keyLen
	return nil
}

// NewBatchDecoder creates a new BatchDecoder.
func NewBatchDecoder(key []byte, value []byte) (codec.EventBatchDecoder, error) {
	version := binary.BigEndian.Uint64(key[:8])
	key = key[8:]
	if version != codec.BatchVersion1 {
		return nil, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("unexpected key format version")
	}
	// if only decode one byte slice, we choose MixedDecoder
	if len(key) > 0 && len(value) == 0 {
		return &BatchMixedDecoder{
			mixedBytes: key,
		}, nil
	}
	return &BatchDecoder{
		keyBytes:   key,
		valueBytes: value,
	}, nil
}
