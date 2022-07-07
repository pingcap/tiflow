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

package codec

import (
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// OpenProtocolBatchMixedDecoder decodes the byte of a batch into the original messages.
type OpenProtocolBatchMixedDecoder struct {
	mixedBytes []byte
	nextKey    *mqMessageKey
	nextKeyLen uint64
}

// HasNext implements the EventBatchDecoder interface
func (b *OpenProtocolBatchMixedDecoder) HasNext() (model.MessageType, bool, error) {
	if !b.hasNext() {
		return 0, false, nil
	}
	if err := b.decodeNextKey(); err != nil {
		return 0, false, err
	}
	return b.nextKey.Type, true, nil
}

// NextResolvedEvent implements the EventBatchDecoder interface
func (b *OpenProtocolBatchMixedDecoder) NextResolvedEvent() (uint64, error) {
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
func (b *OpenProtocolBatchMixedDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
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
	rowMsg := new(mqMessageRow)
	if err := rowMsg.decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	rowEvent := msgToRowChange(b.nextKey, rowMsg)
	b.nextKey = nil
	return rowEvent, nil
}

// NextDDLEvent implements the EventBatchDecoder interface
func (b *OpenProtocolBatchMixedDecoder) NextDDLEvent() (*model.DDLEvent, error) {
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
	ddlMsg := new(mqMessageDDL)
	if err := ddlMsg.decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	ddlEvent := msgToDDLEvent(b.nextKey, ddlMsg)
	b.nextKey = nil
	return ddlEvent, nil
}

func (b *OpenProtocolBatchMixedDecoder) hasNext() bool {
	return len(b.mixedBytes) > 0
}

func (b *OpenProtocolBatchMixedDecoder) decodeNextKey() error {
	keyLen := binary.BigEndian.Uint64(b.mixedBytes[:8])
	key := b.mixedBytes[8 : keyLen+8]
	// drop value bytes
	msgKey := new(mqMessageKey)
	err := msgKey.decode(key)
	if err != nil {
		return errors.Trace(err)
	}
	b.nextKey = msgKey
	b.nextKeyLen = keyLen
	return nil
}

// OpenProtocolBatchDecoder decodes the byte of a batch into the original messages.
type OpenProtocolBatchDecoder struct {
	keyBytes   []byte
	valueBytes []byte
	nextKey    *mqMessageKey
	nextKeyLen uint64
}

// HasNext implements the EventBatchDecoder interface
func (b *OpenProtocolBatchDecoder) HasNext() (model.MessageType, bool, error) {
	if !b.hasNext() {
		return 0, false, nil
	}
	if err := b.decodeNextKey(); err != nil {
		return 0, false, err
	}
	return b.nextKey.Type, true, nil
}

// NextResolvedEvent implements the EventBatchDecoder interface
func (b *OpenProtocolBatchDecoder) NextResolvedEvent() (uint64, error) {
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
func (b *OpenProtocolBatchDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
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
	rowMsg := new(mqMessageRow)
	if err := rowMsg.decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	rowEvent := msgToRowChange(b.nextKey, rowMsg)
	b.nextKey = nil
	return rowEvent, nil
}

// NextDDLEvent implements the EventBatchDecoder interface
func (b *OpenProtocolBatchDecoder) NextDDLEvent() (*model.DDLEvent, error) {
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
	ddlMsg := new(mqMessageDDL)
	if err := ddlMsg.decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	ddlEvent := msgToDDLEvent(b.nextKey, ddlMsg)
	b.nextKey = nil
	return ddlEvent, nil
}

func (b *OpenProtocolBatchDecoder) hasNext() bool {
	return len(b.keyBytes) > 0 && len(b.valueBytes) > 0
}

func (b *OpenProtocolBatchDecoder) decodeNextKey() error {
	keyLen := binary.BigEndian.Uint64(b.keyBytes[:8])
	key := b.keyBytes[8 : keyLen+8]
	msgKey := new(mqMessageKey)
	err := msgKey.decode(key)
	if err != nil {
		return errors.Trace(err)
	}
	b.nextKey = msgKey
	b.nextKeyLen = keyLen
	return nil
}

// NewOpenProtocolBatchDecoder creates a new OpenProtocolBatchDecoder.
func NewOpenProtocolBatchDecoder(key []byte, value []byte) (EventBatchDecoder, error) {
	version := binary.BigEndian.Uint64(key[:8])
	key = key[8:]
	if version != BatchVersion1 {
		return nil, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("unexpected key format version")
	}
	// if only decode one byte slice, we choose MixedDecoder
	if len(key) > 0 && len(value) == 0 {
		return &OpenProtocolBatchMixedDecoder{
			mixedBytes: key,
		}, nil
	}
	return &OpenProtocolBatchDecoder{
		keyBytes:   key,
		valueBytes: value,
	}, nil
}
