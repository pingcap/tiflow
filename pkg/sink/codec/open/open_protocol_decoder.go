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
	"context"
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/internal"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// BatchMixedDecoder decodes the byte of a batch into the original messages.
type BatchMixedDecoder struct {
	mixedBytes []byte
	nextKey    *internal.MessageKey
	nextKeyLen uint64
}

// HasNext implements the RowEventDecoder interface
func (b *BatchMixedDecoder) HasNext() (model.MessageType, bool, error) {
	if !b.hasNext() {
		return 0, false, nil
	}
	if err := b.decodeNextKey(); err != nil {
		return 0, false, err
	}
	return b.nextKey.Type, true, nil
}

// NextResolvedEvent implements the RowEventDecoder interface
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

// NextRowChangedEvent implements the RowEventDecoder interface
func (b *BatchMixedDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
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

// NextDDLEvent implements the RowEventDecoder interface
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

	storage storage.ExternalStorage
}

// HasNext implements the RowEventDecoder interface
func (b *BatchDecoder) HasNext() (model.MessageType, bool, error) {
	if !b.hasNext() {
		return 0, false, nil
	}
	if err := b.decodeNextKey(); err != nil {
		return 0, false, err
	}
	return b.nextKey.Type, true, nil
}

// NextResolvedEvent implements the RowEventDecoder interface
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

func (b *BatchDecoder) assembleEventFromClaimCheckStorage() (*model.RowChangedEvent, error) {
	data, err := b.storage.ReadFile(context.Background(), b.nextKey.ClaimCheckLocation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	claimCheckM, err := common.UnmarshalClaimCheckMessage(data)
	if err != nil {
		return nil, errors.Trace(err)
	}

	version := binary.BigEndian.Uint64(claimCheckM.Key[:8])
	if version != codec.BatchVersion1 {
		return nil, cerror.ErrOpenProtocolCodecInvalidData.
			GenWithStack("unexpected key format version")
	}

	key := claimCheckM.Key[8:]
	keyLen := binary.BigEndian.Uint64(key[:8])
	key = key[8 : keyLen+8]
	msgKey := new(internal.MessageKey)
	if err := msgKey.Decode(key); err != nil {
		return nil, errors.Trace(err)
	}

	rowMsg := new(messageRow)
	valueLen := binary.BigEndian.Uint64(claimCheckM.Value[:8])
	value := claimCheckM.Value[8 : valueLen+8]
	if err := rowMsg.decode(value); err != nil {
		return nil, errors.Trace(err)
	}

	event := msgToRowChange(msgKey, rowMsg)
	b.nextKey = nil

	return event, nil
}

// NextRowChangedEvent implements the RowEventDecoder interface
func (b *BatchDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if b.nextKey == nil {
		if err := b.decodeNextKey(); err != nil {
			return nil, err
		}
	}
	b.keyBytes = b.keyBytes[b.nextKeyLen+8:]
	if b.nextKey.Type != model.MessageTypeRow {
		return nil, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found row event message")
	}

	// claim-check message found
	if b.nextKey.ClaimCheckLocation != "" {
		return b.assembleEventFromClaimCheckStorage()
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

// NextDDLEvent implements the RowEventDecoder interface
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
func NewBatchDecoder(ctx context.Context, replicaConfig *config.ReplicaConfig) (codec.RowEventDecoder, error) {
	var (
		storage storage.ExternalStorage
		err     error
	)
	if replicaConfig.Sink.KafkaConfig.LargeMessageHandle.EnableClaimCheck() {
		storageURI := replicaConfig.Sink.KafkaConfig.LargeMessageHandle.ClaimCheckStorageURI
		storage, err = util.GetExternalStorageFromURI(ctx, storageURI)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
		}
		log.Info("claim-check enabled, set the external storage for the kafka consumer",
			zap.String("storageURI", storageURI))
	}
	return &BatchDecoder{
		storage: storage,
	}, nil

}

// AddKeyValue implements the RowEventDecoder interface
func (b *BatchDecoder) AddKeyValue(key, value []byte) error {
	if len(b.keyBytes) != 0 || len(b.valueBytes) != 0 {
		return cerror.ErrOpenProtocolCodecInvalidData.
			GenWithStack("decoder key and value not nil")
	}
	version := binary.BigEndian.Uint64(key[:8])
	key = key[8:]
	if version != codec.BatchVersion1 {
		return cerror.ErrOpenProtocolCodecInvalidData.
			GenWithStack("unexpected key format version")
	}

	b.keyBytes = key
	b.valueBytes = value

	return nil

}

// AddKeyValue implements the RowEventDecoder interface
func (b *BatchMixedDecoder) AddKeyValue(key, value []byte) error {
	if key != nil || value != nil {
		return cerror.ErrOpenProtocolCodecInvalidData.
			GenWithStack("decoder key and value not nil")
	}
	version := binary.BigEndian.Uint64(key[:8])
	key = key[8:]
	if version != codec.BatchVersion1 {
		return cerror.ErrOpenProtocolCodecInvalidData.
			GenWithStack("unexpected key format version")
	}

	b.mixedBytes = key
	return nil
}
