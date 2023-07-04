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
	"bytes"
	"context"
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"go.uber.org/zap"
)

// BatchEncoder encodes the events into the byte of a batch into.
type BatchEncoder struct {
	messageBuf []*common.Message

	curBatch *batch
	config   *common.Config
}

func (d *BatchEncoder) buildMessageOnlyHandleKeyColumns(e *model.RowChangedEvent) ([]byte, []byte, error) {
	// set the `largeMessageOnlyHandleKeyColumns` to true to only encode handle key columns.
	keyMsg, valueMsg, err := rowChangeToMsg(e, d.config.DeleteOnlyHandleKeyColumns, true)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	key, err := keyMsg.Encode()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	value, err := valueMsg.encode(d.config.OnlyOutputUpdatedColumns)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	length := len(key) + len(value) + common.MaxRecordOverhead + 16 + 8
	if length > d.config.MaxMessageBytes {
		log.Warn("Single message is too large for open-protocol",
			zap.Int("maxMessageBytes", d.config.MaxMessageBytes),
			zap.Int("length", length),
			zap.Any("table", e.Table),
			zap.Any("key", key))
		return nil, nil, cerror.ErrMessageTooLarge.GenWithStackByArgs()
	}

	log.Warn("open-protocol: message too large, only send handle key columns",
		zap.Any("table", e.Table), zap.Uint64("commitTs", e.CommitTs))

	return key, value, nil
}

// AppendRowChangedEvent implements the RowEventEncoder interface
func (d *BatchEncoder) AppendRowChangedEvent(
	_ context.Context,
	_ string,
	e *model.RowChangedEvent,
	callback func(),
) error {
	keyMsg, valueMsg, err := rowChangeToMsg(e, d.config.DeleteOnlyHandleKeyColumns, false)
	if err != nil {
		return errors.Trace(err)
	}
	key, err := keyMsg.Encode()
	if err != nil {
		return errors.Trace(err)
	}
	value, err := valueMsg.encode(d.config.OnlyOutputUpdatedColumns)
	if err != nil {
		return errors.Trace(err)
	}

	// for single message that is longer than max-message-bytes
	// 16 is the length of `keyLenByte` and `valueLenByte`, 8 is the length of `versionHead`
	length := len(key) + len(value) + common.MaxRecordOverhead + 16 + 8
	if length > d.config.MaxMessageBytes {
		log.Warn("Single message is too large for open-protocol",
			zap.Int("maxMessageBytes", d.config.MaxMessageBytes),
			zap.Int("length", length),
			zap.Any("table", e.Table),
			zap.Any("key", key))
		if !d.config.LargeMessageOnlyHandleKeyColumns {
			return cerror.ErrMessageTooLarge.GenWithStackByArgs()
		}

		key, value, err = d.buildMessageOnlyHandleKeyColumns(e)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if d.curBatch.full(key, value) {
		d.b
		uildBatch()
		d.resetBatch()
	}

	d.curBatch.appendKeyValue(key, value, callback)
	return nil
}

// EncodeDDLEvent implements the RowEventEncoder interface
func (d *BatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*common.Message, error) {
	keyMsg, valueMsg := ddlEventToMsg(e)
	key, err := keyMsg.Encode()
	if err != nil {
		return nil, errors.Trace(err)
	}
	value, err := valueMsg.encode()
	if err != nil {
		return nil, errors.Trace(err)
	}

	var keyLenByte [8]byte
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	var valueLenByte [8]byte
	binary.BigEndian.PutUint64(valueLenByte[:], uint64(len(value)))

	keyBuf := new(bytes.Buffer)
	var versionByte [8]byte
	binary.BigEndian.PutUint64(versionByte[:], codec.BatchVersion1)
	keyBuf.Write(versionByte[:])
	keyBuf.Write(keyLenByte[:])
	keyBuf.Write(key)

	valueBuf := new(bytes.Buffer)
	valueBuf.Write(valueLenByte[:])
	valueBuf.Write(value)

	ret := common.NewDDLMsg(config.ProtocolOpen, keyBuf.Bytes(), valueBuf.Bytes(), e)
	return ret, nil
}

// EncodeCheckpointEvent implements the RowEventEncoder interface
func (d *BatchEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	keyMsg := newResolvedMessage(ts)
	key, err := keyMsg.Encode()
	if err != nil {
		return nil, errors.Trace(err)
	}

	var keyLenByte [8]byte
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	var valueLenByte [8]byte
	binary.BigEndian.PutUint64(valueLenByte[:], 0)

	keyBuf := new(bytes.Buffer)
	var versionByte [8]byte
	binary.BigEndian.PutUint64(versionByte[:], codec.BatchVersion1)
	keyBuf.Write(versionByte[:])
	keyBuf.Write(keyLenByte[:])
	keyBuf.Write(key)

	valueBuf := new(bytes.Buffer)
	valueBuf.Write(valueLenByte[:])

	ret := common.NewResolvedMsg(config.ProtocolOpen, keyBuf.Bytes(), valueBuf.Bytes(), ts)
	return ret, nil
}

// Build implements the RowEventEncoder interface
func (d *BatchEncoder) Build() (messages []*common.Message) {
	d.buildBatch()
	d.resetBatch()
	ret := d.messageBuf
	d.messageBuf = make([]*common.Message, 0)
	return ret
}

type batchEncoderBuilder struct {
	config *common.Config
}

// Build a BatchEncoder
func (b *batchEncoderBuilder) Build() codec.RowEventEncoder {
	return NewBatchEncoder(b.config)
}

// NewBatchEncoderBuilder creates an open-protocol batchEncoderBuilder.
func NewBatchEncoderBuilder(config *common.Config) codec.RowEventEncoderBuilder {
	return &batchEncoderBuilder{config: config}
}

// NewBatchEncoder creates a new BatchEncoder.
func NewBatchEncoder(config *common.Config) codec.RowEventEncoder {
	return &BatchEncoder{
		config:   config,
		curBatch: newBatch(config),
	}
}

type batch struct {
	message      *common.Message
	callbackBuff []func()

	config *common.Config
}

func newBatch(config *common.Config) *batch {
	batch := &batch{
		config: config,
	}
	batch.reset()
	return batch
}

func (b *batch) appendKeyValue(key, value []byte, callback func()) {
	var (
		keyLenByte   [8]byte
		valueLenByte [8]byte
	)
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	binary.BigEndian.PutUint64(valueLenByte[:], uint64(len(value)))

	b.message.Key = append(b.message.Key, keyLenByte[:]...)
	b.message.Key = append(b.message.Key, key...)

	b.message.Value = append(b.message.Value, valueLenByte[:]...)
	b.message.Value = append(b.message.Value, value...)

	b.callbackBuff = append(b.callbackBuff, callback)
}

func (b *batch) build() *common.Message {
	result := b.message
	callback := func() {
		for _, cb := range b.callbackBuff {
			cb()
		}
	}
	result.Callback = callback
	return result
}

func (b *batch) reset() {
	versionHead := make([]byte, 8)
	binary.BigEndian.PutUint64(versionHead, codec.BatchVersion1)
	b.message = common.NewMsg(config.ProtocolOpen, versionHead, nil, 0, model.MessageTypeRow, nil, nil)
	b.callbackBuff = make([]func(), 0, b.config.MaxBatchSize)
}

func (b *batch) full(key, value []byte) bool {
	if len(b.callbackBuff) >= b.config.MaxBatchSize {
		return true
	}

	return b.message.Length()+len(key)+len(value)+16 > b.config.MaxMessageBytes
}

func (d *BatchEncoder) buildBatch() {
	message := d.curBatch.build()
	d.messageBuf = append(d.messageBuf, message)
}

func (d *BatchEncoder) resetBatch() {
	d.curBatch.reset()
}
