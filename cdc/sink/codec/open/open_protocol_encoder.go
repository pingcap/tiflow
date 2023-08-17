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
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// BatchEncoder encodes the events into the byte of a batch into.
type BatchEncoder struct {
	messageBuf   []*common.Message
	callbackBuff []func()
	curBatchSize int

	config *common.Config
}

func (d *BatchEncoder) buildMessageOnlyHandleKeyColumns(e *model.RowChangedEvent) ([]byte, []byte, error) {
	// set the `largeMessageOnlyHandleKeyColumns` to true to only encode handle key columns.
	keyMsg, valueMsg := rowChangeToMsg(e, d.config, true)
	key, err := keyMsg.Encode()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	value, err := valueMsg.encode()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	// for single message that is longer than max-message-bytes
	// 16 is the length of `keyLenByte` and `valueLenByte`, 8 is the length of `versionHead`
	length := len(key) + len(value) + common.MaxRecordOverhead + 16 + 8
	if length > d.config.MaxMessageBytes {
		log.Error("Single message is too large for open-protocol, only encode handle key columns",
			zap.Int("maxMessageBytes", d.config.MaxMessageBytes),
			zap.Int("length", length),
			zap.Any("table", e.Table),
			zap.Any("key", key))
		return nil, nil, cerror.ErrMessageTooLarge.GenWithStackByArgs()
	}

	return key, value, nil
}

// AppendRowChangedEvent implements the EventBatchEncoder interface
func (d *BatchEncoder) AppendRowChangedEvent(
	_ context.Context,
	_ string,
	e *model.RowChangedEvent,
	callback func(),
) error {
	keyMsg, valueMsg := rowChangeToMsg(e, d.config, false)
	key, err := keyMsg.Encode()
	if err != nil {
		return errors.Trace(err)
	}
	value, err := valueMsg.encode()
	if err != nil {
		return errors.Trace(err)
	}

	// for single message that is longer than max-message-bytes, do not send it.
	// 16 is the length of `keyLenByte` and `valueLenByte`, 8 is the length of `versionHead`
	length := len(key) + len(value) + common.MaxRecordOverhead + 16 + 8
	if length > d.config.MaxMessageBytes {
		if d.config.LargeMessageHandle.Disabled() {
			log.Error("Single message is too large for open-protocol",
				zap.Int("maxMessageBytes", d.config.MaxMessageBytes),
				zap.Int("length", length),
				zap.Any("table", e.Table),
				zap.Any("key", key))
			return cerror.ErrMessageTooLarge.GenWithStackByArgs()
		}
		key, value, err = d.buildMessageOnlyHandleKeyColumns(e)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if len(d.messageBuf) == 0 ||
		d.curBatchSize >= d.config.MaxBatchSize ||
		d.messageBuf[len(d.messageBuf)-1].Length()+len(key)+len(value)+16 > d.config.MaxMessageBytes {
		// Before we create a new message, we should handle the previous callbacks.
		d.tryBuildCallback()
		versionHead := make([]byte, 8)
		binary.BigEndian.PutUint64(versionHead, codec.BatchVersion1)
		msg := common.NewMsg(config.ProtocolOpen, versionHead, nil, 0, model.MessageTypeRow, nil, nil)
		d.messageBuf = append(d.messageBuf, msg)
		d.curBatchSize = 0
	}

	var (
		keyLenByte   [8]byte
		valueLenByte [8]byte
	)
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	binary.BigEndian.PutUint64(valueLenByte[:], uint64(len(value)))

	message := d.messageBuf[len(d.messageBuf)-1]
	message.Key = append(message.Key, keyLenByte[:]...)
	message.Key = append(message.Key, key...)
	message.Value = append(message.Value, valueLenByte[:]...)
	message.Value = append(message.Value, value...)
	message.Ts = e.CommitTs
	message.Schema = &e.Table.Schema
	message.Table = &e.Table.Table
	message.IncRowsCount()

	if callback != nil {
		d.callbackBuff = append(d.callbackBuff, callback)
	}

	d.curBatchSize++
	return nil
}

// EncodeDDLEvent implements the EventBatchEncoder interface
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

// EncodeCheckpointEvent implements the EventBatchEncoder interface
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

// Build implements the EventBatchEncoder interface
func (d *BatchEncoder) Build() (messages []*common.Message) {
	d.tryBuildCallback()
	ret := d.messageBuf
	d.messageBuf = make([]*common.Message, 0)
	return ret
}

// tryBuildCallback will collect all the callbacks into one message's callback.
func (d *BatchEncoder) tryBuildCallback() {
	if len(d.messageBuf) != 0 && len(d.callbackBuff) != 0 {
		lastMsg := d.messageBuf[len(d.messageBuf)-1]
		callbacks := d.callbackBuff
		lastMsg.Callback = func() {
			for _, cb := range callbacks {
				cb()
			}
		}
		d.callbackBuff = make([]func(), 0)
	}
}

type batchEncoderBuilder struct {
	config *common.Config
}

// Build a BatchEncoder
func (b *batchEncoderBuilder) Build() codec.EventBatchEncoder {
	return NewBatchEncoder(b.config)
}

// NewBatchEncoderBuilder creates an open-protocol batchEncoderBuilder.
func NewBatchEncoderBuilder(config *common.Config) codec.EncoderBuilder {
	return &batchEncoderBuilder{config: config}
}

// NewBatchEncoder creates a new BatchEncoder.
func NewBatchEncoder(config *common.Config) codec.EventBatchEncoder {
	return &BatchEncoder{
		config: config,
	}
}
