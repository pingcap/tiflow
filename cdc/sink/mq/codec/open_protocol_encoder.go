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
	"bytes"
	"context"
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// OpenProtocolBatchEncoder encodes the events into the byte of a batch into.
type OpenProtocolBatchEncoder struct {
	messageBuf   []*MQMessage
	callbackBuff []func()
	curBatchSize int

	// configs
	maxMessageBytes int
	maxBatchSize    int
}

// GetMaxMessageBytes is only for unit testing.
func (d *OpenProtocolBatchEncoder) GetMaxMessageBytes() int {
	return d.maxMessageBytes
}

// GetMaxBatchSize is only for unit testing.
func (d *OpenProtocolBatchEncoder) GetMaxBatchSize() int {
	return d.maxBatchSize
}

// AppendRowChangedEvent implements the EventBatchEncoder interface
func (d *OpenProtocolBatchEncoder) AppendRowChangedEvent(
	_ context.Context,
	_ string,
	e *model.RowChangedEvent,
	callback func(),
) error {
	keyMsg, valueMsg := rowChangeToMsg(e)
	key, err := keyMsg.encode()
	if err != nil {
		return errors.Trace(err)
	}
	value, err := valueMsg.encode()
	if err != nil {
		return errors.Trace(err)
	}

	var keyLenByte [8]byte
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	var valueLenByte [8]byte
	binary.BigEndian.PutUint64(valueLenByte[:], uint64(len(value)))

	// for single message that longer than max-message-size, do not send it.
	// 16 is the length of `keyLenByte` and `valueLenByte`, 8 is the length of `versionHead`
	length := len(key) + len(value) + MaxRecordOverhead + 16 + 8
	if length > d.maxMessageBytes {
		log.Warn("Single message too large",
			zap.Int("max-message-size", d.maxMessageBytes), zap.Int("length", length), zap.Any("table", e.Table))
		return cerror.ErrOpenProtocolCodecRowTooLarge.GenWithStackByArgs()
	}

	if len(d.messageBuf) == 0 ||
		d.curBatchSize >= d.maxBatchSize ||
		d.messageBuf[len(d.messageBuf)-1].Length()+len(key)+len(value)+16 > d.maxMessageBytes {
		// Before we create a new message, we should handle the previous callbacks.
		d.tryBuildCallback()
		versionHead := make([]byte, 8)
		binary.BigEndian.PutUint64(versionHead, BatchVersion1)
		msg := newMsg(config.ProtocolOpen, versionHead, nil, 0, model.MessageTypeRow, nil, nil)
		d.messageBuf = append(d.messageBuf, msg)
		d.curBatchSize = 0
	}

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
func (d *OpenProtocolBatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*MQMessage, error) {
	keyMsg, valueMsg := ddlEventToMsg(e)
	key, err := keyMsg.encode()
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
	binary.BigEndian.PutUint64(versionByte[:], BatchVersion1)
	keyBuf.Write(versionByte[:])
	keyBuf.Write(keyLenByte[:])
	keyBuf.Write(key)

	valueBuf := new(bytes.Buffer)
	valueBuf.Write(valueLenByte[:])
	valueBuf.Write(value)

	ret := newDDLMsg(config.ProtocolOpen, keyBuf.Bytes(), valueBuf.Bytes(), e)
	return ret, nil
}

// EncodeCheckpointEvent implements the EventBatchEncoder interface
func (d *OpenProtocolBatchEncoder) EncodeCheckpointEvent(ts uint64) (*MQMessage, error) {
	keyMsg := newResolvedMessage(ts)
	key, err := keyMsg.encode()
	if err != nil {
		return nil, errors.Trace(err)
	}

	var keyLenByte [8]byte
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	var valueLenByte [8]byte
	binary.BigEndian.PutUint64(valueLenByte[:], 0)

	keyBuf := new(bytes.Buffer)
	var versionByte [8]byte
	binary.BigEndian.PutUint64(versionByte[:], BatchVersion1)
	keyBuf.Write(versionByte[:])
	keyBuf.Write(keyLenByte[:])
	keyBuf.Write(key)

	valueBuf := new(bytes.Buffer)
	valueBuf.Write(valueLenByte[:])

	ret := newResolvedMsg(config.ProtocolOpen, keyBuf.Bytes(), valueBuf.Bytes(), ts)
	return ret, nil
}

// Build implements the EventBatchEncoder interface
func (d *OpenProtocolBatchEncoder) Build() (mqMessages []*MQMessage) {
	d.tryBuildCallback()
	ret := d.messageBuf
	d.messageBuf = make([]*MQMessage, 0)
	return ret
}

// tryBuildCallback will collect all the callbacks into one message's callback.
func (d *OpenProtocolBatchEncoder) tryBuildCallback() {
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

type openProtocolBatchEncoderBuilder struct {
	config *Config
}

// Build a OpenProtocolBatchEncoder
func (b *openProtocolBatchEncoderBuilder) Build() EventBatchEncoder {
	encoder := newOpenProtocolBatchEncoder()
	encoder.(*OpenProtocolBatchEncoder).maxMessageBytes = b.config.maxMessageBytes
	encoder.(*OpenProtocolBatchEncoder).maxBatchSize = b.config.maxBatchSize

	return encoder
}

func newOpenProtocolBatchEncoderBuilder(config *Config) EncoderBuilder {
	return &openProtocolBatchEncoderBuilder{config: config}
}

// newOpenProtocolBatchEncoder creates a new OpenProtocolBatchEncoder.
func newOpenProtocolBatchEncoder() EventBatchEncoder {
	batch := &OpenProtocolBatchEncoder{}
	return batch
}
