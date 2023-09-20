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

package canal

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	canal "github.com/pingcap/tiflow/proto/canal"
	"go.uber.org/zap"
)

// BatchEncoder encodes the events into the byte of a batch into.
type BatchEncoder struct {
	messages     *canal.Messages
	callbackBuf  []func()
	packet       *canal.Packet
	entryBuilder *canalEntryBuilder

	config *common.Config
}

// EncodeCheckpointEvent implements the RowEventEncoder interface
func (d *BatchEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	// For canal now, there is no such a corresponding type to ResolvedEvent so far.
	// Therefore, the event is ignored.
	return nil, nil
}

// AppendRowChangedEvent implements the RowEventEncoder interface
func (d *BatchEncoder) AppendRowChangedEvent(
	_ context.Context,
	_ string,
	e *model.RowChangedEvent,
	callback func(),
) error {
	entry, err := d.entryBuilder.fromRowEvent(e, d.config.DeleteOnlyHandleKeyColumns)
	if err != nil {
		return errors.Trace(err)
	}
	b, err := proto.Marshal(entry)
	if err != nil {
		return cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}
	d.messages.Messages = append(d.messages.Messages, b)
	if callback != nil {
		d.callbackBuf = append(d.callbackBuf, callback)
	}
	return nil
}

// EncodeDDLEvent implements the RowEventEncoder interface
func (d *BatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*common.Message, error) {
	entry, err := d.entryBuilder.fromDDLEvent(e)
	if err != nil {
		return nil, errors.Trace(err)
	}
	b, err := proto.Marshal(entry)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}

	messages := new(canal.Messages)
	messages.Messages = append(messages.Messages, b)
	b, err = messages.Marshal()
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}

	packet := &canal.Packet{
		VersionPresent: &canal.Packet_Version{
			Version: CanalPacketVersion,
		},
		Type: canal.PacketType_MESSAGES,
	}
	packet.Body = b
	b, err = packet.Marshal()
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}

	return common.NewDDLMsg(config.ProtocolCanal, nil, b, e), nil
}

// Build implements the RowEventEncoder interface
func (d *BatchEncoder) Build() []*common.Message {
	rowCount := len(d.messages.Messages)
	if rowCount == 0 {
		return nil
	}

	err := d.refreshPacketBody()
	if err != nil {
		log.Panic("Error when generating Canal packet", zap.Error(err))
	}

	value, err := proto.Marshal(d.packet)
	if err != nil {
		log.Panic("Error when serializing Canal packet", zap.Error(err))
	}
	ret := common.NewMsg(config.ProtocolCanal, nil, value, 0, model.MessageTypeRow, nil, nil)
	ret.SetRowsCount(rowCount)
	d.messages.Reset()
	d.resetPacket()

	if len(d.callbackBuf) != 0 && len(d.callbackBuf) == rowCount {
		callbacks := d.callbackBuf
		ret.Callback = func() {
			for _, cb := range callbacks {
				cb()
			}
		}
		d.callbackBuf = make([]func(), 0)
	}
	return []*common.Message{ret}
}

// refreshPacketBody() marshals the messages to the packet body
func (d *BatchEncoder) refreshPacketBody() error {
	oldSize := len(d.packet.Body)
	newSize := proto.Size(d.messages)
	if newSize > oldSize {
		// resize packet body slice
		d.packet.Body = append(d.packet.Body, make([]byte, newSize-oldSize)...)
	} else {
		d.packet.Body = d.packet.Body[:newSize]
	}

	_, err := d.messages.MarshalToSizedBuffer(d.packet.Body)
	return err
}

func (d *BatchEncoder) resetPacket() {
	d.packet = &canal.Packet{
		VersionPresent: &canal.Packet_Version{
			Version: CanalPacketVersion,
		},
		Type: canal.PacketType_MESSAGES,
	}
}

// newBatchEncoder creates a new canalBatchEncoder.
func newBatchEncoder(config *common.Config) codec.RowEventEncoder {
	encoder := &BatchEncoder{
		messages:     &canal.Messages{},
		callbackBuf:  make([]func(), 0),
		entryBuilder: newCanalEntryBuilder(),

		config: config,
	}

	encoder.resetPacket()
	return encoder
}

type batchEncoderBuilder struct {
	config *common.Config
}

// Build a `canalBatchEncoder`
func (b *batchEncoderBuilder) Build() codec.RowEventEncoder {
	return newBatchEncoder(b.config)
}

// NewBatchEncoderBuilder creates a canal batchEncoderBuilder.
func NewBatchEncoderBuilder(config *common.Config) codec.RowEventEncoderBuilder {
	return &batchEncoderBuilder{
		config: config,
	}
}
