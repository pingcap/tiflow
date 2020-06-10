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

//compatible with canal-1.1.4

package codec

import (
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/ticdc/cdc/model"
	canal "github.com/pingcap/ticdc/proto/canal"
)

const (
	CanalPacketVersion int32 = 1
)

type CanalEventBatchEncoder struct {
	messages *canal.Messages
	packet   *canal.Packet
}

// AppendResolvedEvent implements the EventBatchEncoder interface
func (d *CanalEventBatchEncoder) AppendResolvedEvent(ts uint64) error {
	// For canal now, there is no such a corresponding type to ResolvedEvent so far.
	// Therefore the event is ignored.
	return nil
}

// AppendRowChangedEvent implements the EventBatchEncoder interface
func (d *CanalEventBatchEncoder) AppendRowChangedEvent(e *model.RowChangedEvent) error {
	panic("implement me")
}

// AppendDDLEvent implements the EventBatchEncoder interface
func (d *CanalEventBatchEncoder) AppendDDLEvent(e *model.DDLEvent) error {
	panic("implement me")
}

// Build implements the EventBatchEncoder interface
func (d *CanalEventBatchEncoder) Build() (key []byte, value []byte) {
	err := d.refreshPacketBody()
	if err != nil {
		panic(err)
	}
	value, err = proto.Marshal(d.packet)
	if err != nil {
		panic(err)
	}
	return nil, value
}

// Size implements the EventBatchEncoder interface
func (d *CanalEventBatchEncoder) Size() int {
	// TODO: avoid marshaling the messages every time for calculating the size of the packet
	err := d.refreshPacketBody()
	if err != nil {
		panic(err)
	}
	return proto.Size(d.packet)
}

// refreshPacketBody() marshals the messages to the packet body
func (d *CanalEventBatchEncoder) refreshPacketBody() error {
	oldSize := len(d.packet.Body)
	newSize := proto.Size(d.messages)
	if newSize > oldSize {
		// resize packet body slice
		d.packet.Body = append(d.packet.Body, make([]byte, newSize-oldSize)...)
	}
	_, err := d.messages.MarshalToSizedBuffer(d.packet.Body[:newSize])
	return err
}

// NewCanalEventBatchEncoder creates a new CanalEventBatchEncoder.
func NewCanalEventBatchEncoder() EventBatchEncoder {
	p := &canal.Packet{
		VersionPresent: &canal.Packet_Version{
			Version: CanalPacketVersion,
		},
		Type: canal.PacketType_MESSAGES,
	}

	encoder := &CanalEventBatchEncoder{
		messages: &canal.Messages{},
		packet:   p,
	}
	return encoder
}
