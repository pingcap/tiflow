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

package codec

import (
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
)

// EventBatchEncoder is an abstraction for events encoder
type EventBatchEncoder interface {
	// EncodeCheckpointEvent appends a checkpoint event into the batch.
	// This event will be broadcast to all partitions to signal a global checkpoint.
	EncodeCheckpointEvent(ts uint64) (*MQMessage, error)
	// AppendRowChangedEvent appends a row changed event into the batch
	AppendRowChangedEvent(e *model.RowChangedEvent) (EncoderResult, error)
	// AppendResolvedEvent appends a resolved event into the batch.
	// This event is used to tell the encoder that no event prior to ts will be sent.
	AppendResolvedEvent(ts uint64) (EncoderResult, error)
	// EncodeDDLEvent appends a DDL event into the batch
	EncodeDDLEvent(e *model.DDLEvent) (*MQMessage, error)
	// Build builds the batch and returns the bytes of key and value.
	Build() []*MQMessage
	// MixedBuild builds the batch and returns the bytes of mixed keys and values.
	// This is used for cdc log, to merge key and value into one byte slice
	// when first create file, we should set withVersion to true, to tell us that
	// the first 8 byte represents the encoder version
	// TODO decouple it out
	MixedBuild(withVersion bool) []byte
	// Size returns the size of the batch(bytes)
	// Deprecated: Size is deprecated
	Size() int
	// Reset reset the kv buffer
	Reset()
	// SetParams provides the encoder with more info on the sink
	SetParams(params map[string]string) error
}

// MQMessage represents an MQ message to the mqSink
type MQMessage struct {
	Key      []byte
	Value    []byte
	Ts       uint64              // reserved for possible output sorting
	Schema   *string             // schema
	Table    *string             // table
	Type     model.MqMessageType // type
	Protocol Protocol            // protocol
}

// Length returns the expected size of the Kafka message
func (m *MQMessage) Length() int {
	return len(m.Key) + len(m.Value)
}

// PhysicalTime returns physical time part of Ts in time.Time
func (m *MQMessage) PhysicalTime() time.Time {
	return oracle.GetTimeFromTS(m.Ts)
}

func newDDLMQMessage(proto Protocol, key, value []byte, event *model.DDLEvent) *MQMessage {
	return NewMQMessage(proto, key, value, event.CommitTs, model.MqMessageTypeDDL, &event.TableInfo.Schema, &event.TableInfo.Table)
}

func newResolvedMQMessage(proto Protocol, key, value []byte, ts uint64) *MQMessage {
	return NewMQMessage(proto, key, value, ts, model.MqMessageTypeResolved, nil, nil)
}

// NewMQMessage should be used when creating a MQMessage struct.
// It copies the input byte slices to avoid any surprises in asynchronous MQ writes.
func NewMQMessage(proto Protocol, key []byte, value []byte, ts uint64, ty model.MqMessageType, schema, table *string) *MQMessage {
	ret := &MQMessage{
		Key:      nil,
		Value:    nil,
		Ts:       ts,
		Schema:   schema,
		Table:    table,
		Type:     ty,
		Protocol: proto,
	}

	if key != nil {
		ret.Key = make([]byte, len(key))
		copy(ret.Key, key)
	}

	if value != nil {
		ret.Value = make([]byte, len(value))
		copy(ret.Value, value)
	}

	return ret
}

// EventBatchDecoder is an abstraction for events decoder
// this interface is only for testing now
type EventBatchDecoder interface {
	// HasNext returns
	//     1. the type of the next event
	//     2. a bool if the next event is exist
	//     3. error
	HasNext() (model.MqMessageType, bool, error)
	// NextResolvedEvent returns the next resolved event if exists
	NextResolvedEvent() (uint64, error)
	// NextRowChangedEvent returns the next row changed event if exists
	NextRowChangedEvent() (*model.RowChangedEvent, error)
	// NextDDLEvent returns the next DDL event if exists
	NextDDLEvent() (*model.DDLEvent, error)
}

// EncoderResult indicates an action request by the encoder to the mqSink
type EncoderResult uint8

// Enum types of EncoderResult
const (
	EncoderNoOperation EncoderResult = iota
	EncoderNeedAsyncWrite
	EncoderNeedSyncWrite
)

// Protocol is the protocol of the mq message
type Protocol int

// Enum types of the Protocol
const (
	ProtocolDefault Protocol = iota
	ProtocolCanal
	ProtocolAvro
	ProtocolMaxwell
	ProtocolCanalJSON
)

// FromString converts the protocol from string to Protocol enum type
func (p *Protocol) FromString(protocol string) {
	switch strings.ToLower(protocol) {
	case "default":
		*p = ProtocolDefault
	case "canal":
		*p = ProtocolCanal
	case "avro":
		*p = ProtocolAvro
	case "maxwell":
		*p = ProtocolMaxwell
	case "canal-json":
		*p = ProtocolCanalJSON
	default:
		*p = ProtocolDefault
		log.Warn("can't support codec protocol, using default protocol", zap.String("protocol", protocol))
	}
}

// NewEventBatchEncoder returns a function of creating an EventBatchEncoder
func NewEventBatchEncoder(p Protocol) func() EventBatchEncoder {
	switch p {
	case ProtocolDefault:
		return NewJSONEventBatchEncoder
	case ProtocolCanal:
		return NewCanalEventBatchEncoder
	case ProtocolAvro:
		return NewAvroEventBatchEncoder
	case ProtocolMaxwell:
		return NewMaxwellEventBatchEncoder
	case ProtocolCanalJSON:
		return NewCanalFlatEventBatchEncoder
	default:
		log.Warn("unknown codec protocol value of EventBatchEncoder", zap.Int("protocol_value", int(p)))
		return NewJSONEventBatchEncoder
	}
}
