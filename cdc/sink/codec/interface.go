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

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/cdc/model"
)

// EventBatchEncoder is an abstraction for events encoder
type EventBatchEncoder interface {
	// AppendResolvedEvent appends a resolved event into the batch
	AppendResolvedEvent(ts uint64) (EncoderResult, error)
	// AppendRowChangedEvent appends a row changed event into the batch
	AppendRowChangedEvent(e *model.RowChangedEvent) (EncoderResult, error)
	// AppendDDLEvent appends a DDL event into the batch
	AppendDDLEvent(e *model.DDLEvent) (EncoderResult, error)
	// UpdateResolvedTs help encoder use resolved ts to split row change event from resolved and unresolved
	UpdateResolvedTs(ts uint64) (EncoderResult, error)
	// Build return a list of <key value>, it always reset the encoder state based on implementation.
	Build() (keys [][]byte, values [][]byte)
	// MixedBuild builds the batch and returns the bytes of mixed keys and values.
	// This is used for cdc log, to merge key and value into one byte slice
	MixedBuild() []byte
	// Size returns the size of the batch(bytes)
	Size() int
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
	default:
		log.Warn("unknown codec protocol value of EventBatchEncoder", zap.Int("protocol_value", int(p)))
		return NewJSONEventBatchEncoder
	}
}
