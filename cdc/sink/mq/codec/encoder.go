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
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// EventBatchEncoder is an abstraction for events encoder
type EventBatchEncoder interface {
	// EncodeCheckpointEvent appends a checkpoint event into the batch.
	// This event will be broadcast to all partitions to signal a global checkpoint.
	EncodeCheckpointEvent(ts uint64) (*MQMessage, error)
	// AppendRowChangedEvent appends the calling context, a row changed event and the dispatch
	// topic into the batch
	AppendRowChangedEvent(context.Context, string, *model.RowChangedEvent) error
	// EncodeDDLEvent appends a DDL event into the batch
	EncodeDDLEvent(e *model.DDLEvent) (*MQMessage, error)
	// Build builds the batch and returns the bytes of key and value.
	Build() []*MQMessage
}

// EncoderBuilder builds encoder with context.
type EncoderBuilder interface {
	Build() EventBatchEncoder
}

// NewEventBatchEncoderBuilder returns an EncoderBuilder
func NewEventBatchEncoderBuilder(ctx context.Context, c *Config) (EncoderBuilder, error) {
	switch c.protocol {
	case config.ProtocolDefault, config.ProtocolOpen:
		return newJSONEventBatchEncoderBuilder(c), nil
	case config.ProtocolCanal:
		return newCanalEventBatchEncoderBuilder(), nil
	case config.ProtocolAvro:
		return newAvroEventBatchEncoderBuilder(ctx, c)
	case config.ProtocolMaxwell:
		return newMaxwellEventBatchEncoderBuilder(), nil
	case config.ProtocolCanalJSON:
		return newCanalFlatEventBatchEncoderBuilder(c), nil
	case config.ProtocolCraft:
		return newCraftEventBatchEncoderBuilder(c), nil
	default:
		return nil, cerror.ErrMQSinkUnknownProtocol.GenWithStackByArgs(c.protocol)
	}
}
