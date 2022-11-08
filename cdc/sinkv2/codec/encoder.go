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
	"github.com/pingcap/tiflow/cdc/sinkv2/codec/common"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
)

const (
	// BatchVersion1 represents the version of batch format
	BatchVersion1 uint64 = 1
)

// EventBatchEncoder is an abstraction for events encoder
type EventBatchEncoder interface {
	// EncodeCheckpointEvent appends a checkpoint event into the batch.
	// This event will be broadcast to all partitions to signal a global checkpoint.
	EncodeCheckpointEvent(ts uint64) (*common.Message, error)
	// AppendRowChangedEvents appends multiple messages to the encoder, should be called before
	// call the build method to build message.
	AppendRowChangedEvents(context.Context, string, []*eventsink.RowChangeCallbackableEvent) error
	// EncodeDDLEvent appends a DDL event into the batch
	EncodeDDLEvent(e *model.DDLEvent) (*common.Message, error)
	// Build builds the batch and returns the bytes of key and value.
	Build() []*common.Message
}

// EncoderBuilder builds encoder with context.
type EncoderBuilder interface {
	Build() EventBatchEncoder
}
