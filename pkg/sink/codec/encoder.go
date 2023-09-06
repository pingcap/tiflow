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

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
)

const (
	// BatchVersion1 represents the version of batch format
	BatchVersion1 uint64 = 1
)

// DDLEventBatchEncoder is an abstraction for DDL event encoder.
type DDLEventBatchEncoder interface {
	// EncodeCheckpointEvent appends a checkpoint event into the batch.
	// This event will be broadcast to all partitions to signal a global checkpoint.
	EncodeCheckpointEvent(ts uint64) (*common.Message, error)
	// EncodeDDLEvent appends a DDL event into the batch
	EncodeDDLEvent(e *model.DDLEvent) (*common.Message, error)
}

// MessageBuilder is an abstraction to build message.
type MessageBuilder interface {
	// Build builds the batch and returns the bytes of key and value.
	// Should be called after `AppendRowChangedEvent`
	Build() []*common.Message
}

// RowEventEncoder is an abstraction for events encoder
type RowEventEncoder interface {
	DDLEventBatchEncoder
	// AppendRowChangedEvent appends a row changed event into the batch or buffer.
	AppendRowChangedEvent(context.Context, string, *model.RowChangedEvent, func()) error
	MessageBuilder
}

// RowEventEncoderBuilder builds row encoder with context.
type RowEventEncoderBuilder interface {
	Build() RowEventEncoder
	CleanMetrics()
}

// TxnEventEncoder is an abstraction for txn events encoder.
type TxnEventEncoder interface {
	// AppendTxnEvent append a txn event into the buffer.
	AppendTxnEvent(*model.SingleTableTxn, func()) error
	MessageBuilder
}

// TxnEventEncoderBuilder builds txn encoder with context.
type TxnEventEncoderBuilder interface {
	Build() TxnEventEncoder
}

// IsColumnValueEqual checks whether the preValue and updatedValue are equal.
func IsColumnValueEqual(preValue, updatedValue interface{}) bool {
	if preValue == nil || updatedValue == nil {
		return preValue == updatedValue
	}

	preValueBytes, ok1 := preValue.([]byte)
	updatedValueBytes, ok2 := updatedValue.([]byte)
	if ok1 && ok2 {
		return bytes.Equal(preValueBytes, updatedValueBytes)
	}
	// mounter use the same table info to parse the value,
	// the value type should be the same
	return preValue == updatedValue
}
