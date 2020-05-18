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

import "github.com/pingcap/ticdc/cdc/model"

// EventBatchEncoder is an abstraction for events encoder
type EventBatchEncoder interface {
	// AppendResolvedEvent appends a resolved event into the batch
	AppendResolvedEvent(ts uint64) error
	// AppendRowChangedEvent appends a row changed event into the batch
	AppendRowChangedEvent(e *model.RowChangedEvent) error
	// AppendDDLEvent appends a DDL event into the batch
	AppendDDLEvent(e *model.DDLEvent) error
	// Build builds the batch and returns the bytes of key and value.
	Build() (key []byte, value []byte)
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
