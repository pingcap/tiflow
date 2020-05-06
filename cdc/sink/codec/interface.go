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
