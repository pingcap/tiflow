package codec

import "github.com/pingcap/ticdc/cdc/model"

type EventBatchEncoder interface {
	AppendResolvedEvent(ts uint64) error
	AppendRowChangedEvent(e *model.RowChangedEvent) error
	AppendDDLEvent(e *model.DDLEvent) error
	Build() (key []byte, value []byte)
	Size() int
}

type EventBatchDecoder interface {
	HasNext() (model.MqMessageType, bool, error)
	NextResolvedEvent() (uint64, error)
	NextRowChangedEvent() (*model.RowChangedEvent, error)
	NextDDLEvent() (*model.DDLEvent, error)
}
