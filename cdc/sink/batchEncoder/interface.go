package batchEncoder

import "github.com/pingcap/ticdc/cdc/model"

type EventBatchEncoder interface {
	AppendResolvedEvent(ts uint64) error
	AppendRowChangedEvent(e *model.RowChangedEvent) error
	AppendDDLEvent(e *model.DDLEvent) error
	Build() (key []byte, value []byte)
	Size() int
}
