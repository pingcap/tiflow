package dispatcher

import "github.com/pingcap/ticdc/cdc/model"

type rowIDDispatcher struct {
	partitionNum int32
}

func (r *rowIDDispatcher) Dispatch(row *model.RowChangedEvent) int32 {
	return int32(uint64(row.RowID) % uint64(r.partitionNum))
}
