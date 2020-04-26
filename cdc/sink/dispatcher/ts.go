package dispatcher

import "github.com/pingcap/ticdc/cdc/model"

type tsDispatcher struct {
	partitionNum int32
}

func (t *tsDispatcher) Dispatch(row *model.RowChangedEvent) int32 {
	return int32(row.CRTs % uint64(t.partitionNum))
}
