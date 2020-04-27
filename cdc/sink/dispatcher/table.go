package dispatcher

import (
	"hash/crc32"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

type tableDispatcher struct {
	partitionNum int32
}

func (t *tableDispatcher) Dispatch(row *model.RowChangedEvent) int32 {
	hash := crc32.NewIEEE()
	// distribute partition by table
	_, err := hash.Write([]byte(row.Schema))
	if err != nil {
		log.Fatal("calculate hash of message key failed, please report a bug", zap.Error(err))
	}
	_, err = hash.Write([]byte(row.Table))
	if err != nil {
		log.Fatal("calculate hash of message key failed, please report a bug", zap.Error(err))
	}
	return int32(hash.Sum32() % uint32(t.partitionNum))
}
