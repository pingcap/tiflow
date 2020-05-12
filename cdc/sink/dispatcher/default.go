package dispatcher

import (
	"encoding/json"
	"hash/crc32"
	"log"

	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

type defaultDispatcher struct {
	partitionNum int32
}

func (d *defaultDispatcher) Dispatch(row *model.RowChangedEvent) int32 {
	hash := crc32.NewIEEE()
	if len(row.IndieMarkCol) == 0 {
		// distribute partition by table
		_, err := hash.Write([]byte(row.Table.Schema))
		if err != nil {
			log.Fatal("calculate hash of message key failed, please report a bug", zap.Error(err))
		}
		_, err = hash.Write([]byte(row.Table.Table))
		if err != nil {
			log.Fatal("calculate hash of message key failed, please report a bug", zap.Error(err))
		}
		return int32(hash.Sum32() % uint32(d.partitionNum))
	}
	// distribute partition by rowid or unique column value
	value := row.Columns[row.IndieMarkCol].Value
	b, err := json.Marshal(value)
	if err != nil {
		log.Fatal("calculate hash of message key failed, please report a bug", zap.Error(err))
	}
	_, err = hash.Write(b)
	if err != nil {
		log.Fatal("calculate hash of message key failed, please report a bug", zap.Error(err))
	}
	return int32(hash.Sum32() % uint32(d.partitionNum))
}
