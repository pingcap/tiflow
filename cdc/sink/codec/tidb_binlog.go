package codec

import (
	"github.com/pingcap/ticdc/cdc/sink/common"
	pb "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
)

type TiDBBinlogEventBatchEncoder struct {
	message  *pb.Binlog
	txnCache *common.UnresolvedTxnCache
}
