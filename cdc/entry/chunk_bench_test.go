package entry

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

type prepareData struct {
	mu        sync.Mutex
	tableInfo *model.TableInfo
	rawKv     *model.RawKVEntry
	isReady   atomic.Bool
}

var pData = &prepareData{}

func prepare() {
	replicaConfig := config.GetDefaultReplicaConfig()
	t := &testing.T{}
	helper := NewSchemaTestHelperWithReplicaConfig(t, replicaConfig)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	sql := `create table t (id int primary key, name varchar(255))`
	_ = helper.DDL2Event(sql)
	rawKV := helper.DML2RawKV(`insert into t (id, name) values (1, "jiangjiang")`, "test", "t")

	tableInfo, _ := helper.schemaStorage.GetLastSnapshot().TableByName("test", "t")
	log.Info("fizz tableInfo", zap.Any("tableName", tableInfo.Name))
	pData.mu.Lock()
	defer pData.mu.Unlock()
	pData.tableInfo = tableInfo
	pData.rawKv = rawKV
	pData.isReady.Store(true)
}

func getPData() *prepareData {
	if !pData.isReady.Load() {
		prepare()
	}
	return pData
}

func BenchmarkRawKVToChunk(b *testing.B) {
	tz, err := util.GetTimezone(config.GetGlobalServerConfig().TZ)
	if err != nil {
		log.Panic("invalid timezone", zap.Error(err))
	}
	pData := getPData()
	for i := 0; i < b.N; i++ {
		_ = rawKVToChunk(pData.rawKv, pData.tableInfo, tz)
	}
}
