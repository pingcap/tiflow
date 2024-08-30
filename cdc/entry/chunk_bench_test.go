package entry

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/integrity"
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

func BenchmarkRawKV_To_Chunk_1RowTxn(b *testing.B) {
	tz, err := util.GetTimezone(config.GetGlobalServerConfig().TZ)
	if err != nil {
		log.Panic("invalid timezone", zap.Error(err))
	}
	pData := getPData()
	for i := 0; i < b.N; i++ {
		_ = rawKVToChunk(pData.rawKv, pData.tableInfo, tz, 1)
	}
}

func BenchmarkRawKV_To_RowKVEntry_1RowTxn(b *testing.B) {
	pData := getPData()
	base := baseKVEntry{}
	m := mounter{}
	for i := 0; i < b.N; i++ {
		_, _ = m.unmarshalRowKVEntry(pData.tableInfo, pData.rawKv.Key, pData.rawKv.Value, nil, base)
	}
}

func BenchmarkRawKV_To_Chunk_100RowTxn(b *testing.B) {
	tz, err := util.GetTimezone(config.GetGlobalServerConfig().TZ)
	if err != nil {
		log.Panic("invalid timezone", zap.Error(err))
	}
	pData := getPData()
	for i := 0; i < b.N; i++ {
		_ = rawKVToChunk(pData.rawKv, pData.tableInfo, tz, 100)
	}
}

func BenchmarkRawKV_To_RowKVEntry_100RowTxn(b *testing.B) {
	pData := getPData()
	base := baseKVEntry{}
	m := mounter{}
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			_, _ = m.unmarshalRowKVEntry(pData.tableInfo, pData.rawKv.Key, pData.rawKv.Value, nil, base)
		}
	}
}

func BenchmarkRawKV_To_Chunk_To_Value_100RowTxn(b *testing.B) {
	tz, err := util.GetTimezone(config.GetGlobalServerConfig().TZ)
	if err != nil {
		log.Panic("invalid timezone", zap.Error(err))
	}
	pData := getPData()
	for i := 0; i < b.N; i++ {
		chk := rawKVToChunk(pData.rawKv, pData.tableInfo, tz, 100)
		_ = chunkToRows(chk, pData.tableInfo)
	}
}

func BenchmarkRawKV_To_RowKVEntry_To_Value_100RowTxn(b *testing.B) {
	pData := getPData()
	base := baseKVEntry{}
	m := mounter{}
	m.integrity = &integrity.Config{}
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			row, _ := m.unmarshalRowKVEntry(pData.tableInfo, pData.rawKv.Key, pData.rawKv.Value, nil, base)
			m.mountRowKVEntry(pData.tableInfo, row, pData.rawKv.Key, pData.rawKv.ApproximateDataSize())
		}
	}
}
