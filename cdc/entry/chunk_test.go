package entry

import (
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRawKVToChunk(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()

	helper := NewSchemaTestHelperWithReplicaConfig(t, replicaConfig)
	defer helper.Close()

	helper.Tk().MustExec("set global tidb_enable_row_level_checksum = 1")
	helper.Tk().MustExec("use test")

	sql := `create table t (id int primary key, name varchar(255))`
	_ = helper.DDL2Event(sql)
	rawKV := helper.DML2RawKV(`insert into t (id, name) values (1, "jiangjiang")`, "test", "t")
	require.NotNil(t, rawKV)

	tableInfo, ok := helper.schemaStorage.GetLastSnapshot().TableByName("test", "t")
	require.True(t, ok)
	require.NotNil(t, tableInfo)
	tz, err := util.GetTimezone(config.GetGlobalServerConfig().TZ)
	require.NoError(t, err)
	start := time.Now()
	chk := rawKVToChunk(rawKV, tableInfo, tz, 100)
	require.NotNil(t, chk)
	require.Equal(t, 2, chk.NumCols())
	require.Equal(t, 100, chk.NumRows())

	// for i := 0; i < 100; i++ {
	// 	log.Info("fizz rawKv", zap.Any("rawKv", string(rawKV.Value)))
	// 	info := tableInfo.Clone()
	// 	_ = rawKVToChunk(rawKV, info, tz, 100)
	// }
	t.Logf("fizz time: %v", time.Since(start))
}

func TestChunkToRow(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()

	helper := NewSchemaTestHelperWithReplicaConfig(t, replicaConfig)
	defer helper.Close()

	helper.Tk().MustExec("set global tidb_enable_row_level_checksum = 1")
	helper.Tk().MustExec("use test")

	sql := `create table t (id int primary key, name varchar(255))`
	_ = helper.DDL2Event(sql)
	rawKV := helper.DML2RawKV(`insert into t (id, name) values (1, "jiangjiang")`, "test", "t")
	require.NotNil(t, rawKV)

	tableInfo, ok := helper.schemaStorage.GetLastSnapshot().TableByName("test", "t")
	require.True(t, ok)
	require.NotNil(t, tableInfo)
	tz, err := util.GetTimezone(config.GetGlobalServerConfig().TZ)
	require.NoError(t, err)
	chk := rawKVToChunk(rawKV, tableInfo, tz, 100)
	require.NotNil(t, chk)
	require.Equal(t, 2, chk.NumCols())
	require.Equal(t, 100, chk.NumRows())

	rows := chunkToRows(chk, tableInfo)
	require.Len(t, rows, 100)
	for _, row := range rows {
		require.Len(t, row.Columns, 2)
	}
	log.Info("fizz rows", zap.Stringer("10nd row", rows[10]))
}
