package entry

import (
	"testing"
	"time"

	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
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
	chk := rawKVToChunk(rawKV, tableInfo, tz)
	require.NotNil(t, chk)
	require.Equal(t, 2, chk.NumCols())
	require.Equal(t, 1, chk.NumRows())
	for i := 0; i < 2; i++ {
		_ = rawKVToChunk(rawKV, tableInfo, tz)
	}
	t.Logf("fizz time: %v", time.Since(start))
}
