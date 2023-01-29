package pdutil

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestGetSourceID(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	domain, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domain.Close()
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "set @@global.tidb_source_id=2;")
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		client := store.(kv.StorageWithPD).GetPDClient()
		sourceID, err := GetSourceID(context.Background(), client)
		require.NoError(t, err)
		if sourceID == 1 {
			continue
		}
		require.Equal(t, uint64(2), sourceID)
		return
	}
}
