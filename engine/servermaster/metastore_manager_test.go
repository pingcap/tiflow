package servermaster

import (
	"testing"

	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	pkgOrm "github.com/hanfei1991/microcosm/pkg/orm"
	"github.com/stretchr/testify/require"
)

func TestMetaStoreManager(t *testing.T) {
	t.Parallel()

	storeConf := &metaclient.StoreConfigParams{
		Endpoints: []string{
			"127.0.0.1",
			"127.0.0.2",
		},
	}

	manager := NewMetaStoreManager()
	err := manager.Register("root", storeConf)
	require.Nil(t, err)

	err = manager.Register("root", &metaclient.StoreConfigParams{})
	require.Error(t, err)

	store := manager.GetMetaStore("default")
	require.Nil(t, store)

	store = manager.GetMetaStore("root")
	require.NotNil(t, store)
	require.Equal(t, storeConf, store)

	manager.UnRegister("root")
	store = manager.GetMetaStore("root")
	require.Nil(t, store)
}

func TestDefaultMetaStoreManager(t *testing.T) {
	t.Parallel()

	store := NewFrameMetaConfig()
	require.Equal(t, metaclient.FrameMetaID, store.StoreID)
	require.Equal(t, pkgOrm.DefaultFrameMetaEndpoints, store.Endpoints[0])

	store = NewDefaultUserMetaConfig()
	require.Equal(t, metaclient.DefaultUserMetaID, store.StoreID)
	require.Equal(t, metaclient.DefaultUserMetaEndpoints, store.Endpoints[0])
}
