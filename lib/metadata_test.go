package lib

import (
	"context"
	"testing"

	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/stretchr/testify/require"
)

func TestMasterMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	metaKVClient := metadata.NewMetaMock()
	meta := []*MasterMetaKVData{
		{
			ID: JobManagerUUID,
			Tp: JobManager,
		},
		{
			ID: "master-1",
			Tp: FakeJobMaster,
		},
		{
			ID: "master-2",
			Tp: FakeJobMaster,
		},
	}
	for _, data := range meta {
		cli := NewMasterMetadataClient(data.ID, metaKVClient)
		err := cli.Store(ctx, data)
		require.Nil(t, err)
	}
	cli := NewMasterMetadataClient("job-manager", metaKVClient)
	masters, err := cli.LoadAllMasters(ctx)
	require.Nil(t, err)
	require.Len(t, masters, 2)
	for _, master := range masters {
		require.Equal(t, FakeJobMaster, master.Tp)
	}
}

func TestStoreMasterMetadata(t *testing.T) {
	t.Parallel()
	var (
		ctx          = context.Background()
		metaKVClient = metadata.NewMetaMock()
		addr1        = "127.0.0.1:10000"
		addr2        = "127.0.0.1:10001"
		meta         = &MasterMetaKVData{
			ID:   "master-1",
			Tp:   FakeJobMaster,
			Addr: addr1,
		}
	)
	loadMeta := func() *MasterMetaKVData {
		cli := NewMasterMetadataClient(meta.ID, metaKVClient)
		meta, err := cli.Load(ctx)
		require.NoError(t, err)
		return meta
	}

	// persist master meta for the first time
	err := StoreMasterMeta(ctx, metaKVClient, meta)
	require.NoError(t, err)
	require.Equal(t, addr1, loadMeta().Addr)

	// overwrite master meta
	meta.Addr = addr2
	err = StoreMasterMeta(ctx, metaKVClient, meta)
	require.NoError(t, err)
	require.Equal(t, addr2, loadMeta().Addr)
}
