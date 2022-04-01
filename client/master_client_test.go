package client_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/servermaster"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
)

func TestMasterClient(t *testing.T) {
	test.SetGlobalTestFlag(true)
	defer test.SetGlobalTestFlag(false)

	ctx := context.Background()
	abnormalHost := "127.0.0.1:10003"
	join := []string{"127.0.0.1:10001", "127.0.0.1:10002", abnormalHost}

	for _, addr := range join {
		if addr == abnormalHost {
			continue
		}
		srv := &servermaster.Server{}
		_, err := mock.NewMasterServer(addr, srv)
		require.Nil(t, err)
	}

	mcli, err := client.NewMasterClient(ctx, join)
	require.Nil(t, err)
	require.Len(t, mcli.Endpoints(), 2)

	// dial to an abonrmal server master, will silent error
	mcli.UpdateClients(ctx, join, "")
	require.Len(t, mcli.Endpoints(), 2)

	// abnormal server master comes back
	srv := &servermaster.Server{}
	_, err = mock.NewMasterServer(abnormalHost, srv)
	require.Nil(t, err)
	mcli.UpdateClients(ctx, join, "")
	require.Len(t, mcli.Endpoints(), 3)
}
