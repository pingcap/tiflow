// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package client_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/client"
	"github.com/pingcap/tiflow/engine/servermaster"
	"github.com/pingcap/tiflow/engine/test"
	"github.com/pingcap/tiflow/engine/test/mock"
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
