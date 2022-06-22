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

package test

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
)

func TestPrepareEtcd(t *testing.T) {
	t.Parallel()

	err := log.InitLogger(&log.Config{})
	require.Nil(t, err)

	ctx := context.Background()
	addr, etcd, client, cleanFn := PrepareEtcd(t, "test-etcd")
	resp, err := client.Status(ctx, addr)
	require.Nil(t, err)
	require.Equal(t, uint64(etcd.Server.ID()), resp.Header.MemberId)
	cleanFn()
}

func TestPrepareEtcdCluster(t *testing.T) {
	t.Parallel()

	err := log.InitLogger(&log.Config{})
	require.Nil(t, err)

	ctx := context.Background()
	names := []string{"etcd-1", "etcd-2", "etcd-3"}
	addrs, etcds, client, cleanFn := PrepareEtcdCluster(t, names)
	serverIDs := make([]uint64, 0, len(etcds))
	for _, etcd := range etcds {
		serverIDs = append(serverIDs, uint64(etcd.Server.ID()))
	}
	for _, addr := range addrs {
		resp, err := client.Status(ctx, addr)
		require.Nil(t, err)
		require.Contains(t, serverIDs, resp.Header.MemberId)
	}
	cleanFn()
}
