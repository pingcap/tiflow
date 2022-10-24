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

package metadata

import (
	"context"
	"testing"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	"github.com/pingcap/tiflow/engine/pkg/meta/mock"
	"github.com/stretchr/testify/require"
)

func TestInfoStore(t *testing.T) {
	ver1 := *semver.New("1.0.0")
	clusterInfo := NewClusterInfo(ver1)
	require.True(t, clusterInfo.Version.Equal(ver1))

	clusterInfoStore := NewClusterInfoStore(mock.NewMetaMock())
	key := clusterInfoStore.key()
	keys, err := adapter.DMInfoKeyAdapter.Decode(key)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, keys[0], "")

	state := clusterInfoStore.createState()
	require.IsType(t, &ClusterInfo{}, state)

	state, err = clusterInfoStore.Get(context.Background())
	require.EqualError(t, err, "state not found")
	require.Nil(t, state)
	require.EqualError(t, clusterInfoStore.UpdateVersion(context.Background(), ver1), "state not found")
	require.NoError(t, clusterInfoStore.Put(context.Background(), clusterInfo))
	state, err = clusterInfoStore.Get(context.Background())
	require.NoError(t, err)
	clusterInfo2 := state.(*ClusterInfo)
	require.True(t, clusterInfo2.Version.Equal(clusterInfo.Version))

	ver2 := *semver.New("2.0.0")
	clusterInfoStore.UpdateVersion(context.Background(), ver2)
	state, err = clusterInfoStore.Get(context.Background())
	require.NoError(t, err)
	clusterInfo3 := state.(*ClusterInfo)
	require.True(t, clusterInfo3.Version.Equal(ver2))
}
