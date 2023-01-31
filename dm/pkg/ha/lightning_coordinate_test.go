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

package ha

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestLightningStatus(t *testing.T) {
	integration.BeforeTestExternal(t)
	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	etcdCli := mockCluster.RandClient()
	task := "test-lightning-status"
	source1 := "s1"
	source2 := "s2"
	status, err := GetAllLightningStatus(etcdCli, task)
	require.NoError(t, err)
	require.Len(t, status, 0)

	_, err = PutLightningNotReadyForAllSources(etcdCli, task, []string{source1, source2})
	require.NoError(t, err)

	status, err = GetAllLightningStatus(etcdCli, task)
	require.NoError(t, err)
	require.Equal(t, []string{LightningNotReady, LightningNotReady}, status)

	_, err = PutLightningStatus(etcdCli, task, source1, LightningReady)
	require.NoError(t, err)
	status, err = GetAllLightningStatus(etcdCli, task)
	require.NoError(t, err)
	require.Contains(t, status, LightningReady)
	require.Contains(t, status, LightningNotReady)

	_, err = DeleteLightningStatusForTask(etcdCli, task)
	require.NoError(t, err)
	status, err = GetAllLightningStatus(etcdCli, task)
	require.NoError(t, err)
	require.Len(t, status, 0)
}
