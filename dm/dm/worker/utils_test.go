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

package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/ha"
)

func TestGetExpectValidatorStage(t *testing.T) {
	integration.BeforeTestExternal(t)
	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	etcdTestCli := mockCluster.RandClient()
	defer func() {
		require.Nil(t, ha.ClearTestInfoOperation(etcdTestCli))
	}()
	cfg := config.SubTaskConfig{}
	require.Nil(t, cfg.Decode(config.SampleSubtaskConfig, true))
	source := cfg.SourceID
	task := cfg.Name
	stage := ha.NewSubTaskStage(pb.Stage_Running, source, task)

	validatorStage, err := getExpectValidatorStage(cfg.ValidatorCfg, etcdTestCli, source, task, 0)
	require.Nil(t, err)
	require.Equal(t, pb.Stage_InvalidStage, validatorStage)

	cfg.ValidatorCfg.Mode = config.ValidationFast
	validatorStage, err = getExpectValidatorStage(cfg.ValidatorCfg, etcdTestCli, source, task, 0)
	require.Nil(t, err)
	require.Equal(t, pb.Stage_InvalidStage, validatorStage)

	// put subtask config and stage at the same time
	rev, err := ha.PutSubTaskCfgStage(etcdTestCli, []config.SubTaskConfig{cfg}, []ha.Stage{stage}, []ha.Stage{stage})
	require.Nil(t, err)

	validatorStage, err = getExpectValidatorStage(cfg.ValidatorCfg, etcdTestCli, source, task, 0)
	require.Nil(t, err)
	require.Equal(t, pb.Stage_Running, validatorStage)
	validatorStage, err = getExpectValidatorStage(cfg.ValidatorCfg, etcdTestCli, source, task+"not exist", 0)
	require.Nil(t, err)
	require.Equal(t, pb.Stage_InvalidStage, validatorStage)

	_, err = getExpectValidatorStage(cfg.ValidatorCfg, etcdTestCli, source, task+"not exist", rev+1)
	require.NotNil(t, err)
}
