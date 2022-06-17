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

package config

import (
	"context"
	"fmt"
	"testing"

	"github.com/BurntSushi/toml"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	dmmaster "github.com/pingcap/tiflow/dm/dm/master"
	"github.com/stretchr/testify/require"
)

const (
	jobTemplatePath    = "./job_template.yaml"
	subtaskTemplateDir = "."
)

func checkAndNoAdjustSourceConfigMock(ctx context.Context, cfg *dmconfig.SourceConfig) error {
	if _, err := cfg.Yaml(); err != nil {
		return err
	}
	return cfg.Verify()
}

func TestJobCfg(t *testing.T) {
	funcBackup := dmmaster.CheckAndAdjustSourceConfigFunc
	dmmaster.CheckAndAdjustSourceConfigFunc = checkAndNoAdjustSourceConfigMock
	defer func() {
		dmmaster.CheckAndAdjustSourceConfigFunc = funcBackup
	}()

	jobCfg := &JobCfg{}
	require.NoError(t, jobCfg.DecodeFile(jobTemplatePath))
	require.Equal(t, "test", jobCfg.Name)
	content, err := jobCfg.Yaml()
	require.NoError(t, err)

	clone, err := jobCfg.Clone()
	require.NoError(t, err)
	content2, err := clone.Yaml()
	require.NoError(t, err)
	require.Equal(t, content2, content)

	dmTaskCfg, err := clone.toDMTaskConfig()
	require.NoError(t, err)
	require.NoError(t, clone.fromDMTaskConfig(dmTaskCfg))
	content3, err := clone.Yaml()
	require.NoError(t, err)
	require.Equal(t, content3, content)

	require.Error(t, jobCfg.DecodeFile("./job_not_exist.yaml"))
}

func TestTaskCfg(t *testing.T) {
	funcBackup := dmmaster.CheckAndAdjustSourceConfigFunc
	dmmaster.CheckAndAdjustSourceConfigFunc = checkAndNoAdjustSourceConfigMock
	defer func() {
		dmmaster.CheckAndAdjustSourceConfigFunc = funcBackup
	}()

	jobCfg := &JobCfg{}
	require.NoError(t, jobCfg.DecodeFile(jobTemplatePath))

	taskCfgs := jobCfg.ToTaskCfgs()

	taskCfgList := make([]*TaskCfg, 0, len(taskCfgs))
	for _, taskCfg := range taskCfgs {
		taskCfgList = append(taskCfgList, taskCfg)
	}
	jobCfg2 := FromTaskCfgs(taskCfgList)
	taskCfgs = jobCfg2.ToTaskCfgs()

	for _, taskCfg := range taskCfgs {
		subTaskCfg := taskCfg.ToDMSubTaskCfg()
		expectCfg := &dmconfig.SubTaskConfig{}
		_, err := toml.DecodeFile(fmt.Sprintf("%s/dm_subtask_%d.toml", subtaskTemplateDir, taskCfg.Upstreams[0].DBCfg.Port), expectCfg)
		require.NoError(t, err)
		require.EqualValues(t, subTaskCfg, expectCfg)
	}
}
