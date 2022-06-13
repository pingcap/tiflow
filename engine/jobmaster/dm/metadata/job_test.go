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

	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	dmmaster "github.com/pingcap/tiflow/dm/dm/master"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	"github.com/pingcap/tiflow/engine/pkg/meta/kvclient/mock"
)

const (
	jobTemplatePath = "../config/job_template.yaml"
)

func checkAndNoAdjustSourceConfigMock(ctx context.Context, cfg *dmconfig.SourceConfig) error {
	if _, err := cfg.Yaml(); err != nil {
		return err
	}
	return cfg.Verify()
}

func TestJobStore(t *testing.T) {
	funcBackup := dmmaster.CheckAndAdjustSourceConfigFunc
	dmmaster.CheckAndAdjustSourceConfigFunc = checkAndNoAdjustSourceConfigMock
	defer func() {
		dmmaster.CheckAndAdjustSourceConfigFunc = funcBackup
	}()

	var (
		source1 = "mysql-replica-01"
		source2 = "mysql-replica-02"
	)
	t.Parallel()

	jobStore := NewJobStore("job_test", mock.NewMetaMock())
	key := jobStore.Key()
	keys, err := adapter.DMJobKeyAdapter.Decode(key)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, keys[0], "job_test")

	require.Error(t, jobStore.UpdateStages(context.Background(), []string{}, StageRunning))

	state := jobStore.CreateState()
	require.IsType(t, &Job{}, state)

	jobCfg := &config.JobCfg{}
	require.NoError(t, jobCfg.DecodeFile(jobTemplatePath))

	job := NewJob(jobCfg)
	require.NoError(t, jobStore.Put(context.Background(), job))
	state, err = jobStore.Get(context.Background())
	require.NoError(t, err)
	require.NotNil(t, state)
	require.IsType(t, &Job{}, state)

	job = state.(*Job)
	require.Len(t, job.Tasks, len(jobCfg.Upstreams))
	require.Contains(t, job.Tasks, source1)
	require.Contains(t, job.Tasks, source1)
	require.Equal(t, job.Tasks[source1].Stage, StageRunning)
	require.Equal(t, job.Tasks[source2].Stage, StageRunning)

	require.Error(t, jobStore.UpdateStages(context.Background(), []string{"task-not-exist"}, StageRunning))
	require.Error(t, jobStore.UpdateStages(context.Background(), []string{source1, "task-not-exist"}, StageRunning))
	state, _ = jobStore.Get(context.Background())
	job = state.(*Job)
	require.Equal(t, job.Tasks[source1].Stage, StageRunning)
	require.Equal(t, job.Tasks[source2].Stage, StageRunning)

	require.NoError(t, jobStore.UpdateStages(context.Background(), nil, StagePaused))
	require.Equal(t, job.Tasks[source1].Stage, StageRunning)
	require.Equal(t, job.Tasks[source2].Stage, StageRunning)
	state, _ = jobStore.Get(context.Background())
	job = state.(*Job)
	require.Equal(t, job.Tasks[source1].Stage, StagePaused)
	require.Equal(t, job.Tasks[source2].Stage, StagePaused)

	require.NoError(t, jobStore.UpdateStages(context.Background(), []string{source2}, StageRunning))
	state, _ = jobStore.Get(context.Background())
	job = state.(*Job)
	require.Equal(t, job.Tasks[source1].Stage, StagePaused)
	require.Equal(t, job.Tasks[source2].Stage, StageRunning)
}
