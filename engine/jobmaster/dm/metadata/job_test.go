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
	"encoding/json"
	"testing"

	"github.com/pingcap/log"
	dmconfig "github.com/pingcap/tiflow/dm/config"
	dmmaster "github.com/pingcap/tiflow/dm/master"
	dmpb "github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	"github.com/pingcap/tiflow/engine/pkg/meta/mock"
	"github.com/stretchr/testify/require"
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

	jobStore := NewJobStore(mock.NewMetaMock(), log.L())
	key := jobStore.key()
	keys, err := adapter.DMJobKeyAdapter.Decode(key)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, keys[0], "")

	require.Error(t, jobStore.UpdateStages(context.Background(), []string{}, StageRunning))
	require.Error(t, jobStore.UpdateConfig(context.Background(), nil))
	require.Error(t, jobStore.MarkDeleting(context.Background()))

	state := jobStore.createState()
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
	require.Equal(t, job.Tasks[source1].Cfg.ModRevision, uint64(0))
	require.Equal(t, job.Tasks[source2].Cfg.ModRevision, uint64(0))

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

	require.NoError(t, jobStore.UpdateConfig(context.Background(), jobCfg))
	state, err = jobStore.Get(context.Background())
	require.NoError(t, err)
	job = state.(*Job)
	require.Equal(t, job.Tasks[source1].Stage, StagePaused)
	require.Equal(t, job.Tasks[source2].Stage, StageRunning)
	require.Equal(t, job.Tasks[source1].Cfg.ModRevision, uint64(1))
	require.Equal(t, job.Tasks[source2].Cfg.ModRevision, uint64(1))
	require.False(t, job.Deleting)

	require.NoError(t, jobStore.MarkDeleting(context.Background()))
	state, err = jobStore.Get(context.Background())
	require.NoError(t, err)
	job = state.(*Job)
	require.True(t, job.Deleting)

	require.EqualError(t, jobStore.UpdateStages(context.Background(), []string{source2}, StagePaused), "failed to update stages because job is being deleted")
	require.EqualError(t, jobStore.UpdateConfig(context.Background(), jobCfg), "failed to update config because job is being deleted")

	require.Len(t, jobStore.UpgradeFuncs(), 0)
}

func TestTaskStage(t *testing.T) {
	t.Parallel()
	for i, s := range typesStringify {
		if len(s) == 0 {
			continue
		}
		ts, ok := toTaskStage[s]
		require.True(t, ok)
		bs, err := json.Marshal(ts)
		require.NoError(t, err)
		var ts2 TaskStage
		require.NoError(t, json.Unmarshal(bs, &ts2))
		require.Equal(t, ts, ts2)
		require.Equal(t, ts, TaskStage(i))
	}

	ts := TaskStage(-1)
	require.Equal(t, "Unknown TaskStage -1", ts.String())
	ts = TaskStage(1000)
	require.Equal(t, "Unknown TaskStage 1000", ts.String())
	bs, err := json.Marshal(ts)
	require.NoError(t, err)
	var ts2 TaskStage
	require.EqualError(t, json.Unmarshal(bs, &ts2), "Unknown TaskStage Unknown TaskStage 1000")
	require.Equal(t, TaskStage(0), ts2)
}

func TestTaskStageValue(t *testing.T) {
	require.Equal(t, int(dmpb.Stage_New), int(StageInit))
	require.Equal(t, int(dmpb.Stage_Running), int(StageRunning))
	require.Equal(t, int(dmpb.Stage_Paused), int(StagePaused))
	require.Equal(t, int(dmpb.Stage_Finished), int(StageFinished))
	require.Equal(t, int(dmpb.Stage_Pausing), int(StagePausing))
	require.Greater(t, int(StageError), int(dmpb.Stage_Stopping))
	require.Greater(t, int(StageUnscheduled), int(dmpb.Stage_Stopping))

	require.Equal(t, 15, int(StageError))
	require.Equal(t, 16, int(StageUnscheduled))

	require.Equal(t, 0, int(toTaskStage[""]))

	require.Equal(t, "Initing", StageInit.String())
	require.Equal(t, "Running", StageRunning.String())
	require.Equal(t, "Paused", StagePaused.String())
	require.Equal(t, "Finished", StageFinished.String())
	require.Equal(t, "Error", StageError.String())
	require.Equal(t, "Pausing", StagePausing.String())
	require.Equal(t, "Unscheduled", StageUnscheduled.String())
}
