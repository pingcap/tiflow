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

package master

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/stretchr/testify/require"
)

func TestCreateTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := setupTestServer(ctx, t)

	taskCfg := config.NewTaskConfig()
	require.Nil(t, taskCfg.Decode(taskConfig))
	require.Nil(t, s.createTask(ctx, taskCfg))

	subTaskM := s.scheduler.GetSubTaskCfgsByTask(taskCfg.Name)
	require.Equal(t, len(subTaskM), 1)
	require.Equal(t, subTaskM[source1Name].Name, taskCfg.Name)
	require.Equal(t, s.scheduler.GetExpectSubTaskStage(taskCfg.Name, taskCfg.MySQLInstances[0].SourceID), pb.Stage_Stopped)
}
