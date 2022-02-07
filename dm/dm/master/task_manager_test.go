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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/suite"
)

type TaskManagerSuite struct {
	suite.Suite

	ctx     context.Context
	cancel  context.CancelFunc
	master1 *Server
}

// SetupSuite setup a test cluster with one master and two worker.
func (suite *TaskManagerSuite) SetupSuite() {
	suite.Nil(log.InitLogger(&log.Config{}))
	checkAndAdjustSourceConfigFunc = checkAndNoAdjustSourceConfigMock

	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.master1 = setupTestServer(suite.ctx, suite.T())

	sources, workers := defaultWorkerSource()
	// add worker
	for i, workerAddr := range workers {
		workerName := fmt.Sprintf("worker-%d", i)
		suite.Nil(suite.master1.scheduler.AddWorker(workerName, workerAddr))

		go func(ctx context.Context, workerName string) {
			suite.Nil(ha.KeepAlive(ctx, suite.master1.etcdClient, workerName, keepAliveTTL), check.IsNil)
		}(suite.ctx, workerName)
	}
	// add sources
	for _, sourceName := range sources {
		sourceCfg := config.NewSourceConfig()
		sourceCfg.SourceID = sourceName
		suite.Nil(suite.master1.scheduler.AddSourceCfg(sourceCfg))
		// wait worker ready
		suite.True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
			w := suite.master1.scheduler.GetWorkerBySource(sourceName)
			return w != nil
		}))
	}
}

func (suite *TaskManagerSuite) TearDownSuite() {
	suite.cancel()
	suite.master1.Close()

	checkAndAdjustSourceConfigFunc = checkAndAdjustSourceConfig
}

func (suite *TaskManagerSuite) TestCreateTask() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	taskCfg := config.NewTaskConfig()
	suite.Nil(taskCfg.Decode(taskConfig))

	s := suite.master1
	suite.Nil(s.createTask(ctx, taskCfg))
	subTaskM := s.scheduler.GetSubTaskCfgsByTask(taskCfg.Name)
	suite.Equal(len(subTaskM), len(taskCfg.MySQLInstances))
	suite.Equal(subTaskM[source1Name].Name, taskCfg.Name)
	for _, source := range taskCfg.MySQLInstances {
		suite.Equal(s.scheduler.GetExpectSubTaskStage(taskCfg.Name, source.SourceID).Expect, pb.Stage_Stopped)
	}
}

func TestTaskManagerSuite(t *testing.T) {
	suite.Run(t, new(TaskManagerSuite))
}
