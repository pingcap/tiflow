// Copyright 2020 PingCAP, Inc.
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
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
)

func (s *testForEtcd) TestOpsEtcd() {
	defer clearTestInfoOperation(s.T())

	var (
		source         = "mysql-replica-1"
		worker         = "dm-worker-1"
		task1          = "task-1"
		task2          = "task-2"
		relayStage     = NewRelayStage(pb.Stage_Running, source)
		subtaskStage1  = NewSubTaskStage(pb.Stage_Running, source, task1)
		subtaskStage2  = NewSubTaskStage(pb.Stage_Running, source, task2)
		validatorStage = NewSubTaskStage(pb.Stage_Running, source, task2)
		bound          = NewSourceBound(source, worker)

		emptyStage  Stage
		subtaskCfg1 config.SubTaskConfig
	)

	sourceCfg, err := config.LoadFromFile(sourceSampleFilePath)
	s.Require().NoError(err)
	sourceCfg.SourceID = source
	s.Require().NoError(subtaskCfg1.Decode(config.SampleSubtaskConfig, true))
	subtaskCfg1.SourceID = source
	subtaskCfg1.Name = task1
	s.Require().NoError(subtaskCfg1.Adjust(true))
	subtaskCfg2 := subtaskCfg1
	subtaskCfg2.Name = task2
	s.Require().NoError(subtaskCfg2.Adjust(true))

	// put relay stage and source bound.
	rev1, err := PutRelayStageRelayConfigSourceBound(etcdTestCli, relayStage, bound)
	s.Require().NoError(err)
	s.Require().Greater(rev1, int64(0))
	// put source config.
	rev2, err := PutSourceCfg(etcdTestCli, sourceCfg)
	s.Require().NoError(err)
	s.Require().Greater(rev2, rev1)

	// get them back.
	st1, rev3, err := GetRelayStage(etcdTestCli, source)
	s.Require().NoError(err)
	s.Require().Equal(rev2, rev3)
	relayStage.Revision = rev1
	s.Require().Equal(relayStage, st1)
	sbm1, rev3, err := GetSourceBound(etcdTestCli, worker)
	s.Require().NoError(err)
	s.Require().Equal(rev2, rev3)
	s.Require().Len(sbm1, 1)
	bound.Revision = rev1
	s.Require().Equal(bound, sbm1[worker])
	scm1, rev3, err := GetSourceCfg(etcdTestCli, source, 0)
	s.Require().NoError(err)
	s.Require().Equal(rev2, rev3)
	soCfg1 := scm1[source]
	s.Require().Equal(sourceCfg, soCfg1)

	// delete source config, relay stage and source bound.
	rev4, err := DeleteSourceCfgRelayStageSourceBound(etcdTestCli, source, worker)
	s.Require().NoError(err)
	s.Require().Greater(rev4, rev3)

	// try to get them back again.
	st2, rev5, err := GetRelayStage(etcdTestCli, source)
	s.Require().NoError(err)
	s.Require().Equal(rev4, rev5)
	s.Require().Equal(emptyStage, st2)
	sbm2, rev5, err := GetSourceBound(etcdTestCli, worker)
	s.Require().NoError(err)
	s.Require().Equal(rev4, rev5)
	s.Require().Len(sbm2, 0)
	scm2, rev5, err := GetSourceCfg(etcdTestCli, source, 0)
	s.Require().NoError(err)
	s.Require().Equal(rev4, rev5)
	s.Require().Len(scm2, 0)

	// put subtask config and subtask stage.
	rev6, err := PutSubTaskCfgStage(etcdTestCli, []config.SubTaskConfig{subtaskCfg1, subtaskCfg2}, []Stage{subtaskStage1, subtaskStage2}, []Stage{validatorStage})
	s.Require().NoError(err)
	s.Require().Greater(rev6, rev5)

	// get them back.
	stcm, rev7, err := GetSubTaskCfg(etcdTestCli, source, "", 0)
	s.Require().NoError(err)
	s.Require().Equal(rev6, rev7)
	s.Require().Len(stcm, 2)
	s.Require().Equal(subtaskCfg1, stcm[task1])
	s.Require().Equal(subtaskCfg2, stcm[task2])
	stsm, rev7, err := GetSubTaskStage(etcdTestCli, source, "")
	s.Require().NoError(err)
	s.Require().Equal(rev6, rev7)
	s.Require().Len(stsm, 2)
	subtaskStage1.Revision = rev6
	subtaskStage2.Revision = rev6
	s.Require().Equal(subtaskStage1, stsm[task1])
	s.Require().Equal(subtaskStage2, stsm[task2])
	validatorStages, rev7, err := GetValidatorStage(etcdTestCli, source, "", rev6)
	s.Require().NoError(err)
	s.Require().Equal(rev6, rev7)
	s.Require().Len(validatorStages, 1)
	validatorStage.Revision = rev6
	s.Require().Equal(validatorStage, validatorStages[task2])
	// get with task name
	validatorStages, rev7, err = GetValidatorStage(etcdTestCli, source, task2, rev6)
	s.Require().NoError(err)
	s.Require().Equal(rev6, rev7)
	s.Require().Len(validatorStages, 1)
	validatorStage.Revision = rev6
	s.Require().Equal(validatorStage, validatorStages[task2])

	// delete them.
	rev8, err := DeleteSubTaskCfgStage(etcdTestCli, []config.SubTaskConfig{subtaskCfg1, subtaskCfg2}, []Stage{subtaskStage1, subtaskStage2}, []Stage{validatorStage})
	s.Require().NoError(err)
	s.Require().Greater(rev8, rev7)

	// try to get them back again.
	stcm, rev9, err := GetSubTaskCfg(etcdTestCli, source, "", 0)
	s.Require().NoError(err)
	s.Require().Equal(rev8, rev9)
	s.Require().Len(stcm, 0)
	stsm, rev9, err = GetSubTaskStage(etcdTestCli, source, "")
	s.Require().NoError(err)
	s.Require().Equal(rev8, rev9)
	s.Require().Len(stsm, 0)
	validatorStages, rev9, err = GetValidatorStage(etcdTestCli, source, "", 0)
	s.Require().NoError(err)
	s.Require().Equal(rev8, rev9)
	s.Require().Len(validatorStages, 0)
}
