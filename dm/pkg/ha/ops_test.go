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
	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
)

func (t *testForEtcd) TestOpsEtcd(c *check.C) {
	defer clearTestInfoOperation(c)

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
	c.Assert(err, check.IsNil)
	sourceCfg.SourceID = source
	c.Assert(subtaskCfg1.Decode(config.SampleSubtaskConfig, true), check.IsNil)
	subtaskCfg1.SourceID = source
	subtaskCfg1.Name = task1
	c.Assert(subtaskCfg1.Adjust(true), check.IsNil)
	subtaskCfg2 := subtaskCfg1
	subtaskCfg2.Name = task2
	c.Assert(subtaskCfg2.Adjust(true), check.IsNil)

	// put relay stage and source bound.
	rev1, err := PutRelayStageRelayConfigSourceBound(etcdTestCli, relayStage, bound)
	c.Assert(err, check.IsNil)
	c.Assert(rev1, check.Greater, int64(0))
	// put source config.
	rev2, err := PutSourceCfg(etcdTestCli, sourceCfg)
	c.Assert(err, check.IsNil)
	c.Assert(rev2, check.Greater, rev1)

	// get them back.
	st1, rev3, err := GetRelayStage(etcdTestCli, source)
	c.Assert(err, check.IsNil)
	c.Assert(rev3, check.Equals, rev2)
	relayStage.Revision = rev1
	c.Assert(st1, check.DeepEquals, relayStage)
	sbm1, rev3, err := GetSourceBound(etcdTestCli, worker)
	c.Assert(err, check.IsNil)
	c.Assert(rev3, check.Equals, rev2)
	c.Assert(sbm1, check.HasLen, 1)
	bound.Revision = rev1
	c.Assert(sbm1[worker], check.DeepEquals, bound)
	scm1, rev3, err := GetSourceCfg(etcdTestCli, source, 0)
	c.Assert(err, check.IsNil)
	c.Assert(rev3, check.Equals, rev2)
	soCfg1 := scm1[source]
	c.Assert(soCfg1, check.DeepEquals, sourceCfg)

	// delete source config, relay stage and source bound.
	rev4, err := DeleteSourceCfgRelayStageSourceBound(etcdTestCli, source, worker)
	c.Assert(err, check.IsNil)
	c.Assert(rev4, check.Greater, rev3)

	// try to get them back again.
	st2, rev5, err := GetRelayStage(etcdTestCli, source)
	c.Assert(err, check.IsNil)
	c.Assert(rev5, check.Equals, rev4)
	c.Assert(st2, check.Equals, emptyStage)
	sbm2, rev5, err := GetSourceBound(etcdTestCli, worker)
	c.Assert(err, check.IsNil)
	c.Assert(rev5, check.Equals, rev4)
	c.Assert(sbm2, check.HasLen, 0)
	scm2, rev5, err := GetSourceCfg(etcdTestCli, source, 0)
	c.Assert(err, check.IsNil)
	c.Assert(rev5, check.Equals, rev4)
	c.Assert(scm2, check.HasLen, 0)

	// put subtask config and subtask stage.
	rev6, err := PutSubTaskCfgStage(etcdTestCli, []config.SubTaskConfig{subtaskCfg1, subtaskCfg2}, []Stage{subtaskStage1, subtaskStage2}, []Stage{validatorStage})
	c.Assert(err, check.IsNil)
	c.Assert(rev6, check.Greater, rev5)

	// get them back.
	stcm, rev7, err := GetSubTaskCfg(etcdTestCli, source, "", 0)
	c.Assert(err, check.IsNil)
	c.Assert(rev7, check.Equals, rev6)
	c.Assert(stcm, check.HasLen, 2)
	c.Assert(stcm[task1], check.DeepEquals, subtaskCfg1)
	c.Assert(stcm[task2], check.DeepEquals, subtaskCfg2)
	stsm, rev7, err := GetSubTaskStage(etcdTestCli, source, "")
	c.Assert(err, check.IsNil)
	c.Assert(rev7, check.Equals, rev6)
	c.Assert(stsm, check.HasLen, 2)
	subtaskStage1.Revision = rev6
	subtaskStage2.Revision = rev6
	c.Assert(stsm[task1], check.DeepEquals, subtaskStage1)
	c.Assert(stsm[task2], check.DeepEquals, subtaskStage2)
	validatorStages, rev7, err := GetValidatorStage(etcdTestCli, source, "", rev6)
	c.Assert(err, check.IsNil)
	c.Assert(rev7, check.Equals, rev6)
	c.Assert(validatorStages, check.HasLen, 1)
	validatorStage.Revision = rev6
	c.Assert(validatorStages[task2], check.DeepEquals, validatorStage)
	// get with task name
	validatorStages, rev7, err = GetValidatorStage(etcdTestCli, source, task2, rev6)
	c.Assert(err, check.IsNil)
	c.Assert(rev7, check.Equals, rev6)
	c.Assert(validatorStages, check.HasLen, 1)
	validatorStage.Revision = rev6
	c.Assert(validatorStages[task2], check.DeepEquals, validatorStage)

	// delete them.
	rev8, err := DeleteSubTaskCfgStage(etcdTestCli, []config.SubTaskConfig{subtaskCfg1, subtaskCfg2}, []Stage{subtaskStage1, subtaskStage2}, []Stage{validatorStage})
	c.Assert(err, check.IsNil)
	c.Assert(rev8, check.Greater, rev7)

	// try to get them back again.
	stcm, rev9, err := GetSubTaskCfg(etcdTestCli, source, "", 0)
	c.Assert(err, check.IsNil)
	c.Assert(rev9, check.Equals, rev8)
	c.Assert(stcm, check.HasLen, 0)
	stsm, rev9, err = GetSubTaskStage(etcdTestCli, source, "")
	c.Assert(err, check.IsNil)
	c.Assert(rev9, check.Equals, rev8)
	c.Assert(stsm, check.HasLen, 0)
	validatorStages, rev9, err = GetValidatorStage(etcdTestCli, source, "", 0)
	c.Assert(err, check.IsNil)
	c.Assert(rev9, check.Equals, rev8)
	c.Assert(validatorStages, check.HasLen, 0)
}
