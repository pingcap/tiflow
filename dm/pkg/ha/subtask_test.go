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
	"context"

	"github.com/pingcap/tiflow/dm/config"
)

func (s *testForEtcd) TestSubTaskEtcd() {
	defer clearTestInfoOperation(s.T())

	cfg1 := config.SubTaskConfig{}
	s.Require().NoError(cfg1.Decode(config.SampleSubtaskConfig, true))
	source := cfg1.SourceID
	taskName1 := cfg1.Name

	taskName2 := taskName1 + "2"
	cfg2 := cfg1
	cfg2.Name = taskName2
	err := cfg2.Adjust(true)
	s.Require().NoError(err)

	// no subtask config exist.
	tsm1, rev1, err := GetSubTaskCfg(etcdTestCli, source, taskName1, 0)
	s.Require().NoError(err)
	s.Require().Greater(rev1, int64(0))
	s.Require().Len(tsm1, 0)

	// put subtask configs.
	rev2, err := PutSubTaskCfgStage(etcdTestCli, []config.SubTaskConfig{cfg1, cfg2}, []Stage{}, nil)
	s.Require().NoError(err)
	s.Require().Greater(rev2, rev1)

	// get single config back.
	tsm2, rev3, err := GetSubTaskCfg(etcdTestCli, source, taskName1, 0)
	s.Require().NoError(err)
	s.Require().Equal(rev2, rev3)
	s.Require().Len(tsm2, 1)
	s.Require().Contains(tsm2, taskName1)
	s.Require().Equal(cfg1, tsm2[taskName1])

	tsm3, rev4, err := GetSubTaskCfg(etcdTestCli, source, "", 0)
	s.Require().NoError(err)
	s.Require().Equal(rev3, rev4)
	s.Require().Len(tsm3, 2)
	s.Require().Contains(tsm3, taskName1)
	s.Require().Contains(tsm3, taskName2)
	s.Require().Equal(cfg1, tsm3[taskName1])
	s.Require().Equal(cfg2, tsm3[taskName2])

	// get all subtask configs.
	stmm, rev4, err := GetAllSubTaskCfg(etcdTestCli)
	s.Require().NoError(err)
	s.Require().Equal(rev3, rev4)
	s.Require().Len(stmm, 1)
	s.Require().Len(stmm[source], 2)
	s.Require().Equal(cfg1, stmm[source][taskName1])
	s.Require().Equal(cfg2, stmm[source][taskName2])

	// delete the config.
	deleteOps := deleteSubTaskCfgOp(cfg1)
	_, err = etcdTestCli.Txn(context.Background()).Then(deleteOps...).Commit()
	s.Require().NoError(err)
	deleteOps = deleteSubTaskCfgOp(cfg2)
	deleteResp, err := etcdTestCli.Txn(context.Background()).Then(deleteOps...).Commit()
	s.Require().NoError(err)

	// get again, not exists now.
	tsm4, rev5, err := GetSubTaskCfg(etcdTestCli, source, taskName1, 0)
	s.Require().NoError(err)
	s.Require().Equal(deleteResp.Header.Revision, rev5)
	s.Require().Len(tsm4, 0)

	// put subtask config.
	rev6, err := PutSubTaskCfgStage(etcdTestCli, []config.SubTaskConfig{cfg1}, []Stage{}, nil)
	s.Require().NoError(err)
	s.Require().Greater(rev6, int64(0))

	// update subtask config.
	cfg3 := cfg1
	cfg3.SourceID = "testForRevision"
	rev7, err := PutSubTaskCfgStage(etcdTestCli, []config.SubTaskConfig{cfg3}, []Stage{}, nil)
	s.Require().NoError(err)
	s.Require().Greater(rev7, rev6)

	// get subtask from rev6. shoule be equal to cfg1
	tsm5, rev8, err := GetSubTaskCfg(etcdTestCli, source, taskName1, rev6)
	s.Require().NoError(err)
	s.Require().Equal(rev7, rev8)
	s.Require().Len(tsm5, 1)
	s.Require().Contains(tsm5, taskName1)
	s.Require().Equal(cfg1, tsm5[taskName1])
}
