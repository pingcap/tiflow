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

	. "github.com/pingcap/check"

	"github.com/pingcap/dm/dm/config"
)

const (
	// do not forget to update this path if the file removed/renamed.
	subTaskSampleFile = "../../dm/worker/subtask.toml"
)

func (t *testForEtcd) TestSubTaskEtcd(c *C) {
	defer clearTestInfoOperation(c)

	cfg1 := config.SubTaskConfig{}
	c.Assert(cfg1.DecodeFile(subTaskSampleFile, true), IsNil)
	source := cfg1.SourceID
	taskName1 := cfg1.Name

	taskName2 := taskName1 + "2"
	cfg2 := cfg1
	cfg2.Name = taskName2
	err := cfg2.Adjust(true)
	c.Assert(err, IsNil)

	// no subtask config exist.
	tsm1, rev1, err := GetSubTaskCfg(etcdTestCli, source, taskName1, 0)
	c.Assert(err, IsNil)
	c.Assert(rev1, Greater, int64(0))
	c.Assert(tsm1, HasLen, 0)

	// put subtask configs.
	rev2, err := PutSubTaskCfgStage(etcdTestCli, []config.SubTaskConfig{cfg1, cfg2}, []Stage{})
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// get single config back.
	tsm2, rev3, err := GetSubTaskCfg(etcdTestCli, source, taskName1, 0)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	c.Assert(tsm2, HasLen, 1)
	c.Assert(tsm2, HasKey, taskName1)
	c.Assert(tsm2[taskName1], DeepEquals, cfg1)

	tsm3, rev4, err := GetSubTaskCfg(etcdTestCli, source, "", 0)
	c.Assert(err, IsNil)
	c.Assert(rev4, Equals, rev3)
	c.Assert(tsm3, HasLen, 2)
	c.Assert(tsm3, HasKey, taskName1)
	c.Assert(tsm3, HasKey, taskName2)
	c.Assert(tsm3[taskName1], DeepEquals, cfg1)
	c.Assert(tsm3[taskName2], DeepEquals, cfg2)

	// get all subtask configs.
	stmm, rev4, err := GetAllSubTaskCfg(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev4, Equals, rev3)
	c.Assert(stmm, HasLen, 1)
	c.Assert(stmm[source], HasLen, 2)
	c.Assert(stmm[source][taskName1], DeepEquals, cfg1)
	c.Assert(stmm[source][taskName2], DeepEquals, cfg2)

	// delete the config.
	deleteOps := deleteSubTaskCfgOp(cfg1)
	_, err = etcdTestCli.Txn(context.Background()).Then(deleteOps...).Commit()
	c.Assert(err, IsNil)
	deleteOps = deleteSubTaskCfgOp(cfg2)
	deleteResp, err := etcdTestCli.Txn(context.Background()).Then(deleteOps...).Commit()
	c.Assert(err, IsNil)

	// get again, not exists now.
	tsm4, rev5, err := GetSubTaskCfg(etcdTestCli, source, taskName1, 0)
	c.Assert(err, IsNil)
	c.Assert(rev5, Equals, deleteResp.Header.Revision)
	c.Assert(tsm4, HasLen, 0)

	// put subtask config.
	rev6, err := PutSubTaskCfgStage(etcdTestCli, []config.SubTaskConfig{cfg1}, []Stage{})
	c.Assert(err, IsNil)
	c.Assert(rev6, Greater, int64(0))

	// update subtask config.
	cfg3 := cfg1
	cfg3.SourceID = "testForRevision"
	rev7, err := PutSubTaskCfgStage(etcdTestCli, []config.SubTaskConfig{cfg3}, []Stage{})
	c.Assert(err, IsNil)
	c.Assert(rev7, Greater, rev6)

	// get subtask from rev6. shoule be equal to cfg1
	tsm5, rev8, err := GetSubTaskCfg(etcdTestCli, source, taskName1, rev6)
	c.Assert(err, IsNil)
	c.Assert(rev8, Equals, rev7)
	c.Assert(tsm5, HasLen, 1)
	c.Assert(tsm5, HasKey, taskName1)
	c.Assert(tsm5[taskName1], DeepEquals, cfg1)
}
