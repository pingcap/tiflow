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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
)

func (t *testForEtcd) TestStageJSON(c *C) {
	// stage for relay.
	rs1 := NewRelayStage(pb.Stage_Running, "mysql-replica-1")
	j, err := rs1.toJSON()
	c.Assert(err, IsNil)
	c.Assert(j, Equals, `{"expect":2,"source":"mysql-replica-1"}`)
	c.Assert(j, Equals, rs1.String())

	rs2, err := stageFromJSON(j)
	c.Assert(err, IsNil)
	c.Assert(rs2, DeepEquals, rs1)

	// stage for subtask.
	sts1 := NewSubTaskStage(pb.Stage_Paused, "mysql-replica-1", "task1")
	j, err = sts1.toJSON()
	c.Assert(err, IsNil)
	c.Assert(j, Equals, `{"expect":3,"source":"mysql-replica-1","task":"task1"}`)
	c.Assert(j, Equals, sts1.String())

	sts2, err := stageFromJSON(j)
	c.Assert(err, IsNil)
	c.Assert(sts2, DeepEquals, sts1)
}

func (t *testForEtcd) TestRelayStageEtcd(c *C) {
	defer clearTestInfoOperation(c)

	var (
		watchTimeout = 2 * time.Second
		source1      = "mysql-replica-1"
		source2      = "mysql-replica-2"
		emptyStage   = Stage{}
		stage1       = NewRelayStage(pb.Stage_Running, source1)
		stage2       = NewRelayStage(pb.Stage_Paused, source2)
	)
	c.Assert(stage1.IsDeleted, IsFalse)

	// no relay stage exist.
	st1, rev1, err := GetRelayStage(etcdTestCli, source1)
	c.Assert(err, IsNil)
	c.Assert(rev1, Greater, int64(0))
	c.Assert(st1, DeepEquals, emptyStage)

	// put two stage.
	rev2, err := PutRelayStage(etcdTestCli, stage1, stage2)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// watch the PUT operation for stage1.
	stageCh := make(chan Stage, 10)
	errCh := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), watchTimeout)
	WatchRelayStage(ctx, etcdTestCli, source1, rev2, stageCh, errCh)
	cancel()
	close(stageCh)
	close(errCh)
	c.Assert(len(stageCh), Equals, 1)
	stage1.Revision = rev2
	c.Assert(<-stageCh, DeepEquals, stage1)
	c.Assert(len(errCh), Equals, 0)

	// get stage1 back.
	st2, rev3, err := GetRelayStage(etcdTestCli, source1)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	c.Assert(st2, DeepEquals, stage1)

	// get two stages.
	stm, rev3, err := GetAllRelayStage(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	c.Assert(stm, HasLen, 2)
	stage2.Revision = rev2
	c.Assert(stm[source1], DeepEquals, stage1)
	c.Assert(stm[source2], DeepEquals, stage2)

	// delete stage1.
	deleteOp := deleteRelayStageOp(source1)
	resp, err := etcdTestCli.Txn(context.Background()).Then(deleteOp).Commit()
	c.Assert(err, IsNil)
	rev4 := resp.Header.Revision
	c.Assert(rev4, Greater, rev3)

	// watch the DELETE operation for stage1.
	stageCh = make(chan Stage, 10)
	errCh = make(chan error, 10)
	ctx, cancel = context.WithTimeout(context.Background(), watchTimeout)
	WatchRelayStage(ctx, etcdTestCli, source1, rev4, stageCh, errCh)
	cancel()
	close(stageCh)
	close(errCh)
	c.Assert(len(stageCh), Equals, 1)
	st3 := <-stageCh
	c.Assert(st3.IsDeleted, IsTrue)
	c.Assert(len(errCh), Equals, 0)

	// get again, not exists now.
	st4, rev5, err := GetRelayStage(etcdTestCli, source1)
	c.Assert(err, IsNil)
	c.Assert(rev5, Equals, rev4)
	c.Assert(st4, DeepEquals, emptyStage)
}

func (t *testForEtcd) TestSubTaskStageEtcd(c *C) {
	defer clearTestInfoOperation(c)

	var (
		watchTimeout = 2 * time.Second
		source       = "mysql-replica-1"
		task1        = "task-1"
		task2        = "task-2"
		stage1       = NewSubTaskStage(pb.Stage_Running, source, task1)
		stage2       = NewSubTaskStage(pb.Stage_Paused, source, task2)
	)

	// no stage exists.
	st1, rev1, err := GetSubTaskStage(etcdTestCli, source, task1)
	c.Assert(err, IsNil)
	c.Assert(rev1, Greater, int64(0))
	c.Assert(st1, HasLen, 0)

	// put two stages.
	rev2, err := PutSubTaskStage(etcdTestCli, stage1, stage2)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// watch the PUT operation for stages.
	stageCh := make(chan Stage, 10)
	errCh := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), watchTimeout)
	WatchSubTaskStage(ctx, etcdTestCli, source, rev2, stageCh, errCh)
	cancel()
	close(stageCh)
	close(errCh)
	c.Assert(len(stageCh), Equals, 2)
	stage1.Revision = rev2
	stage2.Revision = rev2
	c.Assert(<-stageCh, DeepEquals, stage1)
	c.Assert(<-stageCh, DeepEquals, stage2)
	c.Assert(len(errCh), Equals, 0)

	// get stages back without specified task.
	stm, rev3, err := GetSubTaskStage(etcdTestCli, source, "")
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	c.Assert(stm, HasLen, 2)
	c.Assert(stm[task1], DeepEquals, stage1)
	c.Assert(stm[task2], DeepEquals, stage2)

	// get the stage back with specified task.
	stm, rev3, err = GetSubTaskStage(etcdTestCli, source, task1)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	c.Assert(stm, HasLen, 1)
	c.Assert(stm[task1], DeepEquals, stage1)

	// get all stages.
	stmm, rev3, err := GetAllSubTaskStage(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	c.Assert(stmm, HasLen, 1)
	c.Assert(stmm[source], HasLen, 2)
	c.Assert(stmm[source][task1], DeepEquals, stage1)
	c.Assert(stmm[source][task2], DeepEquals, stage2)

	// delete two stages.
	rev4, err := DeleteSubTaskStage(etcdTestCli, stage1, stage2)
	c.Assert(err, IsNil)
	c.Assert(rev4, Greater, rev3)

	// watch the DELETE operation for stages.
	stageCh = make(chan Stage, 10)
	errCh = make(chan error, 10)
	ctx, cancel = context.WithTimeout(context.Background(), watchTimeout)
	WatchSubTaskStage(ctx, etcdTestCli, source, rev4, stageCh, errCh)
	cancel()
	close(stageCh)
	close(errCh)
	c.Assert(len(stageCh), Equals, 2)
	for st2 := range stageCh {
		c.Assert(st2.IsDeleted, IsTrue)
	}
	c.Assert(len(errCh), Equals, 0)

	// get again, not exists now.
	stm, rev5, err := GetSubTaskStage(etcdTestCli, source, task1)
	c.Assert(err, IsNil)
	c.Assert(rev5, Equals, rev4)
	c.Assert(stm, HasLen, 0)
}

func (t *testForEtcd) TestGetSubTaskStageConfigEtcd(c *C) {
	defer clearTestInfoOperation(c)

	cfg := config.SubTaskConfig{}
	c.Assert(cfg.Decode(config.SampleSubtaskConfig, true), IsNil)
	source := cfg.SourceID
	task := cfg.Name
	stage := NewSubTaskStage(pb.Stage_Running, source, task)

	// no subtask stage and config
	stm, validatorM, scm, rev1, err := GetSubTaskStageConfig(etcdTestCli, source)
	c.Assert(err, IsNil)
	c.Assert(rev1, Greater, int64(0))
	c.Assert(stm, HasLen, 0)
	c.Assert(validatorM, HasLen, 0)
	c.Assert(scm, HasLen, 0)

	// put subtask config and stage at the same time
	rev2, err := PutSubTaskCfgStage(etcdTestCli, []config.SubTaskConfig{cfg}, []Stage{stage}, []Stage{stage})
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// get subtask config and stage at the same time
	stm, validatorM, scm, rev3, err := GetSubTaskStageConfig(etcdTestCli, source)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	c.Assert(stm, HasLen, 1)
	c.Assert(validatorM, HasLen, 1)
	stage.Revision = rev2
	c.Assert(stm[task], DeepEquals, stage)
	c.Assert(validatorM[task], DeepEquals, stage)
	c.Assert(scm[task], DeepEquals, cfg)
}
