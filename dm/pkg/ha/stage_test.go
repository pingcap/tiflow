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

	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
)

func (s *testForEtcd) TestStageJSON() {
	// stage for relay.
	rs1 := NewRelayStage(pb.Stage_Running, "mysql-replica-1")
	j, err := rs1.toJSON()
	s.Require().NoError(err)
	s.Require().Equal(`{"expect":2,"source":"mysql-replica-1"}`, j)
	s.Require().Equal(rs1.String(), j)

	rs2, err := stageFromJSON(j)
	s.Require().NoError(err)
	s.Require().Equal(rs1, rs2)

	// stage for subtask.
	sts1 := NewSubTaskStage(pb.Stage_Paused, "mysql-replica-1", "task1")
	j, err = sts1.toJSON()
	s.Require().NoError(err)
	s.Require().Equal(`{"expect":3,"source":"mysql-replica-1","task":"task1"}`, j)
	s.Require().Equal(sts1.String(), j)

	sts2, err := stageFromJSON(j)
	s.Require().NoError(err)
	s.Require().Equal(sts1, sts2)
}

func (s *testForEtcd) TestRelayStageEtcd() {
	defer clearTestInfoOperation(s.T())

	var (
		watchTimeout = 2 * time.Second
		source1      = "mysql-replica-1"
		source2      = "mysql-replica-2"
		emptyStage   = Stage{}
		stage1       = NewRelayStage(pb.Stage_Running, source1)
		stage2       = NewRelayStage(pb.Stage_Paused, source2)
	)
	s.Require().False(stage1.IsDeleted)

	// no relay stage exist.
	st1, rev1, err := GetRelayStage(etcdTestCli, source1)
	s.Require().NoError(err)
	s.Require().Greater(rev1, int64(0))
	s.Require().Equal(emptyStage, st1)

	// put two stage.
	rev2, err := PutRelayStage(etcdTestCli, stage1, stage2)
	s.Require().NoError(err)
	s.Require().Greater(rev2, rev1)

	// watch the PUT operation for stage1.
	stageCh := make(chan Stage, 10)
	errCh := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), watchTimeout)
	WatchRelayStage(ctx, etcdTestCli, source1, rev2, stageCh, errCh)
	cancel()
	close(stageCh)
	close(errCh)
	s.Require().Equal(1, len(stageCh))
	stage1.Revision = rev2
	s.Require().Equal(stage1, <-stageCh)
	s.Require().Equal(0, len(errCh))

	// get stage1 back.
	st2, rev3, err := GetRelayStage(etcdTestCli, source1)
	s.Require().NoError(err)
	s.Require().Equal(rev2, rev3)
	s.Require().Equal(stage1, st2)

	// get two stages.
	stm, rev3, err := GetAllRelayStage(etcdTestCli)
	s.Require().NoError(err)
	s.Require().Equal(rev2, rev3)
	s.Require().Len(stm, 2)
	stage2.Revision = rev2
	s.Require().Equal(stage1, stm[source1])
	s.Require().Equal(stage2, stm[source2])

	// delete stage1.
	deleteOp := deleteRelayStageOp(source1)
	resp, err := etcdTestCli.Txn(context.Background()).Then(deleteOp).Commit()
	s.Require().NoError(err)
	rev4 := resp.Header.Revision
	s.Require().Greater(rev4, rev3)

	// watch the DELETE operation for stage1.
	stageCh = make(chan Stage, 10)
	errCh = make(chan error, 10)
	ctx, cancel = context.WithTimeout(context.Background(), watchTimeout)
	WatchRelayStage(ctx, etcdTestCli, source1, rev4, stageCh, errCh)
	cancel()
	close(stageCh)
	close(errCh)
	s.Require().Equal(1, len(stageCh))
	st3 := <-stageCh
	s.Require().True(st3.IsDeleted)
	s.Require().Equal(0, len(errCh))

	// get again, not exists now.
	st4, rev5, err := GetRelayStage(etcdTestCli, source1)
	s.Require().NoError(err)
	s.Require().Equal(rev4, rev5)
	s.Require().Equal(emptyStage, st4)
}

func (s *testForEtcd) TestSubTaskStageEtcd() {
	defer clearTestInfoOperation(s.T())

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
	s.Require().NoError(err)
	s.Require().Greater(rev1, int64(0))
	s.Require().Len(st1, 0)

	// put two stages.
	rev2, err := PutSubTaskStage(etcdTestCli, stage1, stage2)
	s.Require().NoError(err)
	s.Require().Greater(rev2, rev1)

	// watch the PUT operation for stages.
	stageCh := make(chan Stage, 10)
	errCh := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), watchTimeout)
	WatchSubTaskStage(ctx, etcdTestCli, source, rev2, stageCh, errCh)
	cancel()
	close(stageCh)
	close(errCh)
	s.Require().Equal(2, len(stageCh))
	stage1.Revision = rev2
	stage2.Revision = rev2
	s.Require().Equal(stage1, <-stageCh)
	s.Require().Equal(stage2, <-stageCh)
	s.Require().Equal(0, len(errCh))

	// get stages back without specified task.
	stm, rev3, err := GetSubTaskStage(etcdTestCli, source, "")
	s.Require().NoError(err)
	s.Require().Equal(rev2, rev3)
	s.Require().Len(stm, 2)
	s.Require().Equal(stage1, stm[task1])
	s.Require().Equal(stage2, stm[task2])

	// get the stage back with specified task.
	stm, rev3, err = GetSubTaskStage(etcdTestCli, source, task1)
	s.Require().NoError(err)
	s.Require().Equal(rev2, rev3)
	s.Require().Len(stm, 1)
	s.Require().Equal(stage1, stm[task1])

	// get all stages.
	stmm, rev3, err := GetAllSubTaskStage(etcdTestCli)
	s.Require().NoError(err)
	s.Require().Equal(rev2, rev3)
	s.Require().Len(stmm, 1)
	s.Require().Len(stmm[source], 2)
	s.Require().Equal(stage1, stmm[source][task1])
	s.Require().Equal(stage2, stmm[source][task2])

	// delete two stages.
	rev4, err := DeleteSubTaskStage(etcdTestCli, stage1, stage2)
	s.Require().NoError(err)
	s.Require().Greater(rev4, rev3)

	// watch the DELETE operation for stages.
	stageCh = make(chan Stage, 10)
	errCh = make(chan error, 10)
	ctx, cancel = context.WithTimeout(context.Background(), watchTimeout)
	WatchSubTaskStage(ctx, etcdTestCli, source, rev4, stageCh, errCh)
	cancel()
	close(stageCh)
	close(errCh)
	s.Require().Equal(2, len(stageCh))
	for st2 := range stageCh {
		s.Require().True(st2.IsDeleted)
	}
	s.Require().Equal(0, len(errCh))

	// get again, not exists now.
	stm, rev5, err := GetSubTaskStage(etcdTestCli, source, task1)
	s.Require().NoError(err)
	s.Require().Equal(rev4, rev5)
	s.Require().Len(stm, 0)
}

func (s *testForEtcd) TestGetSubTaskStageConfigEtcd() {
	defer clearTestInfoOperation(s.T())

	cfg := config.SubTaskConfig{}
	s.Require().NoError(cfg.Decode(config.SampleSubtaskConfig, true))
	source := cfg.SourceID
	task := cfg.Name
	stage := NewSubTaskStage(pb.Stage_Running, source, task)

	// no subtask stage and config
	stm, validatorM, scm, rev1, err := GetSubTaskStageConfig(etcdTestCli, source)
	s.Require().NoError(err)
	s.Require().Greater(rev1, int64(0))
	s.Require().Len(stm, 0)
	s.Require().Len(validatorM, 0)
	s.Require().Len(scm, 0)

	// put subtask config and stage at the same time
	rev2, err := PutSubTaskCfgStage(etcdTestCli, []config.SubTaskConfig{cfg}, []Stage{stage}, []Stage{stage})
	s.Require().NoError(err)
	s.Require().Greater(rev2, rev1)

	// get subtask config and stage at the same time
	stm, validatorM, scm, rev3, err := GetSubTaskStageConfig(etcdTestCli, source)
	s.Require().NoError(err)
	s.Require().Equal(rev2, rev3)
	s.Require().Len(stm, 1)
	s.Require().Len(validatorM, 1)
	stage.Revision = rev2
	s.Require().Equal(stage, stm[task])
	s.Require().Equal(stage, validatorM[task])
	s.Require().Equal(cfg, scm[task])
}
