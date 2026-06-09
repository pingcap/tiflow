// Copyright 2021 PingCAP, Inc.
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
)

func (s *testForEtcd) TestLoadTaskEtcd() {
	var (
		worker1      = "worker1"
		worker2      = "worker2"
		source1      = "source1"
		source2      = "source2"
		task1        = "task1"
		task2        = "task2"
		watchTimeout = 2 * time.Second
	)
	defer clearTestInfoOperation(s.T())

	// no load worker exist.
	tlswm, rev1, err := GetAllLoadTask(etcdTestCli)
	s.Require().NoError(err)
	s.Require().Len(tlswm, 0)

	// put load worker for task1, source1, worker1
	rev2, err := PutLoadTask(etcdTestCli, task1, source1, worker1)
	s.Require().NoError(err)
	s.Require().Greater(rev2, rev1)

	// get worker for task1, source1
	worker, rev3, err := GetLoadTask(etcdTestCli, task1, source1)
	s.Require().NoError(err)
	s.Require().Equal(worker1, worker)
	s.Require().Equal(rev2, rev3)

	// put load worker for task1, source1, worker1 again
	rev4, err := PutLoadTask(etcdTestCli, task1, source1, worker1)
	s.Require().NoError(err)
	s.Require().Greater(rev4, rev3)

	// get worker for task1, source1 again
	worker, rev5, err := GetLoadTask(etcdTestCli, task1, source1)
	s.Require().NoError(err)
	s.Require().Equal(worker1, worker)
	s.Require().Equal(rev4, rev5)

	// put load worker for task1, source2, worker2
	rev6, err := PutLoadTask(etcdTestCli, task1, source2, worker2)
	s.Require().NoError(err)
	s.Require().Greater(rev6, rev5)

	// get all load worker
	tlswm, rev7, err := GetAllLoadTask(etcdTestCli)
	s.Require().NoError(err)
	s.Require().Equal(rev6, rev7)
	s.Require().Len(tlswm, 1)
	s.Require().Contains(tlswm, task1)
	s.Require().Contains(tlswm[task1], source1)
	s.Require().Contains(tlswm[task1], source2)
	s.Require().Equal(worker1, tlswm[task1][source1])
	s.Require().Equal(worker2, tlswm[task1][source2])

	// Delete load worker for task1, source1
	rev8, succ, err := DelLoadTask(etcdTestCli, task1, source1)
	s.Require().NoError(err)
	s.Require().Greater(rev8, rev7)
	s.Require().True(succ)

	worker, rev9, err := GetLoadTask(etcdTestCli, task1, source1)
	s.Require().NoError(err)
	s.Require().Equal(rev8, rev9)
	s.Require().Equal("", worker)

	worker, rev10, err := GetLoadTask(etcdTestCli, task1, source2)
	s.Require().NoError(err)
	s.Require().Equal(rev9, rev10)
	s.Require().Equal(worker2, worker)

	// Delete load worker by task
	rev11, succ, err := DelLoadTaskByTask(etcdTestCli, task1)
	s.Require().NoError(err)
	s.Require().Greater(rev11, rev10)
	s.Require().True(succ)

	tslwm, rev12, err := GetAllLoadTask(etcdTestCli)
	s.Require().NoError(err)
	s.Require().Equal(rev11, rev12)
	s.Require().Len(tslwm, 0)

	rev13, err := PutLoadTask(etcdTestCli, task2, source1, worker2)
	s.Require().NoError(err)
	s.Require().Greater(rev13, rev12)

	// watch operations for the load worker.
	loadTaskCh := make(chan LoadTask, 10)
	errCh := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), watchTimeout)
	WatchLoadTask(ctx, etcdTestCli, rev7+1, loadTaskCh, errCh)
	cancel()
	close(loadTaskCh)
	close(errCh)
	s.Require().Equal(3, len(loadTaskCh))
	delLoadTask1 := <-loadTaskCh
	s.Require().Equal(task1, delLoadTask1.Task)
	s.Require().Equal(source1, delLoadTask1.Source)
	s.Require().True(delLoadTask1.IsDelete)
	DelLoadTask2 := <-loadTaskCh
	s.Require().Equal(task1, DelLoadTask2.Task)
	s.Require().Equal(source2, DelLoadTask2.Source)
	s.Require().True(DelLoadTask2.IsDelete)
	putLoadTask := <-loadTaskCh
	s.Require().Equal(task2, putLoadTask.Task)
	s.Require().Equal(source1, putLoadTask.Source)
	s.Require().False(putLoadTask.IsDelete)
	s.Require().Equal(worker2, putLoadTask.Worker)
}
