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

	. "github.com/pingcap/check"
)

func (t *testForEtcd) TestLoadTaskEtcd(c *C) {
	var (
		worker1      = "worker1"
		worker2      = "worker2"
		source1      = "source1"
		source2      = "source2"
		task1        = "task1"
		task2        = "task2"
		watchTimeout = 2 * time.Second
	)
	defer clearTestInfoOperation(c)

	// no load worker exist.
	tlswm, rev1, err := GetAllLoadTask(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(tlswm, HasLen, 0)

	// put load worker for task1, source1, worker1
	rev2, err := PutLoadTask(etcdTestCli, task1, source1, worker1)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// get worker for task1, source1
	worker, rev3, err := GetLoadTask(etcdTestCli, task1, source1)
	c.Assert(err, IsNil)
	c.Assert(worker, Equals, worker1)
	c.Assert(rev3, Equals, rev2)

	// put load worker for task1, source1, worker1 again
	rev4, err := PutLoadTask(etcdTestCli, task1, source1, worker1)
	c.Assert(err, IsNil)
	c.Assert(rev4, Greater, rev3)

	// get worker for task1, source1 again
	worker, rev5, err := GetLoadTask(etcdTestCli, task1, source1)
	c.Assert(err, IsNil)
	c.Assert(worker, Equals, worker1)
	c.Assert(rev5, Equals, rev4)

	// put load worker for task1, source2, worker2
	rev6, err := PutLoadTask(etcdTestCli, task1, source2, worker2)
	c.Assert(err, IsNil)
	c.Assert(rev6, Greater, rev5)

	// get all load worker
	tlswm, rev7, err := GetAllLoadTask(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev7, Equals, rev6)
	c.Assert(tlswm, HasLen, 1)
	c.Assert(tlswm, HasKey, task1)
	c.Assert(tlswm[task1], HasKey, source1)
	c.Assert(tlswm[task1], HasKey, source2)
	c.Assert(tlswm[task1][source1], Equals, worker1)
	c.Assert(tlswm[task1][source2], Equals, worker2)

	// Delete load worker for task1, source1
	rev8, succ, err := DelLoadTask(etcdTestCli, task1, source1)
	c.Assert(err, IsNil)
	c.Assert(rev8, Greater, rev7)
	c.Assert(succ, IsTrue)

	worker, rev9, err := GetLoadTask(etcdTestCli, task1, source1)
	c.Assert(err, IsNil)
	c.Assert(rev9, Equals, rev8)
	c.Assert(worker, Equals, "")

	worker, rev10, err := GetLoadTask(etcdTestCli, task1, source2)
	c.Assert(err, IsNil)
	c.Assert(rev10, Equals, rev9)
	c.Assert(worker, Equals, worker2)

	// Delete load worker by task
	rev11, succ, err := DelLoadTaskByTask(etcdTestCli, task1)
	c.Assert(err, IsNil)
	c.Assert(rev11, Greater, rev10)
	c.Assert(succ, IsTrue)

	tslwm, rev12, err := GetAllLoadTask(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev12, Equals, rev11)
	c.Assert(tslwm, HasLen, 0)

	rev13, err := PutLoadTask(etcdTestCli, task2, source1, worker2)
	c.Assert(err, IsNil)
	c.Assert(rev13, Greater, rev12)

	// watch operations for the load worker.
	loadTaskCh := make(chan LoadTask, 10)
	errCh := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), watchTimeout)
	WatchLoadTask(ctx, etcdTestCli, rev7+1, loadTaskCh, errCh)
	cancel()
	close(loadTaskCh)
	close(errCh)
	c.Assert(len(loadTaskCh), Equals, 3)
	delLoadTask1 := <-loadTaskCh
	c.Assert(delLoadTask1.Task, Equals, task1)
	c.Assert(delLoadTask1.Source, Equals, source1)
	c.Assert(delLoadTask1.IsDelete, IsTrue)
	DelLoadTask2 := <-loadTaskCh
	c.Assert(DelLoadTask2.Task, Equals, task1)
	c.Assert(DelLoadTask2.Source, Equals, source2)
	c.Assert(DelLoadTask2.IsDelete, IsTrue)
	putLoadTask := <-loadTaskCh
	c.Assert(putLoadTask.Task, Equals, task2)
	c.Assert(putLoadTask.Source, Equals, source1)
	c.Assert(putLoadTask.IsDelete, IsFalse)
	c.Assert(putLoadTask.Worker, Equals, worker2)
}
