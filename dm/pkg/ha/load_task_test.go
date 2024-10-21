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

	"github.com/pingcap/check"
)

func (t *testForEtcd) TestLoadTaskEtcd(c *check.C) {
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
	c.Assert(err, check.IsNil)
	c.Assert(tlswm, check.HasLen, 0)

	// put load worker for task1, source1, worker1
	rev2, err := PutLoadTask(etcdTestCli, task1, source1, worker1)
	c.Assert(err, check.IsNil)
	c.Assert(rev2, check.Greater, rev1)

	// get worker for task1, source1
	worker, rev3, err := GetLoadTask(etcdTestCli, task1, source1)
	c.Assert(err, check.IsNil)
	c.Assert(worker, check.Equals, worker1)
	c.Assert(rev3, check.Equals, rev2)

	// put load worker for task1, source1, worker1 again
	rev4, err := PutLoadTask(etcdTestCli, task1, source1, worker1)
	c.Assert(err, check.IsNil)
	c.Assert(rev4, check.Greater, rev3)

	// get worker for task1, source1 again
	worker, rev5, err := GetLoadTask(etcdTestCli, task1, source1)
	c.Assert(err, check.IsNil)
	c.Assert(worker, check.Equals, worker1)
	c.Assert(rev5, check.Equals, rev4)

	// put load worker for task1, source2, worker2
	rev6, err := PutLoadTask(etcdTestCli, task1, source2, worker2)
	c.Assert(err, check.IsNil)
	c.Assert(rev6, check.Greater, rev5)

	// get all load worker
	tlswm, rev7, err := GetAllLoadTask(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(rev7, check.Equals, rev6)
	c.Assert(tlswm, check.HasLen, 1)
	c.Assert(tlswm, check.HasKey, task1)
	c.Assert(tlswm[task1], check.HasKey, source1)
	c.Assert(tlswm[task1], check.HasKey, source2)
	c.Assert(tlswm[task1][source1], check.Equals, worker1)
	c.Assert(tlswm[task1][source2], check.Equals, worker2)

	// Delete load worker for task1, source1
	rev8, succ, err := DelLoadTask(etcdTestCli, task1, source1)
	c.Assert(err, check.IsNil)
	c.Assert(rev8, check.Greater, rev7)
	c.Assert(succ, check.IsTrue)

	worker, rev9, err := GetLoadTask(etcdTestCli, task1, source1)
	c.Assert(err, check.IsNil)
	c.Assert(rev9, check.Equals, rev8)
	c.Assert(worker, check.Equals, "")

	worker, rev10, err := GetLoadTask(etcdTestCli, task1, source2)
	c.Assert(err, check.IsNil)
	c.Assert(rev10, check.Equals, rev9)
	c.Assert(worker, check.Equals, worker2)

	// Delete load worker by task
	rev11, succ, err := DelLoadTaskByTask(etcdTestCli, task1)
	c.Assert(err, check.IsNil)
	c.Assert(rev11, check.Greater, rev10)
	c.Assert(succ, check.IsTrue)

	tslwm, rev12, err := GetAllLoadTask(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(rev12, check.Equals, rev11)
	c.Assert(tslwm, check.HasLen, 0)

	rev13, err := PutLoadTask(etcdTestCli, task2, source1, worker2)
	c.Assert(err, check.IsNil)
	c.Assert(rev13, check.Greater, rev12)

	// watch operations for the load worker.
	loadTaskCh := make(chan LoadTask, 10)
	errCh := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), watchTimeout)
	WatchLoadTask(ctx, etcdTestCli, rev7+1, loadTaskCh, errCh)
	cancel()
	close(loadTaskCh)
	close(errCh)
	c.Assert(len(loadTaskCh), check.Equals, 3)
	delLoadTask1 := <-loadTaskCh
	c.Assert(delLoadTask1.Task, check.Equals, task1)
	c.Assert(delLoadTask1.Source, check.Equals, source1)
	c.Assert(delLoadTask1.IsDelete, check.IsTrue)
	DelLoadTask2 := <-loadTaskCh
	c.Assert(DelLoadTask2.Task, check.Equals, task1)
	c.Assert(DelLoadTask2.Source, check.Equals, source2)
	c.Assert(DelLoadTask2.IsDelete, check.IsTrue)
	putLoadTask := <-loadTaskCh
	c.Assert(putLoadTask.Task, check.Equals, task2)
	c.Assert(putLoadTask.Source, check.Equals, source1)
	c.Assert(putLoadTask.IsDelete, check.IsFalse)
	c.Assert(putLoadTask.Worker, check.Equals, worker2)
}
