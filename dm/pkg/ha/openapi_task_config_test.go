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
	. "github.com/pingcap/check"

	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/pingcap/tiflow/dm/openapi/fixtures"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

func (t *testForEtcd) TestOpenAPITaskConfigEtcd(c *C) {
	defer clearTestInfoOperation(c)

	task1, err := fixtures.GenNoShardOpenAPITaskForTest()
	task1.Name = "test-1"
	c.Assert(err, IsNil)
	task2, err := fixtures.GenShardAndFilterOpenAPITaskForTest()
	task2.Name = "test-2"
	c.Assert(err, IsNil)

	// no openapi task config exist.
	task1InEtcd, err := GetOpenAPITaskTemplate(etcdTestCli, task1.Name)
	c.Assert(err, IsNil)
	c.Assert(task1InEtcd, IsNil)

	task2InEtcd, err := GetOpenAPITaskTemplate(etcdTestCli, task2.Name)
	c.Assert(err, IsNil)
	c.Assert(task2InEtcd, IsNil)

	tasks, err := GetAllOpenAPITaskTemplate(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 0)

	// put openapi task config .
	c.Assert(PutOpenAPITaskTemplate(etcdTestCli, task1, false), IsNil)
	c.Assert(PutOpenAPITaskTemplate(etcdTestCli, task2, false), IsNil)

	task1InEtcd, err = GetOpenAPITaskTemplate(etcdTestCli, task1.Name)
	c.Assert(err, IsNil)
	c.Assert(*task1InEtcd, DeepEquals, task1)

	task2InEtcd, err = GetOpenAPITaskTemplate(etcdTestCli, task2.Name)
	c.Assert(err, IsNil)
	c.Assert(*task2InEtcd, DeepEquals, task2)

	tasks, err = GetAllOpenAPITaskTemplate(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)

	// put openapi task config again without overwrite will fail
	c.Assert(terror.ErrOpenAPITaskConfigExist.Equal(PutOpenAPITaskTemplate(etcdTestCli, task1, false)), IsTrue)

	// in overwrite mode, it will overwrite the old one.
	task1.TaskMode = openapi.TaskTaskModeFull
	c.Assert(PutOpenAPITaskTemplate(etcdTestCli, task1, true), IsNil)
	task1InEtcd, err = GetOpenAPITaskTemplate(etcdTestCli, task1.Name)
	c.Assert(err, IsNil)
	c.Assert(*task1InEtcd, DeepEquals, task1)

	// put task config that not exist will fail
	task3, err := fixtures.GenNoShardOpenAPITaskForTest()
	c.Assert(err, IsNil)
	task3.Name = "test-3"
	c.Assert(terror.ErrOpenAPITaskConfigNotExist.Equal(UpdateOpenAPITaskTemplate(etcdTestCli, task3)), IsTrue)

	// update exist openapi task config will success
	task1.TaskMode = openapi.TaskTaskModeAll
	c.Assert(UpdateOpenAPITaskTemplate(etcdTestCli, task1), IsNil)
	task1InEtcd, err = GetOpenAPITaskTemplate(etcdTestCli, task1.Name)
	c.Assert(err, IsNil)
	c.Assert(*task1InEtcd, DeepEquals, task1)

	// delete task config
	c.Assert(DeleteOpenAPITaskTemplate(etcdTestCli, task1.Name), IsNil)
	tasks, err = GetAllOpenAPITaskTemplate(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
}
