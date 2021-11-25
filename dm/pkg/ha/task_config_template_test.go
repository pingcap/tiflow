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
	. "github.com/pingcap/check"

	"github.com/pingcap/ticdc/dm/openapi"
	"github.com/pingcap/ticdc/dm/openapi/fixtures"
	"github.com/pingcap/ticdc/dm/pkg/terror"
)

func (t *testForEtcd) TestTaskConfigTemplateEtcd(c *C) {
	defer clearTestInfoOperation(c)

	task1, err := fixtures.GenNoShardOpenAPITaskForTest()
	task1.Name = "test-1"
	c.Assert(err, IsNil)
	task2, err := fixtures.GenShardAndFilterOpenAPITaskForTest()
	task2.Name = "test-2"
	c.Assert(err, IsNil)

	// no task config template exist.
	task1InEtcd, err := GetTaskConfigTemplate(etcdTestCli, task1.Name)
	c.Assert(err, IsNil)
	c.Assert(task1InEtcd, IsNil)

	task2InEtcd, err := GetTaskConfigTemplate(etcdTestCli, task2.Name)
	c.Assert(err, IsNil)
	c.Assert(task2InEtcd, IsNil)

	tasks, err := GetAllTaskConfigTemplate(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 0)

	// put task config template.
	c.Assert(PutTaskConfigTemplate(etcdTestCli, task1, false), IsNil)
	c.Assert(PutTaskConfigTemplate(etcdTestCli, task2, false), IsNil)

	task1InEtcd, err = GetTaskConfigTemplate(etcdTestCli, task1.Name)
	c.Assert(err, IsNil)
	c.Assert(*task1InEtcd, DeepEquals, task1)

	task2InEtcd, err = GetTaskConfigTemplate(etcdTestCli, task2.Name)
	c.Assert(err, IsNil)
	c.Assert(*task2InEtcd, DeepEquals, task2)

	tasks, err = GetAllTaskConfigTemplate(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)

	// put task config template again without overwrite will fail
	c.Assert(terror.ErrTaskConfigTemplateExists.Equal(PutTaskConfigTemplate(etcdTestCli, task1, false)), IsTrue)

	// in overwrite mode, it will overwrite the old one.
	task1.TaskMode = openapi.TaskTaskModeFull
	c.Assert(PutTaskConfigTemplate(etcdTestCli, task1, true), IsNil)
	task1InEtcd, err = GetTaskConfigTemplate(etcdTestCli, task1.Name)
	c.Assert(err, IsNil)
	c.Assert(*task1InEtcd, DeepEquals, task1)

	// delete task config
	c.Assert(DeleteTaskConfigTemplate(etcdTestCli, task1.Name), IsNil)
	tasks, err = GetAllTaskConfigTemplate(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
}
