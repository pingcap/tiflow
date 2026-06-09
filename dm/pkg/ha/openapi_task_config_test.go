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
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/pingcap/tiflow/dm/openapi/fixtures"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

func (s *testForEtcd) TestOpenAPITaskConfigEtcd() {
	defer clearTestInfoOperation(s.T())

	task1, err := fixtures.GenNoShardOpenAPITaskForTest()
	task1.Name = "test-1"
	s.Require().NoError(err)
	task2, err := fixtures.GenShardAndFilterOpenAPITaskForTest()
	task2.Name = "test-2"
	s.Require().NoError(err)

	// no openapi task config exist.
	task1InEtcd, err := GetOpenAPITaskTemplate(etcdTestCli, task1.Name)
	s.Require().NoError(err)
	s.Require().Nil(task1InEtcd)

	task2InEtcd, err := GetOpenAPITaskTemplate(etcdTestCli, task2.Name)
	s.Require().NoError(err)
	s.Require().Nil(task2InEtcd)

	tasks, err := GetAllOpenAPITaskTemplate(etcdTestCli)
	s.Require().NoError(err)
	s.Require().Len(tasks, 0)

	// put openapi task config .
	s.Require().NoError(PutOpenAPITaskTemplate(etcdTestCli, task1, false))
	s.Require().NoError(PutOpenAPITaskTemplate(etcdTestCli, task2, false))

	task1InEtcd, err = GetOpenAPITaskTemplate(etcdTestCli, task1.Name)
	s.Require().NoError(err)
	s.Require().Equal(task1, *task1InEtcd)

	task2InEtcd, err = GetOpenAPITaskTemplate(etcdTestCli, task2.Name)
	s.Require().NoError(err)
	s.Require().Equal(task2, *task2InEtcd)

	tasks, err = GetAllOpenAPITaskTemplate(etcdTestCli)
	s.Require().NoError(err)
	s.Require().Len(tasks, 2)

	// put openapi task config again without overwrite will fail
	s.Require().True(terror.ErrOpenAPITaskConfigExist.Equal(PutOpenAPITaskTemplate(etcdTestCli, task1, false)))

	// in overwrite mode, it will overwrite the old one.
	task1.TaskMode = openapi.TaskTaskModeFull
	s.Require().NoError(PutOpenAPITaskTemplate(etcdTestCli, task1, true))
	task1InEtcd, err = GetOpenAPITaskTemplate(etcdTestCli, task1.Name)
	s.Require().NoError(err)
	s.Require().Equal(task1, *task1InEtcd)

	// put task config that not exist will fail
	task3, err := fixtures.GenNoShardOpenAPITaskForTest()
	s.Require().NoError(err)
	task3.Name = "test-3"
	s.Require().True(terror.ErrOpenAPITaskConfigNotExist.Equal(UpdateOpenAPITaskTemplate(etcdTestCli, task3)))

	// update exist openapi task config will success
	task1.TaskMode = openapi.TaskTaskModeAll
	s.Require().NoError(UpdateOpenAPITaskTemplate(etcdTestCli, task1))
	task1InEtcd, err = GetOpenAPITaskTemplate(etcdTestCli, task1.Name)
	s.Require().NoError(err)
	s.Require().Equal(task1, *task1InEtcd)

	// delete task config
	s.Require().NoError(DeleteOpenAPITaskTemplate(etcdTestCli, task1.Name))
	tasks, err = GetAllOpenAPITaskTemplate(etcdTestCli)
	s.Require().NoError(err)
	s.Require().Len(tasks, 1)
}
