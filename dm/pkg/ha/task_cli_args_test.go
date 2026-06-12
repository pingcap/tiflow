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
	"github.com/pingcap/tiflow/dm/config"
)

func (s *testForEtcd) TestTaskCliArgs() {
	defer clearTestInfoOperation(s.T())

	task := "test-task-cli-args"
	source1 := "source1"
	source2 := "source2"

	checkNotExist := func(source string) {
		ret, err := GetTaskCliArgs(etcdTestCli, task, source)
		s.Require().NoError(err)
		s.Require().Nil(ret)
	}

	checkNotExist(source1)
	checkNotExist(source2)

	args := config.TaskCliArgs{
		StartTime: "123",
	}
	err := PutTaskCliArgs(etcdTestCli, task, []string{source1}, args)
	s.Require().NoError(err)

	ret, err := GetTaskCliArgs(etcdTestCli, task, source1)
	s.Require().NoError(err)
	s.Require().NotNil(ret)
	s.Require().Equal(args, *ret)
	checkNotExist(source2)

	// put will overwrite
	args.StartTime = "456"
	err = PutTaskCliArgs(etcdTestCli, task, []string{source1, source2}, args)
	s.Require().NoError(err)

	ret, err = GetTaskCliArgs(etcdTestCli, task, source1)
	s.Require().NoError(err)
	s.Require().NotNil(ret)
	s.Require().Equal(args, *ret)
	ret, err = GetTaskCliArgs(etcdTestCli, task, source2)
	s.Require().NoError(err)
	s.Require().NotNil(ret)
	s.Require().Equal(args, *ret)

	// test delete one source
	err = DeleteTaskCliArgs(etcdTestCli, task, []string{source1})
	s.Require().NoError(err)

	checkNotExist(source1)
	ret, err = GetTaskCliArgs(etcdTestCli, task, source2)
	s.Require().NoError(err)
	s.Require().NotNil(ret)
	s.Require().Equal(args, *ret)

	// test delete all source
	err = DeleteAllTaskCliArgs(etcdTestCli, task)
	s.Require().NoError(err)
	checkNotExist(source2)
}
