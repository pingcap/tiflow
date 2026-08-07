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

package openapi

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

var _ = check.Suite(&taskSuite{})

type taskSuite struct{}

func TestTask(t *testing.T) {
	check.TestingT(t)
}

func (t *taskSuite) TestTaskAdjust(c *check.C) {
	meta := "test"
	timezone := "Asia/Shanghai"
	// test no error
	task1 := &Task{MetaSchema: &meta, OnDuplicate: TaskOnDuplicateError, Timezone: &timezone}
	c.Assert(task1.Adjust(), check.IsNil)
	c.Assert(*task1.MetaSchema, check.Equals, meta)
	c.Assert(*task1.Timezone, check.Equals, timezone)

	// test default meta
	task3 := &Task{OnDuplicate: TaskOnDuplicateError}
	c.Assert(task3.Adjust(), check.IsNil)
	c.Assert(*task3.MetaSchema, check.Equals, defaultMetaSchema)

	// an explicit empty timezone keeps the downstream database default.
	emptyTimezone := ""
	task4 := &Task{OnDuplicate: TaskOnDuplicateError, Timezone: &emptyTimezone}
	c.Assert(task4.Adjust(), check.IsNil)

	invalidTimezone := "invalid/timezone"
	task5 := &Task{OnDuplicate: TaskOnDuplicateError, Timezone: &invalidTimezone}
	c.Assert(terror.ErrConfigInvalidTimezone.Equal(task5.Adjust()), check.IsTrue)
}
