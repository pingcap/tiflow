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

func (t *taskSuite) TestTaskTargetSession(c *check.C) {
	input := map[string]string{"FOREIGN_KEY_CHECKS": "1"}
	task := &Task{
		TargetConfig: TaskTargetDataBase{
			Session: &TaskTargetDataBase_Session{AdditionalProperties: input},
		},
	}
	c.Assert(task.Adjust(), check.IsNil)
	c.Assert(task.TargetConfig.Session.AdditionalProperties, check.DeepEquals, map[string]string{
		"foreign_key_checks": "1",
	})
	c.Assert(input, check.DeepEquals, map[string]string{"FOREIGN_KEY_CHECKS": "1"})
	task.TargetConfig.Session.AdditionalProperties["foreign_key_checks"] = "0"
	c.Assert(input["FOREIGN_KEY_CHECKS"], check.Equals, "1")
	normalizedDefault, err := NormalizeTaskTargetSession(map[string]string{"foreign_key_checks": "0"})
	c.Assert(err, check.IsNil)
	c.Assert(normalizedDefault, check.DeepEquals, map[string]string{"foreign_key_checks": "0"})

	emptyTask := &Task{
		TargetConfig: TaskTargetDataBase{
			Session: &TaskTargetDataBase_Session{AdditionalProperties: map[string]string{}},
		},
	}
	c.Assert(emptyTask.Adjust(), check.IsNil)
	c.Assert(emptyTask.TargetConfig.Session, check.IsNil)

	testCases := []struct {
		name       string
		session    map[string]string
		errMessage string
	}{
		{
			name:       "unsupported key",
			session:    map[string]string{"sql_mode": "ANSI_QUOTES"},
			errMessage: `unsupported target session parameter "sql_mode"`,
		},
		{
			name: "case-normalized duplicate",
			session: map[string]string{
				"FOREIGN_KEY_CHECKS": "0",
				"foreign_key_checks": "1",
			},
			errMessage: `target session parameter "foreign_key_checks" is duplicated after case normalization`,
		},
		{
			name:       "on value",
			session:    map[string]string{"foreign_key_checks": "on"},
			errMessage: `target session parameter "foreign_key_checks" must be the exact string "0" or "1"`,
		},
		{
			name:       "value whitespace",
			session:    map[string]string{"foreign_key_checks": "1 "},
			errMessage: `target session parameter "foreign_key_checks" must be the exact string "0" or "1"`,
		},
		{
			name: "deterministic first error",
			session: map[string]string{
				"z_session": "1",
				"a_session": "1",
			},
			errMessage: `unsupported target session parameter "a_session"`,
		},
	}
	for _, testCase := range testCases {
		c.Log(testCase.name)
		invalidTask := &Task{
			TargetConfig: TaskTargetDataBase{
				Session: &TaskTargetDataBase_Session{AdditionalProperties: testCase.session},
			},
		}
		err := invalidTask.Adjust()
		c.Assert(err, check.ErrorMatches, testCase.errMessage)
	}

	var jsonTask Task
	c.Assert(
		jsonTask.FromJSON([]byte(`{"target_config":{"session":{"foreign_key_checks":1}}}`)),
		check.ErrorMatches,
		".*cannot unmarshal number.*",
	)
	c.Assert(jsonTask.FromJSON([]byte(`{"target_config":{"session":{"foreign_key_checks":null}}}`)), check.IsNil)
	c.Assert(
		jsonTask.Adjust(),
		check.ErrorMatches,
		`target session parameter "foreign_key_checks" must be the exact string "0" or "1"`,
	)
}
