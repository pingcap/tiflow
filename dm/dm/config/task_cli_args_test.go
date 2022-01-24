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

package config

import (
	"encoding/json"

	. "github.com/pingcap/check"
)

type testStruct struct {
	TaskCliArgs
	FutureField string `toml:"future_field" json:"future_field"`
}

func (t *testStruct) ToJSON() string {
	cfg, _ := json.Marshal(t)
	return string(cfg)
}

func (t *testConfig) TestTaskCliArgsDowngrade(c *C) {
	s := testStruct{
		TaskCliArgs: TaskCliArgs{"123"},
		FutureField: "456",
	}
	data := s.ToJSON()

	expected := `{"start_time":"123","future_field":"456"}`
	c.Assert(data, Equals, expected)

	afterDowngrade := &TaskCliArgs{}
	c.Assert(afterDowngrade.Decode([]byte(data)), IsNil)
	c.Assert(afterDowngrade.StartTime, Equals, "123")
}

func (t *testConfig) TestTaskCliArgsVerify(c *C) {
	empty := TaskCliArgs{}
	c.Assert(empty.Verify(), IsNil)
	rightStartTime := TaskCliArgs{StartTime: "2006-01-02T15:04:05"}
	c.Assert(rightStartTime.Verify(), IsNil)
	rightStartTime = TaskCliArgs{StartTime: "2006-01-02 15:04:05"}
	c.Assert(rightStartTime.Verify(), IsNil)
	wrongStartTime := TaskCliArgs{StartTime: "15:04:05"}
	c.Assert(wrongStartTime.Verify(), NotNil)
}
