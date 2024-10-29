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

	"github.com/pingcap/check"
)

type testStruct struct {
	TaskCliArgs
	FutureField string `toml:"future_field" json:"future_field"`
}

func (t *testStruct) ToJSON() string {
	cfg, _ := json.Marshal(t)
	return string(cfg)
}

func (t *testConfig) TestTaskCliArgsDowngrade(c *check.C) {
	s := testStruct{
		TaskCliArgs: TaskCliArgs{"123", "1s", "1s"},
		FutureField: "456",
	}
	data := s.ToJSON()

	expected := `{"start_time":"123","safe_mode_duration":"1s","wait_time_on_stop":"1s","future_field":"456"}`
	c.Assert(data, check.Equals, expected)

	afterDowngrade := &TaskCliArgs{}
	c.Assert(afterDowngrade.Decode([]byte(data)), check.IsNil)
	c.Assert(afterDowngrade.StartTime, check.Equals, "123")
}

func (t *testConfig) TestTaskCliArgsVerify(c *check.C) {
	empty := TaskCliArgs{}
	c.Assert(empty.Verify(), check.IsNil)
	rightStartTime := TaskCliArgs{StartTime: "2006-01-02T15:04:05"}
	c.Assert(rightStartTime.Verify(), check.IsNil)
	rightStartTime = TaskCliArgs{StartTime: "2006-01-02 15:04:05"}
	c.Assert(rightStartTime.Verify(), check.IsNil)
	wrongStartTime := TaskCliArgs{StartTime: "15:04:05"}
	c.Assert(wrongStartTime.Verify(), check.NotNil)

	rightSafeModeDuration := TaskCliArgs{SafeModeDuration: "1s"}
	c.Assert(rightSafeModeDuration.Verify(), check.IsNil)
	wrongSafeModeDuration := TaskCliArgs{SafeModeDuration: "1"}
	c.Assert(wrongSafeModeDuration.Verify(), check.NotNil)

	rightWaitTimeOnStop := TaskCliArgs{WaitTimeOnStop: "1s"}
	c.Assert(rightWaitTimeOnStop.Verify(), check.IsNil)
	wrongWaitTimeOnStop := TaskCliArgs{WaitTimeOnStop: "1"}
	c.Assert(wrongWaitTimeOnStop.Verify(), check.NotNil)
}
