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
	"testing"

	"github.com/stretchr/testify/require"
)

type testStruct struct {
	TaskCliArgs
	FutureField string `toml:"future_field" json:"future_field"`
}

func (t *testStruct) ToJSON() string {
	cfg, _ := json.Marshal(t)
	return string(cfg)
}

func TestTaskCliArgsDowngrade(t *testing.T) {
	s := testStruct{
		TaskCliArgs: TaskCliArgs{"123", "1s", "1s"},
		FutureField: "456",
	}
	data := s.ToJSON()

	expected := `{"start_time":"123","safe_mode_duration":"1s","wait_time_on_stop":"1s","future_field":"456"}`
	require.Equal(t, expected, data)

	afterDowngrade := &TaskCliArgs{}
	require.NoError(t, afterDowngrade.Decode([]byte(data)))
	require.Equal(t, "123", afterDowngrade.StartTime)
}

func TestTaskCliArgsVerify(t *testing.T) {
	empty := TaskCliArgs{}
	require.NoError(t, empty.Verify())
	rightStartTime := TaskCliArgs{StartTime: "2006-01-02T15:04:05"}
	require.NoError(t, rightStartTime.Verify())
	rightStartTime = TaskCliArgs{StartTime: "2006-01-02 15:04:05"}
	require.NoError(t, rightStartTime.Verify())
	rightStartTime = TaskCliArgs{StartTime: "2006-01-02T15:04:05+08:00"}
	require.NoError(t, rightStartTime.Verify())
	rightStartTime = TaskCliArgs{StartTime: "2006-01-02 15:04:05+08:00"}
	require.NoError(t, rightStartTime.Verify())
	rightStartTime = TaskCliArgs{StartTime: "2006-01-02T15:04:05+0800"}
	require.NoError(t, rightStartTime.Verify())
	rightStartTime = TaskCliArgs{StartTime: "2006-01-02 15:04:05+0800"}
	require.NoError(t, rightStartTime.Verify())
	wrongStartTime := TaskCliArgs{StartTime: "15:04:05"}
	require.Error(t, wrongStartTime.Verify())

	rightSafeModeDuration := TaskCliArgs{SafeModeDuration: "1s"}
	require.NoError(t, rightSafeModeDuration.Verify())
	wrongSafeModeDuration := TaskCliArgs{SafeModeDuration: "1"}
	require.Error(t, wrongSafeModeDuration.Verify())

	rightWaitTimeOnStop := TaskCliArgs{WaitTimeOnStop: "1s"}
	require.NoError(t, rightWaitTimeOnStop.Verify())
	wrongWaitTimeOnStop := TaskCliArgs{WaitTimeOnStop: "1"}
	require.Error(t, wrongWaitTimeOnStop.Verify())
}
