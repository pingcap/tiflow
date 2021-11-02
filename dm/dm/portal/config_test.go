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

package portal

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func (t *testConfigSuite) TestConfig(c *C) {
	testCases := []struct {
		port         int
		taskFilePath string
		timeout      int
		valid        bool
		str          string
	}{
		{
			1234,
			"/tmp",
			5,
			true,
			"dm-portal config: { port: 1234, task-file-path: /tmp }",
		}, {
			123456,
			"tmp",
			5,
			false,
			"",
		}, {
			1234,
			"/User",
			5,
			false,
			"",
		}, {
			1234,
			"/tmp",
			0,
			false,
			"",
		},
	}

	for _, testCase := range testCases {
		cfg := &Config{
			Port:         testCase.port,
			TaskFilePath: testCase.taskFilePath,
			Timeout:      testCase.timeout,
		}
		c.Assert(cfg.Valid() == nil, Equals, testCase.valid)
		if testCase.valid {
			c.Assert(cfg.String(), Equals, testCase.str)
		}
	}
}

func (t *testConfigSuite) TestTaskConfigVerify(c *C) {
	testCases := []struct {
		cfg   DMTaskConfig
		valid bool
	}{
		{
			cfg: DMTaskConfig{
				Name: "",
			},
			valid: false,
		}, {
			cfg: DMTaskConfig{
				Name:     "test",
				TaskMode: "abc",
			},
			valid: false,
		}, {
			cfg: DMTaskConfig{
				Name:     "test",
				TaskMode: "all",
				MySQLInstances: []*MySQLInstance{
					{
						SourceID: "",
					},
				},
			},
			valid: false,
		}, {
			cfg: DMTaskConfig{
				Name:     "test",
				TaskMode: "incremental",
				MySQLInstances: []*MySQLInstance{
					{
						SourceID: "source-1",
						Meta: &Meta{
							BinLogName: "",
						},
					},
				},
			},
			valid: false,
		}, {
			cfg: DMTaskConfig{
				Name:     "test",
				TaskMode: "incremental",
				MySQLInstances: []*MySQLInstance{
					{
						SourceID: "source-1",
						Meta: &Meta{
							BinLogName: "log-bin.0001",
						},
					},
				},
			},
			valid: true,
		},
	}

	for _, testCase := range testCases {
		c.Assert(testCase.cfg.Verify() == nil, Equals, testCase.valid)
	}
}
