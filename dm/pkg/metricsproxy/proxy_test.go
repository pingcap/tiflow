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

package metricsproxy

import (
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testMetricsProxySuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testMetricsProxySuite struct{}

type testCase struct {
	LabelsNames   []string
	AddArgs       [][]string
	DeleteArgs    []map[string]string
	WantResLength int
}

var testCases = []testCase{
	{
		LabelsNames: []string{
			"task",
			"name",
		},
		AddArgs: [][]string{
			{"task1", "name1"},
			{"task2", "name2"},
			{"task3", "name3"},
			{"task4", "name4"},
			{"task5", "name5"},
			{"task6", "name6"},
			{"task7", "name7"},
			{"task8", "name7"},
			{"task8", "name9"},
			{"task10", "name10"},
		},
		DeleteArgs: []map[string]string{
			{"task": "task1", "name": "name1"},
			{"task": "task1", "name": "name1"},
			{"task": "task2"},
			{"task": "task8"},
			{"name": "name10"},
		},
		WantResLength: 5,
	},
	{
		LabelsNames: []string{
			"task",
		},
		AddArgs: [][]string{
			{"task1"},
			{"task2"},
			{"task3"},
			{"task4"},
			{"task5"},
			{"task6"},
			{"task7"},
		},
		DeleteArgs: []map[string]string{
			{"task": "task2"},
			{"task": "task8"},
		},
		WantResLength: 6,
	},
	{
		LabelsNames: []string{
			"type",
			"task",
			"queueNo",
		},
		AddArgs: [][]string{
			{"flash", "task2", "No.2"},
			{"flash", "task3", "No.3"},
			{"flash", "task4", "No.4"},
			{"flash", "task5", "No.5"},
			{"flash", "task6", "No.6"},
		},
		DeleteArgs: []map[string]string{
			{"type": "flash"},
		},
		WantResLength: 0,
	},
	{
		LabelsNames: []string{
			"type",
			"task",
			"queueNo",
		},
		AddArgs: [][]string{
			{"flash", "task2", "No.2"},
			{"flash", "task2", "No.3"},
			{"flash", "task4", "No.4"},
			{"flash", "task5", "No.4"},
			{"start", "task6", "No.6"},
		},
		DeleteArgs: []map[string]string{
			{"type": "start"},
			{"type": "flash", "task": "task2", "queueNo": "No.3"},
			{"type": "start", "task": "task2", "queueNo": "No.4"},
		},
		WantResLength: 3,
	},
	{
		LabelsNames: []string{
			"type",
			"task",
			"queueNo",
		},
		AddArgs: [][]string{
			{"flash", "task2", "No.2"},
			{"flash", "task2", "No.3"},
			{"flash", "task4", "No.4"},
			{"flash", "task5", "No.4"},
			{"start", "task6", "No.6"},
		},
		DeleteArgs: []map[string]string{
			{},
		},
		WantResLength: 5,
	},
}
