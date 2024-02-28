// Copyright 2022 PingCAP, Inc.
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

package topic

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStaticTopicDispatcher(t *testing.T) {
	p := NewStaticTopicDispatcher("cdctest")
	require.Equal(t, p.Substitute("db1", "tbl1"), "cdctest")
}

func TestDynamicTopicDispatcherForSchema(t *testing.T) {
	t.Parallel()

	topicExpr := Expression("hello_{schema}_world")
	err := topicExpr.Validate()
	require.NoError(t, err)

	testCase := []struct {
		schema      string
		table       string
		expectTopic string
	}{
		{
			schema:      "test1",
			table:       "tb1",
			expectTopic: "hello_test1_world",
		},
		{
			schema:      "test1",
			table:       "tb2",
			expectTopic: "hello_test1_world",
		},
		{
			schema:      "test2",
			table:       "tb1",
			expectTopic: "hello_test2_world",
		},
		{
			schema:      "test2",
			table:       "tb2",
			expectTopic: "hello_test2_world",
		},
	}

	p := NewDynamicTopicDispatcher(topicExpr)
	for _, tc := range testCase {
		require.Equal(t, tc.expectTopic, p.Substitute(tc.schema, tc.table))
	}
}

func TestDynamicTopicDispatcherForTable(t *testing.T) {
	t.Parallel()

	topicExpr := Expression("{schema}_{table}")
	err := topicExpr.Validate()
	require.NoError(t, err)

	testCases := []struct {
		schema        string
		table         string
		expectedTopic string
	}{
		{
			schema:        "test1",
			table:         "tb1",
			expectedTopic: "test1_tb1",
		},
		{
			schema:        "test1",
			table:         "tb2",
			expectedTopic: "test1_tb2",
		},
		{
			schema:        "test2",
			table:         "tb1",
			expectedTopic: "test2_tb1",
		},
		{
			schema:        "test2",
			table:         "tb2",
			expectedTopic: "test2_tb2",
		},
	}
	p := NewDynamicTopicDispatcher(topicExpr)
	for _, tc := range testCases {
		require.Equal(t, tc.expectedTopic, p.Substitute(tc.schema, tc.table))
	}
}
