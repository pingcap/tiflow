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

package dispatcher

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dispatcher/partition"
	"github.com/pingcap/tiflow/cdc/sink/dispatcher/topic"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestEventRouter(t *testing.T) {
	t.Parallel()

	d, err := NewEventRouter(config.GetDefaultReplicaConfig(), "")
	require.Nil(t, err)
	topicDispatcher, partitionDispatcher := d.matchDispatcher("test", "test")
	require.IsType(t, &topic.StaticTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.DefaultDispatcher{}, partitionDispatcher)

	d, err = NewEventRouter(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{Matcher: []string{"test_default1.*"}, PartitionRule: "default"},
				{Matcher: []string{"test_default2.*"}, PartitionRule: "unknown-dispatcher"},
				{Matcher: []string{"test_table.*"}, PartitionRule: "table", TopicRule: "hello_{schema}_world"},
				{Matcher: []string{"test_index_value.*"}, PartitionRule: "index-value", TopicRule: "{schema}_world"},
				{Matcher: []string{"test.*"}, PartitionRule: "rowid", TopicRule: "hello_{schema}"},
				{Matcher: []string{"*.*", "!*.test"}, PartitionRule: "ts", TopicRule: "{schema}_{table}"},
			},
		},
	}, "")
	require.Nil(t, err)
	topicDispatcher, partitionDispatcher = d.matchDispatcher("test", "table1")
	require.IsType(t, &topic.DynamicTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.IndexValueDispatcher{}, partitionDispatcher)

	topicDispatcher, partitionDispatcher = d.matchDispatcher("sbs", "table2")
	require.IsType(t, &topic.DynamicTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.TsDispatcher{}, partitionDispatcher)

	topicDispatcher, partitionDispatcher = d.matchDispatcher("sbs", "test")
	require.IsType(t, &topic.StaticTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.DefaultDispatcher{}, partitionDispatcher)

	topicDispatcher, partitionDispatcher = d.matchDispatcher("test_default1", "test")
	require.IsType(t, &topic.StaticTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.DefaultDispatcher{}, partitionDispatcher)

	topicDispatcher, partitionDispatcher = d.matchDispatcher("test_default2", "test")
	require.IsType(t, &topic.StaticTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.DefaultDispatcher{}, partitionDispatcher)

	topicDispatcher, partitionDispatcher = d.matchDispatcher("test_table", "test")
	require.IsType(t, &topic.DynamicTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.TableDispatcher{}, partitionDispatcher)

	topicDispatcher, partitionDispatcher = d.matchDispatcher("test_index_value", "test")
	require.IsType(t, &topic.DynamicTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.IndexValueDispatcher{}, partitionDispatcher)
}

func TestGetActiveTopics(t *testing.T) {
	t.Parallel()

	d, err := NewEventRouter(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{Matcher: []string{"test_default1.*"}, PartitionRule: "default"},
				{Matcher: []string{"test_default2.*"}, PartitionRule: "unknown-dispatcher"},
				{Matcher: []string{"test_table.*"}, PartitionRule: "table", TopicRule: "hello_{schema}_world"},
				{Matcher: []string{"test_index_value.*"}, PartitionRule: "index-value", TopicRule: "{schema}_world"},
				{Matcher: []string{"test.*"}, PartitionRule: "rowid", TopicRule: "hello_{schema}"},
				{Matcher: []string{"*.*", "!*.test"}, PartitionRule: "ts", TopicRule: "{schema}_{table}"},
			},
		},
	}, "test")
	require.Nil(t, err)
	names := []model.TableName{
		{Schema: "test_default1", Table: "table"},
		{Schema: "test_default2", Table: "table"},
		{Schema: "test_table", Table: "table"},
		{Schema: "test_index_value", Table: "table"},
		{Schema: "test", Table: "table"},
		{Schema: "sbs", Table: "table"},
	}
	topics := d.GetActiveTopics(names)
	require.Equal(t, []string{"test", "hello_test_table_world", "test_index_value_world", "hello_test", "sbs_table"}, topics)
}
