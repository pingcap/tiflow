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

	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dispatcher/partition"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dispatcher/topic"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/stretchr/testify/require"
)

func newReplicaConfig4DispatcherTest() *config.ReplicaConfig {
	return &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				// rule-0
				{
					Matcher:       []string{"test_default1.*"},
					PartitionRule: "default",
				},
				// rule-1
				{
					Matcher:       []string{"test_default2.*"},
					PartitionRule: "unknown-dispatcher",
				},
				// rule-2
				{
					Matcher:       []string{"test_table.*"},
					PartitionRule: "table",
					TopicRule:     "hello_{schema}_world",
				},
				// rule-3
				{
					Matcher:       []string{"test_index_value.*"},
					PartitionRule: "index-value",
					TopicRule:     "{schema}_world",
				},
				// rule-4
				{
					Matcher:       []string{"test.*"},
					PartitionRule: "rowid",
					TopicRule:     "hello_{schema}",
				},
				// rule-5
				{
					Matcher:       []string{"*.*", "!*.test"},
					PartitionRule: "ts",
					TopicRule:     "{schema}_{table}",
				},
				// rule-6: hard code the topic
				{
					Matcher:       []string{"hard_code_schema.*"},
					PartitionRule: "default",
					TopicRule:     "hard_code_topic",
				},
			},
		},
	}
}

func TestEventRouter(t *testing.T) {
	t.Parallel()

	replicaConfig := config.GetDefaultReplicaConfig()
	d, err := NewEventRouter(replicaConfig, config.ProtocolCanalJSON, "test", sink.KafkaScheme)
	require.NoError(t, err)
	require.Equal(t, "test", d.GetDefaultTopic())

	topicDispatcher, partitionDispatcher := d.matchDispatcher("test", "test")
	require.IsType(t, &topic.StaticTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.DefaultDispatcher{}, partitionDispatcher)

	actual := topicDispatcher.Substitute("test", "test")
	require.Equal(t, d.defaultTopic, actual)

	replicaConfig = newReplicaConfig4DispatcherTest()
	d, err = NewEventRouter(replicaConfig, config.ProtocolCanalJSON, "", sink.KafkaScheme)
	require.NoError(t, err)

	// no matched, use the default
	topicDispatcher, partitionDispatcher = d.matchDispatcher("sbs", "test")
	require.IsType(t, &topic.StaticTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.DefaultDispatcher{}, partitionDispatcher)

	// match rule-0
	topicDispatcher, partitionDispatcher = d.matchDispatcher("test_default1", "test")
	require.IsType(t, &topic.StaticTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.DefaultDispatcher{}, partitionDispatcher)

	// match rule-1
	topicDispatcher, partitionDispatcher = d.matchDispatcher("test_default2", "test")
	require.IsType(t, &topic.StaticTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.DefaultDispatcher{}, partitionDispatcher)

	// match rule-2
	topicDispatcher, partitionDispatcher = d.matchDispatcher("test_table", "test")
	require.IsType(t, &topic.DynamicTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.TableDispatcher{}, partitionDispatcher)

	// match rule-4
	topicDispatcher, partitionDispatcher = d.matchDispatcher("test_index_value", "test")
	require.IsType(t, &topic.DynamicTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.IndexValueDispatcher{}, partitionDispatcher)

	// match rule-4
	topicDispatcher, partitionDispatcher = d.matchDispatcher("test", "table1")
	require.IsType(t, &topic.DynamicTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.IndexValueDispatcher{}, partitionDispatcher)

	// match rule-5
	topicDispatcher, partitionDispatcher = d.matchDispatcher("sbs", "table2")
	require.IsType(t, &topic.DynamicTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.TsDispatcher{}, partitionDispatcher)

	// match rule-6
	topicDispatcher, partitionDispatcher = d.matchDispatcher("hard_code_schema", "test")
	require.IsType(t, &topic.StaticTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.DefaultDispatcher{}, partitionDispatcher)
}

func TestGetActiveTopics(t *testing.T) {
	t.Parallel()

	replicaConfig := newReplicaConfig4DispatcherTest()
	d, err := NewEventRouter(replicaConfig, config.ProtocolCanalJSON, "test", sink.KafkaScheme)
	require.NoError(t, err)
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

func TestGetTopicForRowChange(t *testing.T) {
	t.Parallel()

	replicaConfig := newReplicaConfig4DispatcherTest()
	d, err := NewEventRouter(replicaConfig, config.ProtocolCanalJSON, "test", "kafka")
	require.NoError(t, err)

	topicName := d.GetTopicForRowChange(&model.RowChangedEvent{
		Table: &model.TableName{Schema: "test_default1", Table: "table"},
	})
	require.Equal(t, "test", topicName)

	topicName = d.GetTopicForRowChange(&model.RowChangedEvent{
		Table: &model.TableName{Schema: "test_default2", Table: "table"},
	})
	require.Equal(t, "test", topicName)

	topicName = d.GetTopicForRowChange(&model.RowChangedEvent{
		Table: &model.TableName{Schema: "test_table", Table: "table"},
	})
	require.Equal(t, "hello_test_table_world", topicName)

	topicName = d.GetTopicForRowChange(&model.RowChangedEvent{
		Table: &model.TableName{Schema: "test_index_value", Table: "table"},
	})
	require.Equal(t, "test_index_value_world", topicName)

	topicName = d.GetTopicForRowChange(&model.RowChangedEvent{
		Table: &model.TableName{Schema: "a", Table: "table"},
	})
	require.Equal(t, "a_table", topicName)
}

func TestGetPartitionForRowChange(t *testing.T) {
	t.Parallel()

	replicaConfig := newReplicaConfig4DispatcherTest()
	d, err := NewEventRouter(replicaConfig, config.ProtocolCanalJSON, "test", sink.KafkaScheme)
	require.NoError(t, err)

	p, _, err := d.GetPartitionForRowChange(&model.RowChangedEvent{
		Table: &model.TableName{Schema: "test_default1", Table: "table"},
		Columns: []*model.Column{
			{
				Name:  "id",
				Value: 1,
				Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
			},
		},
		IndexColumns: [][]int{{0}},
	}, 16)
	require.NoError(t, err)
	require.Equal(t, int32(14), p)

	p, _, err = d.GetPartitionForRowChange(&model.RowChangedEvent{
		Table: &model.TableName{Schema: "test_default2", Table: "table"},
		Columns: []*model.Column{
			{
				Name:  "id",
				Value: 1,
				Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
			},
		},
		IndexColumns: [][]int{{0}},
	}, 16)
	require.NoError(t, err)
	require.Equal(t, int32(0), p)

	p, _, err = d.GetPartitionForRowChange(&model.RowChangedEvent{
		Table:    &model.TableName{Schema: "test_table", Table: "table"},
		CommitTs: 1,
	}, 16)
	require.NoError(t, err)
	require.Equal(t, int32(15), p)

	p, _, err = d.GetPartitionForRowChange(&model.RowChangedEvent{
		Table: &model.TableName{Schema: "test_index_value", Table: "table"},
		Columns: []*model.Column{
			{
				Name:  "a",
				Value: 11,
				Flag:  model.HandleKeyFlag,
			}, {
				Name:  "b",
				Value: 22,
				Flag:  0,
			},
		},
	}, 10)
	require.NoError(t, err)
	require.Equal(t, int32(1), p)

	p, _, err = d.GetPartitionForRowChange(&model.RowChangedEvent{
		Table:    &model.TableName{Schema: "a", Table: "table"},
		CommitTs: 1,
	}, 2)
	require.NoError(t, err)
	require.Equal(t, int32(1), p)
}

func TestGetTopicForDDL(t *testing.T) {
	t.Parallel()

	replicaConfig := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:       []string{"test.*"},
					PartitionRule: "rowid",
					TopicRule:     "hello_{schema}",
				},
				{
					Matcher:       []string{"*.*", "!*.test"},
					PartitionRule: "ts",
					TopicRule:     "{schema}_{table}",
				},
			},
		},
	}

	d, err := NewEventRouter(replicaConfig, config.ProtocolDefault, "test", "kafka")
	require.NoError(t, err)

	tests := []struct {
		ddl           *model.DDLEvent
		expectedTopic string
	}{
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: "test",
					},
				},
				Type: timodel.ActionCreateSchema,
			},
			expectedTopic: "test",
		},
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: "test",
					},
				},
				Type: timodel.ActionDropSchema,
			},
			expectedTopic: "test",
		},
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: "test",
						Table:  "tb1",
					},
				},
				Type: timodel.ActionCreateTable,
			},
			expectedTopic: "hello_test",
		},
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: "test",
						Table:  "tb1",
					},
				},
				Type: timodel.ActionDropTable,
			},
			expectedTopic: "hello_test",
		},
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: "test1",
						Table:  "view1",
					},
				},
				Type: timodel.ActionDropView,
			},
			expectedTopic: "test1_view1",
		},
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: "test1",
						Table:  "tb1",
					},
				},
				Type: timodel.ActionAddColumn,
			},
			expectedTopic: "test1_tb1",
		},
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: "test1",
						Table:  "tb1",
					},
				},
				Type: timodel.ActionDropColumn,
			},
			expectedTopic: "test1_tb1",
		},
		{
			ddl: &model.DDLEvent{
				PreTableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: "test1",
						Table:  "tb1",
					},
				},
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: "test1",
						Table:  "tb2",
					},
				},
				Type: timodel.ActionRenameTable,
			},
			expectedTopic: "test1_tb1",
		},
	}

	for _, test := range tests {
		require.Equal(t, test.expectedTopic, d.GetTopicForDDL(test.ddl))
	}
}
