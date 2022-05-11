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

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/mq/dispatcher/partition"
	"github.com/pingcap/tiflow/cdc/sink/mq/dispatcher/topic"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestEventRouter(t *testing.T) {
	t.Parallel()

	d, err := NewEventRouter(config.GetDefaultReplicaConfig(), "test")
	require.Nil(t, err)
	require.Equal(t, "test", d.GetDefaultTopic())
	topicDispatcher, partitionDispatcher := d.matchDispatcher("test", "test")
	require.IsType(t, &topic.StaticTopicDispatcher{}, topicDispatcher)
	require.IsType(t, &partition.DefaultDispatcher{}, partitionDispatcher)

	d, err = NewEventRouter(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:       []string{"test_default1.*"},
					PartitionRule: "default",
				},
				{
					Matcher:       []string{"test_default2.*"},
					PartitionRule: "unknown-dispatcher",
				},
				{
					Matcher:       []string{"test_table.*"},
					PartitionRule: "table",
					TopicRule:     "hello_{schema}_world",
				},
				{
					Matcher:       []string{"test_index_value.*"},
					PartitionRule: "index-value",
					TopicRule:     "{schema}_world",
				},
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
				{
					Matcher:       []string{"test_default1.*"},
					PartitionRule: "default",
				},
				{
					Matcher:       []string{"test_default2.*"},
					PartitionRule: "unknown-dispatcher",
				},
				{
					Matcher:       []string{"test_table.*"},
					PartitionRule: "table",
					TopicRule:     "hello_{schema}_world",
				},
				{
					Matcher:       []string{"test_index_value.*"},
					PartitionRule: "index-value",
					TopicRule:     "{schema}_world",
				},
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

func TestGetTopicForRowChange(t *testing.T) {
	t.Parallel()

	d, err := NewEventRouter(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:       []string{"test_default1.*"},
					PartitionRule: "default",
				},
				{
					Matcher:       []string{"test_default2.*"},
					PartitionRule: "unknown-dispatcher",
				},
				{
					Matcher:       []string{"test_table.*"},
					PartitionRule: "table",
					TopicRule:     "hello_{schema}_world",
				},
				{
					Matcher:       []string{"test_index_value.*"},
					PartitionRule: "index-value",
					TopicRule:     "{schema}_world",
				},
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
	}, "test")
	require.Nil(t, err)

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

	d, err := NewEventRouter(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:       []string{"test_default1.*"},
					PartitionRule: "default",
				},
				{
					Matcher:       []string{"test_default2.*"},
					PartitionRule: "unknown-dispatcher",
				},
				{
					Matcher:       []string{"test_table.*"},
					PartitionRule: "table",
					TopicRule:     "hello_{schema}_world",
				},
				{
					Matcher:       []string{"test_index_value.*"},
					PartitionRule: "index-value",
					TopicRule:     "{schema}_world",
				},
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
	}, "test")
	require.Nil(t, err)

	p := d.GetPartitionForRowChange(&model.RowChangedEvent{
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
	require.Equal(t, int32(10), p)
	p = d.GetPartitionForRowChange(&model.RowChangedEvent{
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
	require.Equal(t, int32(4), p)

	p = d.GetPartitionForRowChange(&model.RowChangedEvent{
		Table:    &model.TableName{Schema: "test_table", Table: "table"},
		CommitTs: 1,
	}, 16)
	require.Equal(t, int32(15), p)
	p = d.GetPartitionForRowChange(&model.RowChangedEvent{
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
	require.Equal(t, int32(1), p)
	p = d.GetPartitionForRowChange(&model.RowChangedEvent{
		Table:    &model.TableName{Schema: "a", Table: "table"},
		CommitTs: 1,
	}, 2)
	require.Equal(t, int32(1), p)
}

func TestGetDLLDispatchRuleByProtocol(t *testing.T) {
	t.Parallel()

	d, err := NewEventRouter(&config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:       []string{"test_table.*"},
					PartitionRule: "table",
					TopicRule:     "hello_{schema}_world",
				},
			},
		},
	}, "test")
	require.Nil(t, err)

	tests := []struct {
		protocol     config.Protocol
		expectedRule DDLDispatchRule
	}{
		{
			protocol:     config.ProtocolDefault,
			expectedRule: PartitionAll,
		},
		{
			protocol:     config.ProtocolCanal,
			expectedRule: PartitionZero,
		},
		{
			protocol:     config.ProtocolAvro,
			expectedRule: PartitionAll,
		},
		{
			protocol:     config.ProtocolMaxwell,
			expectedRule: PartitionAll,
		},
		{
			protocol:     config.ProtocolCanalJSON,
			expectedRule: PartitionZero,
		},
		{
			protocol:     config.ProtocolCraft,
			expectedRule: PartitionAll,
		},
		{
			protocol:     config.ProtocolOpen,
			expectedRule: PartitionAll,
		},
	}

	for _, test := range tests {
		rule := d.GetDLLDispatchRuleByProtocol(test.protocol)
		require.Equal(t, test.expectedRule, rule)
	}
}

func TestGetTopicForDDL(t *testing.T) {
	t.Parallel()

	d, err := NewEventRouter(&config.ReplicaConfig{
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
	}, "test")
	require.Nil(t, err)

	tests := []struct {
		ddl           *model.DDLEvent
		expectedTopic string
	}{
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.SimpleTableInfo{
					Schema: "test",
				},
				Type: timodel.ActionCreateSchema,
			},
			expectedTopic: "test",
		},
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.SimpleTableInfo{
					Schema: "test",
				},
				Type: timodel.ActionDropSchema,
			},
			expectedTopic: "test",
		},
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.SimpleTableInfo{
					Schema: "test",
					Table:  "tb1",
				},
				Type: timodel.ActionCreateTable,
			},
			expectedTopic: "hello_test",
		},
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.SimpleTableInfo{
					Schema: "test",
					Table:  "tb1",
				},
				Type: timodel.ActionDropTable,
			},
			expectedTopic: "hello_test",
		},
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.SimpleTableInfo{
					Schema: "test1",
					Table:  "view1",
				},
				Type: timodel.ActionDropView,
			},
			expectedTopic: "test1_view1",
		},
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.SimpleTableInfo{
					Schema: "test1",
					Table:  "tb1",
				},
				Type: timodel.ActionAddColumn,
			},
			expectedTopic: "test1_tb1",
		},
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.SimpleTableInfo{
					Schema: "test1",
					Table:  "tb1",
				},
				Type: timodel.ActionDropColumn,
			},
			expectedTopic: "test1_tb1",
		},
		{
			ddl: &model.DDLEvent{
				PreTableInfo: &model.SimpleTableInfo{
					Schema: "test1",
					Table:  "tb1",
				},
				TableInfo: &model.SimpleTableInfo{
					Schema: "test1",
					Table:  "tb2",
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
