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

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestStaticTopicDispatcher(t *testing.T) {
	row := &model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "db1",
			Table:  "tbl1",
		},
	}

	ddl := &model.DDLEvent{
		TableInfo: &model.SimpleTableInfo{
			Schema: "db1",
			Table:  "tbl1",
		},
	}

	p := NewStaticTopicDispatcher("cdctest")
	require.Equal(t, p.DispatchRowChangedEvent(row), "cdctest")
	require.Equal(t, p.DispatchDDLEvent(ddl), "cdctest")
}

func TestDynamicTopicDispatcherForSchema(t *testing.T) {
	t.Parallel()

	topicExpr := Expression("hello_{schema}_world")
	err := topicExpr.Validate()
	require.Nil(t, err)
	testCase := []struct {
		row         *model.RowChangedEvent
		expectTopic string
	}{
		{
			row: &model.RowChangedEvent{
				Table: &model.TableName{
					Schema: "test1",
					Table:  "tb1",
				},
			},
			expectTopic: "hello_test1_world",
		},
		{
			row: &model.RowChangedEvent{
				Table: &model.TableName{
					Schema: "test1",
					Table:  "tb2",
				},
			},
			expectTopic: "hello_test1_world",
		},
		{
			row: &model.RowChangedEvent{
				Table: &model.TableName{
					Schema: "test2",
					Table:  "tb1",
				},
			},
			expectTopic: "hello_test2_world",
		},
		{
			row: &model.RowChangedEvent{
				Table: &model.TableName{
					Schema: "test2",
					Table:  "tb2",
				},
			},
			expectTopic: "hello_test2_world",
		},
	}

	p := NewDynamicTopicDispatcher("cdctest", topicExpr)
	for _, tc := range testCase {
		require.Equal(t, tc.expectTopic, p.DispatchRowChangedEvent(tc.row))
	}
}

func TestDynamicTopicDispatcherForTable(t *testing.T) {
	t.Parallel()

	topicExpr := Expression("{schema}_{table}")
	err := topicExpr.Validate()
	require.Nil(t, err)
	testCases := []struct {
		row           *model.RowChangedEvent
		expectedTopic string
	}{
		{
			row: &model.RowChangedEvent{
				Table: &model.TableName{
					Schema: "test1",
					Table:  "tb1",
				},
			},
			expectedTopic: "test1_tb1",
		},
		{
			row: &model.RowChangedEvent{
				Table: &model.TableName{
					Schema: "test1",
					Table:  "tb2",
				},
			},
			expectedTopic: "test1_tb2",
		},
		{
			row: &model.RowChangedEvent{
				Table: &model.TableName{
					Schema: "test2",
					Table:  "tb1",
				},
			},
			expectedTopic: "test2_tb1",
		},
		{
			row: &model.RowChangedEvent{
				Table: &model.TableName{
					Schema: "test2",
					Table:  "tb2",
				},
			},
			expectedTopic: "test2_tb2",
		},
	}
	p := NewDynamicTopicDispatcher("cdctest", topicExpr)
	for _, tc := range testCases {
		require.Equal(t, tc.expectedTopic, p.DispatchRowChangedEvent(tc.row))
	}
}

func TestDynamicTopicDispatcherForDDL(t *testing.T) {
	t.Parallel()

	defaultTopic := "cdctest"
	topicExpr := Expression("{schema}_{table}")
	testCases := []struct {
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
			expectedTopic: defaultTopic,
		},
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.SimpleTableInfo{
					Schema: "test",
				},
				Type: timodel.ActionDropSchema,
			},
			expectedTopic: defaultTopic,
		},
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.SimpleTableInfo{
					Schema: "cdc",
					Table:  "person",
				},
				Query: "CREATE TABLE person(id int, name varchar(32), primary key(id))",
				Type:  timodel.ActionCreateTable,
			},
			expectedTopic: "cdc_person",
		},
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.SimpleTableInfo{
					Schema: "cdc",
					Table:  "person",
				},
				Query: "DROP TABLE person",
				Type:  timodel.ActionDropTable,
			},
			expectedTopic: "cdc_person",
		},
		{
			ddl: &model.DDLEvent{
				TableInfo: &model.SimpleTableInfo{
					Schema: "cdc",
					Table:  "person",
				},
				Query: "ALTER TABLE cdc.person ADD COLUMN age int",
				Type:  timodel.ActionAddColumn,
			},
			expectedTopic: "cdc_person",
		},
	}

	p := NewDynamicTopicDispatcher(defaultTopic, topicExpr)
	for _, tc := range testCases {
		require.Equal(t, tc.expectedTopic, p.DispatchDDLEvent(tc.ddl))
	}
}
