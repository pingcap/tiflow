// Copyright 2023 PingCAP, Inc.
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

package column_selector

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

var event = &model.RowChangedEvent{
	Table: &model.TableName{
		Schema: "test",
		Table:  "table1",
	},
	Columns: []*model.Column{
		{
			Name:  "col1",
			Value: []byte("val1"),
		},
		{
			Name:  "col2",
			Value: []byte("val2"),
		},
		{
			Name:  "col3",
			Value: []byte("val3"),
		},
	},
	PreColumns: []*model.Column{
		{
			Name:  "col1",
			Value: []byte("val1"),
		},
		{
			Name:  "col2",
			Value: []byte("val2"),
		},
		{
			Name:  "col3",
			Value: []byte("val3"),
		},
	},
}

func TestNewColumnSelectorNoRules(t *testing.T) {
	// the column selector is not set
	replicaConfig := config.GetDefaultReplicaConfig()
	selectors, err := New(replicaConfig)
	require.NoError(t, err)
	require.NotNil(t, selectors)
	require.Len(t, selectors.selectors, 0)

	err = selectors.Apply(event)
	require.NoError(t, err)
	for _, column := range event.Columns {
		require.NotNil(t, column.Value)
	}
	for _, column := range event.PreColumns {
		require.NotNil(t, column.Value)
	}
}

func TestNewColumnSelector(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.ColumnSelectors = []*config.ColumnSelector{
		{
			Matcher: []string{"test.*"},
			Columns: []string{"col1", "col2"},
		},
		{
			Matcher: []string{"test1.*"},
			Columns: []string{"*", "!a"},
		},
		{
			Matcher: []string{"test2.*"},
			Columns: []string{"co*", "!col2"},
		},
		{
			Matcher: []string{"test3.*"},
			Columns: []string{"co?1"},
		},
	}
	selectors, err := New(replicaConfig)
	require.NoError(t, err)
	require.Len(t, selectors.selectors, 4)

	// column3 is filter out, set to nil.
	err = selectors.Apply(event)
	require.NoError(t, err)
	require.Equal(t, []byte("val1"), event.Columns[0].Value)
	require.Equal(t, []byte("val2"), event.Columns[1].Value)
	require.Nil(t, event.Columns[2])

	require.Equal(t, []byte("val1"), event.PreColumns[0].Value)
	require.Equal(t, []byte("val2"), event.PreColumns[1].Value)
	require.Nil(t, event.PreColumns[2])

	event = &model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test1",
			Table:  "table1",
		},
		Columns: []*model.Column{
			{
				Name:  "a",
				Value: []byte("a"),
			},
			{
				Name:  "b",
				Value: []byte("b"),
			},
			{
				Name:  "c",
				Value: []byte("c"),
			},
		},
	}
	// the first column `a` is filter out, set to nil.
	err = selectors.Apply(event)
	require.NoError(t, err)
	require.Nil(t, event.Columns[0])
	require.Equal(t, []byte("b"), event.Columns[1].Value)
	require.Equal(t, []byte("c"), event.Columns[2].Value)

	event = &model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test2",
			Table:  "table1",
		},
		Columns: []*model.Column{
			{
				Name:  "col",
				Value: []byte("col"),
			},
			{
				Name:  "col1",
				Value: []byte("col1"),
			},
			{
				Name:  "col2",
				Value: []byte("col2"),
			},
			{
				Name:  "col3",
				Value: []byte("col3"),
			},
		},
	}
	err = selectors.Apply(event)
	require.NoError(t, err)
	require.Equal(t, []byte("col"), event.Columns[0].Value)
	require.Equal(t, []byte("col1"), event.Columns[1].Value)
	require.Nil(t, event.Columns[2])
	require.Equal(t, []byte("col3"), event.Columns[3].Value)

	event = &model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test3",
			Table:  "table1",
		},
		Columns: []*model.Column{
			{
				Name:  "col",
				Value: []byte("col"),
			},
			{
				Name:  "col1",
				Value: []byte("col1"),
			},
			{
				Name:  "col2",
				Value: []byte("col2"),
			},
			{
				Name:  "coA1",
				Value: []byte("coA1"),
			},
		},
	}
	err = selectors.Apply(event)
	require.NoError(t, err)
	require.Nil(t, event.Columns[0])
	require.Equal(t, []byte("col1"), event.Columns[1].Value)
	require.Nil(t, event.Columns[2])
	require.Equal(t, []byte("coA1"), event.Columns[3].Value)
}
