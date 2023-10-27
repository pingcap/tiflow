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

package transformer

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

var (
	eventTable1 = &model.RowChangedEvent{
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
)

func TestNewColumnSelectorNoRules(t *testing.T) {
	// the column selector rule is not set
	replicaConfig := config.GetDefaultReplicaConfig()
	selectors, err := NewColumnSelector(replicaConfig)
	require.NoError(t, err)
	require.NotNil(t, selectors)
	require.Len(t, selectors, 0)

	err = selectors.Transform(eventTable1)
	require.NoError(t, err)
	for _, column := range eventTable1.Columns {
		require.NotNil(t, column.Value)
	}
	for _, column := range eventTable1.PreColumns {
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
			Columns: []string{"co*", "!col1"},
		},
		{
			Matcher: []string{"test3.*"},
			Columns: []string{"co?1"},
		},
	}
	selectors, err := NewColumnSelector(replicaConfig)
	require.NoError(t, err)
	require.Len(t, selectors, 4)

	err = selectors.Transform(eventTable1)
	require.NoError(t, err)
	require.Equal(t, []byte("val1"), eventTable1.Columns[0].Value)
	require.Equal(t, []byte("val2"), eventTable1.Columns[1].Value)
	require.Nil(t, eventTable1.Columns[3].Value)

	require.Equal(t, []byte("val1"), eventTable1.PreColumns[0].Value)
	require.Equal(t, []byte("val2"), eventTable1.PreColumns[1].Value)
	require.Nil(t, eventTable1.PreColumns[3].Value)

}
