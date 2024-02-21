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

//go:build intest
// +build intest

package columnselector

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dispatcher"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestNewColumnSelector(t *testing.T) {
	// the column selector is not set
	replicaConfig := config.GetDefaultReplicaConfig()
	selectors, err := New(replicaConfig)
	require.NoError(t, err)
	require.NotNil(t, selectors)
	require.Len(t, selectors.selectors, 0)

	event := &model.RowChangedEvent{
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema: "test",
				Table:  "table1",
			},
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
		},
	}

	for _, column := range event.Columns {
		require.NotNil(t, column)
	}
	for _, column := range event.PreColumns {
		require.NotNil(t, column)
	}

	replicaConfig.Sink.ColumnSelectors = []*config.ColumnSelector{
		{
			Matcher: []string{"test.*"},
			Columns: []string{"a", "b"},
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
	selectors, err = New(replicaConfig)
	require.NoError(t, err)
	require.Len(t, selectors.selectors, 4)
}

func TestVerifyTables(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(
    	a int primary key,
    	b int,
    	c int,
    	d int,
    	e int,
    	unique key uk_b_c(b, c),
    	unique key uk_d_e(d, e),
    	key idx_c_d(c, d))`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 0, job.BinlogInfo.TableInfo)
	infos := []*model.TableInfo{tableInfo}

	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.ColumnSelectors = []*config.ColumnSelector{
		{
			Matcher: []string{"test.t1"},
			Columns: []string{"a", "b"},
		},
	}
	selectors, err := New(replicaConfig)
	require.NoError(t, err)

	eventRouter, err := dispatcher.NewEventRouter(replicaConfig, config.ProtocolDefault, "default", "default")
	require.NoError(t, err)
	// handle key column `a` is retained, so return no error
	err = selectors.VerifyTables(infos, eventRouter)
	require.NoError(t, err)

	event4Test := func() *model.RowChangedEvent {
		return &model.RowChangedEvent{
			TableInfo: tableInfo,
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: []byte("1"),
				},
				{
					Name:  "b",
					Value: []byte("2"),
				},
				{
					Name:  "c",
					Value: []byte("3"),
				},
				{
					Name:  "d",
					Value: []byte("4"),
				},
				{
					Name:  "e",
					Value: []byte("5"),
				},
			},
			PreColumns: []*model.Column{
				{
					Name:  "a",
					Value: []byte("1"),
				},
				{
					Name:  "b",
					Value: []byte("2"),
				},
				{
					Name:  "c",
					Value: []byte("3"),
				},
				{
					Name:  "d",
					Value: []byte("4"),
				},
				{
					Name:  "e",
					Value: []byte("5"),
				},
			},
		}
	}

	event := event4Test()
	err = selectors.Apply(event)
	require.NoError(t, err)
	require.Nil(t, event.Columns[2])
	require.Nil(t, event.Columns[3])
	require.Nil(t, event.Columns[4])
	require.Nil(t, event.PreColumns[2])
	require.Nil(t, event.PreColumns[3])
	require.Nil(t, event.PreColumns[4])

	event = event4Test()
	event.PreColumns = nil
	err = selectors.Apply(event)
	require.NoError(t, err)

	event = event4Test()
	event.Columns = nil
	err = selectors.Apply(event)
	require.NoError(t, err)

	replicaConfig.Sink.ColumnSelectors = []*config.ColumnSelector{
		{
			Matcher: []string{"test.t1"},
			Columns: []string{"b", "c"},
		},
	}
	selectors, err = New(replicaConfig)
	require.NoError(t, err)

	// handle key column `a` is filtered out, but unique key `uk_b_c` has all columns retained, so return no error.
	err = selectors.VerifyTables(infos, eventRouter)
	require.NoError(t, err)

	event = event4Test()
	err = selectors.Apply(event)
	require.NoError(t, err)
	require.Nil(t, event.Columns[0])
	require.Nil(t, event.Columns[3])
	require.Nil(t, event.Columns[4])
	require.Nil(t, event.PreColumns[0])
	require.Nil(t, event.PreColumns[3])
	require.Nil(t, event.PreColumns[4])

	replicaConfig.Sink.ColumnSelectors = []*config.ColumnSelector{
		{
			Matcher: []string{"test.*"},
			Columns: []string{"c", "d"},
		},
		{
			Matcher: []string{"test.t1"},
			Columns: []string{"a", "b"},
		},
	}
	selectors, err = New(replicaConfig)
	require.NoError(t, err)

	// handle key column `a` is filtered out, no one unique key has all columns retained, so return error.
	err = selectors.VerifyTables(infos, eventRouter)
	require.ErrorIs(t, err, errors.ErrColumnSelectorFailed)

	event = event4Test()
	err = selectors.Apply(event)
	require.ErrorIs(t, err, errors.ErrColumnSelectorFailed)
}

func TestVerifyTablesColumnFilteredInDispatcher(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.ColumnSelectors = []*config.ColumnSelector{
		{
			Matcher: []string{"test.*"},
			Columns: []string{"a", "b"},
		},
	}
	replicaConfig.Sink.DispatchRules = []*config.DispatchRule{
		{
			Matcher:       []string{"test.*"},
			PartitionRule: "columns",
			Columns:       []string{"c"},
		},
	}

	selectors, err := New(replicaConfig)
	require.NoError(t, err)

	eventRouter, err := dispatcher.NewEventRouter(replicaConfig, config.ProtocolDefault, "default", "default")
	require.NoError(t, err)

	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(a int primary key, b int, c int)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 0, job.BinlogInfo.TableInfo)
	infos := []*model.TableInfo{tableInfo}

	err = selectors.VerifyTables(infos, eventRouter)
	require.ErrorIs(t, err, errors.ErrColumnSelectorFailed)
}
