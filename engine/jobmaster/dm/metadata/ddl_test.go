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

package metadata

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pingcap/tiflow/engine/pkg/adapter"
	"github.com/pingcap/tiflow/engine/pkg/meta/mock"
	"github.com/stretchr/testify/require"
)

func TestMarshal(t *testing.T) {
	t.Parallel()

	st := SourceTable{Source: "source", Schema: "schema", Table: "table"}
	bytes, err := json.Marshal(st)
	require.NoError(t, err)
	var st2 SourceTable
	require.NoError(t, json.Unmarshal(bytes, &st2))
	require.Equal(t, st, st2)

	tt := TargetTable{Schema: "schema", Table: "table"}
	bytes, err = json.Marshal(tt)
	require.NoError(t, err)
	var tt2 TargetTable
	require.NoError(t, json.Unmarshal(bytes, &tt2))
	require.Equal(t, tt, tt2)
}

func TestDroppedColumnsStore(t *testing.T) {
	t.Parallel()

	var (
		targetTable         = TargetTable{Schema: "schema", Table: "table"}
		sourceTable1        = SourceTable{Source: "source", Schema: "schema", Table: "table1"}
		sourceTable2        = SourceTable{Source: "source", Schema: "schema", Table: "table2"}
		droppedColumnsStore = NewDroppedColumnsStore(mock.NewMetaMock(), targetTable)
	)
	key := droppedColumnsStore.key()
	keys, err := adapter.DMDroppedColumnsKeyAdapter.Decode(key)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	var tt TargetTable
	require.NoError(t, json.Unmarshal([]byte(keys[0]), &tt))
	require.Equal(t, tt, targetTable)

	state := droppedColumnsStore.createState()
	require.IsType(t, &DroppedColumns{}, state)

	require.False(t, droppedColumnsStore.HasDroppedColumn(context.Background(), "col1", sourceTable1))
	require.NoError(t, droppedColumnsStore.DelDroppedColumn(context.Background(), "col"))
	require.NoError(t, droppedColumnsStore.DelDroppedColumnForTable(context.Background(), sourceTable1))

	require.NoError(t, droppedColumnsStore.AddDroppedColumns(context.Background(), []string{"col1"}, sourceTable1))
	require.True(t, droppedColumnsStore.HasDroppedColumn(context.Background(), "col1", sourceTable1))
	require.False(t, droppedColumnsStore.HasDroppedColumn(context.Background(), "col2", sourceTable1))
	require.False(t, droppedColumnsStore.HasDroppedColumn(context.Background(), "col1", sourceTable2))

	require.NoError(t, droppedColumnsStore.AddDroppedColumns(context.Background(), []string{"col1", "col2"}, sourceTable2))
	require.True(t, droppedColumnsStore.HasDroppedColumn(context.Background(), "col1", sourceTable1))
	require.False(t, droppedColumnsStore.HasDroppedColumn(context.Background(), "col2", sourceTable1))
	require.True(t, droppedColumnsStore.HasDroppedColumn(context.Background(), "col1", sourceTable2))
	require.True(t, droppedColumnsStore.HasDroppedColumn(context.Background(), "col2", sourceTable2))

	require.NoError(t, droppedColumnsStore.DelDroppedColumn(context.Background(), "col1"))
	require.False(t, droppedColumnsStore.HasDroppedColumn(context.Background(), "col1", sourceTable2))
	require.True(t, droppedColumnsStore.HasDroppedColumn(context.Background(), "col2", sourceTable2))
	require.NoError(t, droppedColumnsStore.DelDroppedColumn(context.Background(), "col2"))
	require.False(t, droppedColumnsStore.HasDroppedColumn(context.Background(), "col2", sourceTable2))

	require.NoError(t, droppedColumnsStore.AddDroppedColumns(context.Background(), []string{"col1", "col2"}, sourceTable1))
	require.NoError(t, droppedColumnsStore.AddDroppedColumns(context.Background(), []string{"col1", "col2"}, sourceTable2))
	require.NoError(t, droppedColumnsStore.DelDroppedColumnForTable(context.Background(), sourceTable1))
	require.False(t, droppedColumnsStore.HasDroppedColumn(context.Background(), "col1", sourceTable1))
	require.True(t, droppedColumnsStore.HasDroppedColumn(context.Background(), "col1", sourceTable2))
	require.NoError(t, droppedColumnsStore.DelDroppedColumnForTable(context.Background(), sourceTable2))
	require.False(t, droppedColumnsStore.HasDroppedColumn(context.Background(), "col1", sourceTable2))

	require.NoError(t, droppedColumnsStore.AddDroppedColumns(context.Background(), []string{"col1"}, sourceTable1))
	require.NoError(t, droppedColumnsStore.AddDroppedColumns(context.Background(), []string{"col1"}, sourceTable2))
	require.NoError(t, DelAllDroppedColumns(context.Background(), droppedColumnsStore.kvClient))
	// mock kv client doesn't support WithPrefix
	// require.False(t, droppedColumnsStore.HasDroppedColumn(context.Background(), "col1", sourceTable1))
	// require.False(t, droppedColumnsStore.HasDroppedColumn(context.Background(), "col1", sourceTable2))
}
