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

package tp

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"github.com/stretchr/testify/require"
)

func TestTableManager(t *testing.T) {
	t.Parallel()

	// pretend there are 4 tables
	mockTableExecutor := newMockTableExecutor()
	mockTableExecutor.tables[model.TableID(1)] = pipeline.TableStatePreparing
	mockTableExecutor.tables[model.TableID(2)] = pipeline.TableStatePrepared
	mockTableExecutor.tables[model.TableID(3)] = pipeline.TableStateReplicating
	mockTableExecutor.tables[model.TableID(4)] = pipeline.TableStateStopped

	tableM := newTableManager(mockTableExecutor)
	require.Equal(t, schedulepb.TableStatePreparing, tableM.tables[model.TableID(1)].status.State)
	require.Equal(t, schedulepb.TableStatePrepared, tableM.tables[model.TableID(2)].status.State)
	require.Equal(t, schedulepb.TableStateReplicating, tableM.tables[model.TableID(3)].status.State)
	require.Equal(t, schedulepb.TableStateStopped, tableM.tables[model.TableID(4)].status.State)

	table, ok := tableM.getTable(model.TableID(5))
	require.False(t, ok)
	require.Nil(t, table)

	table = tableM.addTable(model.TableID(1))
	require.Equal(t, schedulepb.TableStatePreparing, table.status.State)

	table = tableM.addTable(model.TableID(5))
	require.Equal(t, schedulepb.TableStateAbsent, table.status.State)

	tableM.dropTable(model.TableID(5))
	require.NotContains(t, tableM.tables, model.TableID(5))
}
