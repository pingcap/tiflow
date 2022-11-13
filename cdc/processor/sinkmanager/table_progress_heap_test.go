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

package sinkmanager

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/sorter"
	"github.com/stretchr/testify/require"
)

func TestTableProgresses(t *testing.T) {
	t.Parallel()

	tableState := tablepb.TableStateReplicating

	p := newTableProgresses()
	p.push(&progress{
		tableID: 1,
		nextLowerBoundPos: sorter.Position{
			StartTs:  1,
			CommitTs: 2,
		},
		tableState: &tableState,
	})
	p.push(&progress{
		tableID: 3,
		nextLowerBoundPos: sorter.Position{
			StartTs:  2,
			CommitTs: 2,
		},
		tableState: &tableState,
	})
	p.push(&progress{
		tableID: 2,
		nextLowerBoundPos: sorter.Position{
			StartTs:  2,
			CommitTs: 3,
		},
		tableState: &tableState,
	})

	require.Equal(t, p.len(), 3)

	pg := p.pop()
	require.Equal(t, int64(1), pg.tableID, "table1 is the slowest table")
	pg = p.pop()
	require.Equal(t, int64(3), pg.tableID, "table2 is the slowest table")
	pg = p.pop()
	require.Equal(t, int64(2), pg.tableID, "table3 is the slowest table")

	require.Equal(t, p.len(), 0, "all tables are popped")
}

func TestHeapWithState(t *testing.T) {
	t.Parallel()
	replicatingState := tablepb.TableStateReplicating
	stoppingState := tablepb.TableStateStopping
	preparingState := tablepb.TableStatePreparing

	p := newTableProgresses()
	p.push(&progress{
		tableID: 1,
		nextLowerBoundPos: sorter.Position{
			StartTs:  1,
			CommitTs: 2,
		},
		tableState: &stoppingState,
	})
	p.push(&progress{
		tableID: 3,
		nextLowerBoundPos: sorter.Position{
			StartTs:  2,
			CommitTs: 2,
		},
		tableState: &replicatingState,
	})
	p.push(&progress{
		tableID: 2,
		nextLowerBoundPos: sorter.Position{
			StartTs:  2,
			CommitTs: 3,
		},
		tableState: &preparingState,
	})

	require.Equal(t, p.len(), 3)

	pg := p.pop()
	require.Equal(t, int64(3), pg.tableID, "table3 is replicating, so it comes first")
	pg = p.pop()
	require.Equal(t, int64(1), pg.tableID, "other states are equal, so table1 comes first")
	pg = p.pop()
	require.Equal(t, int64(2), pg.tableID, "table2 is last")

	require.Equal(t, p.len(), 0, "all tables are popped")
}
