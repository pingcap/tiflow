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

	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/stretchr/testify/require"
)

func TestTableProgresses(t *testing.T) {
	t.Parallel()

	p := newTableProgresses()
	p.push(&progress{
		tableID: 1,
		nextLowerBoundPos: engine.Position{
			StartTs:  1,
			CommitTs: 2,
		},
	})
	p.push(&progress{
		tableID: 3,
		nextLowerBoundPos: engine.Position{
			StartTs:  2,
			CommitTs: 2,
		},
	})
	p.push(&progress{
		tableID: 2,
		nextLowerBoundPos: engine.Position{
			StartTs:  2,
			CommitTs: 3,
		},
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
