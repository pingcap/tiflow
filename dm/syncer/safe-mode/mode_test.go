// Copyright 2019 PingCAP, Inc.
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

package mode

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util/filter"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/stretchr/testify/require"
)

func TestMode(t *testing.T) {
	m := NewSafeMode()
	require.False(t, m.Enable())

	tctx := tcontext.Background()
	// Add 1
	err := m.Add(tctx, 1)
	require.NoError(t, err)
	require.True(t, m.Enable())
	err = m.Add(tctx, -1)
	require.False(t, m.Enable())
	require.NoError(t, err)

	// Add n
	err = m.Add(tctx, 101)
	require.True(t, m.Enable())
	require.NoError(t, err)
	err = m.Add(tctx, -1)
	require.True(t, m.Enable())
	require.NoError(t, err)
	err = m.Add(tctx, -100)
	require.False(t, m.Enable())
	require.NoError(t, err)

	// IncrForTable
	table := &filter.Table{
		Schema: "schema",
		Name:   "table",
	}
	err = m.IncrForTable(tctx, table)
	require.NoError(t, err)
	err = m.IncrForTable(tctx, table) // re-Add
	require.NoError(t, err)
	require.True(t, m.Enable())
	err = m.DescForTable(tctx, table)
	require.NoError(t, err)
	require.False(t, m.Enable())

	// Add n + IncrForTable
	err = m.Add(tctx, 100)
	require.NoError(t, err)
	err = m.IncrForTable(tctx, table)
	require.NoError(t, err)
	require.True(t, m.Enable())
	err = m.Add(tctx, -100)
	require.NoError(t, err)
	err = m.DescForTable(tctx, table)
	require.False(t, m.Enable())
	require.NoError(t, err)

	// Add becomes to negative
	err = m.Add(tctx, -1)
	require.Error(t, err)
}
