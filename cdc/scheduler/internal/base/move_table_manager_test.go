// Copyright 2021 PingCAP, Inc.
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

package base

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestMoveTableManagerBasics(t *testing.T) {
	m := newMoveTableManager()

	// Test 1: Add a table.
	m.Add(1, "capture-1")
	_, ok := m.GetTargetByTableID(1)
	require.False(t, ok)

	// Test 2: Add a table again.
	m.Add(2, "capture-2")
	_, ok = m.GetTargetByTableID(2)
	require.False(t, ok)

	// Test 3: Add a table with the same ID.
	ok = m.Add(2, "capture-2-1")
	require.False(t, ok)

	ctx := context.Background()
	// Test 4: Remove one table
	var removedTable model.TableID
	ok, err := m.DoRemove(ctx, func(ctx context.Context, tableID model.TableID, _ model.CaptureID) (result removeTableResult, err error) {
		if removedTable != 0 {
			return removeTableResultUnavailable, nil
		}
		removedTable = tableID
		return removeTableResultOK, nil
	})
	require.NoError(t, err)
	require.False(t, ok)
	require.Containsf(t, []model.TableID{1, 2}, removedTable, "removedTable: %d", removedTable)

	// Test 5: Check removed table's target
	target, ok := m.GetTargetByTableID(removedTable)
	require.True(t, ok)
	require.Equal(t, fmt.Sprintf("capture-%d", removedTable), target)

	// Test 6: Remove another table
	var removedTable1 model.TableID
	_, err = m.DoRemove(ctx, func(ctx context.Context, tableID model.TableID, _ model.CaptureID) (result removeTableResult, err error) {
		if removedTable1 != 0 {
			require.Fail(t, "Should not have been called twice")
		}
		removedTable1 = tableID
		return removeTableResultOK, nil
	})
	require.NoError(t, err)

	// Test 7: Mark table done
	m.MarkDone(1)
	_, ok = m.GetTargetByTableID(1)
	require.False(t, ok)
}

func TestMoveTableManagerCaptureRemoved(t *testing.T) {
	m := newMoveTableManager()

	ok := m.Add(1, "capture-1")
	require.True(t, ok)

	ok = m.Add(2, "capture-2")
	require.True(t, ok)

	ok = m.Add(3, "capture-1")
	require.True(t, ok)

	ok = m.Add(4, "capture-2")
	require.True(t, ok)

	m.OnCaptureRemoved("capture-2")

	ctx := context.Background()
	var count int
	ok, err := m.DoRemove(ctx,
		func(ctx context.Context, tableID model.TableID, target model.CaptureID) (result removeTableResult, err error) {
			require.NotEqual(t, model.TableID(2), tableID)
			require.NotEqual(t, model.TableID(4), tableID)
			require.Equal(t, "capture-1", target)
			count++
			return removeTableResultOK, nil
		},
	)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestMoveTableManagerGiveUp(t *testing.T) {
	m := newMoveTableManager()

	ok := m.Add(1, "capture-1")
	require.True(t, ok)

	ok = m.Add(2, "capture-2")
	require.True(t, ok)

	ctx := context.Background()
	ok, err := m.DoRemove(ctx,
		func(ctx context.Context, tableID model.TableID, target model.CaptureID) (result removeTableResult, err error) {
			if tableID == 1 {
				return removeTableResultOK, nil
			}
			return removeTableResultGiveUp, nil
		},
	)
	require.NoError(t, err)
	require.True(t, ok)

	target, ok := m.GetTargetByTableID(1)
	require.True(t, ok)
	require.Equal(t, "capture-1", target)

	_, ok = m.GetTargetByTableID(2)
	require.False(t, ok)
}
