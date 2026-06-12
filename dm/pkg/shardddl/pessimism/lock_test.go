// Copyright 2020 PingCAP, Inc.
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

package pessimism

import (
	"testing"

	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestLock(t *testing.T) {
	var (
		ID      = "test-`foo`.`bar`"
		task    = "test"
		source1 = "mysql-replica-1"
		source2 = "mysql-replica-2"
		source3 = "mysql-replica-3"
		DDLs    = []string{
			"ALTER TABLE bar ADD COLUMN c1 INT",
			"ALTER TABLE bar ADD COLUMN c2 INT",
		}
	)

	// create the lock with only 1 source.
	l1 := NewLock(ID, task, source1, DDLs, []string{source1})

	// DDLs mismatch.
	synced, remain, err := l1.TrySync(source1, DDLs[1:], []string{source1})
	require.True(t, terror.ErrMasterShardingDDLDiff.Equal(err))
	require.False(t, synced)
	require.Equal(t, 1, remain)
	require.Equal(t, map[string]bool{source1: false}, l1.Ready())
	synced, _ = l1.IsSynced()
	require.False(t, synced)
	require.False(t, l1.IsDone(source1))
	require.False(t, l1.IsResolved())

	// synced.
	synced, remain, err = l1.TrySync(source1, DDLs, []string{source1})
	require.NoError(t, err)
	require.True(t, synced)
	require.Equal(t, 0, remain)
	require.Equal(t, map[string]bool{source1: true}, l1.Ready())
	synced, _ = l1.IsSynced()
	require.True(t, synced)
	require.False(t, l1.IsDone(source1))
	require.False(t, l1.IsResolved())

	// mark done.
	l1.MarkDone(source1)
	require.True(t, l1.IsDone(source1))
	require.True(t, l1.IsResolved())

	// create the lock with 2 sources.
	l2 := NewLock(ID, task, source1, DDLs, []string{source1, source2})

	// join a new source.
	synced, remain, err = l2.TrySync(source1, DDLs, []string{source2, source3})
	require.NoError(t, err)
	require.False(t, synced)
	require.Equal(t, 2, remain)
	require.Equal(t, map[string]bool{
		source1: true,
		source2: false,
		source3: false,
	}, l2.Ready())

	// sync other sources.
	synced, remain, err = l2.TrySync(source2, DDLs, []string{})
	require.NoError(t, err)
	require.False(t, synced)
	require.Equal(t, 1, remain)
	require.Equal(t, map[string]bool{
		source1: true,
		source2: true,
		source3: false,
	}, l2.Ready())
	synced, remain, err = l2.TrySync(source3, DDLs, nil)
	require.NoError(t, err)
	require.True(t, synced)
	require.Equal(t, 0, remain)
	require.Equal(t, map[string]bool{
		source1: true,
		source2: true,
		source3: true,
	}, l2.Ready())

	// done none.
	require.False(t, l2.IsDone(source1))
	require.False(t, l2.IsDone(source2))
	require.False(t, l2.IsDone(source3))
	require.False(t, l2.IsResolved())

	// done some.
	l2.MarkDone(source1)
	l2.MarkDone(source2)
	require.True(t, l2.IsDone(source1))
	require.True(t, l2.IsDone(source2))
	require.False(t, l2.IsDone(source3))
	require.False(t, l2.IsResolved())

	// done all.
	l2.MarkDone(source3)
	require.True(t, l2.IsDone(source3))
	require.True(t, l2.IsResolved())

	// mark on not existing source has no effect.
	l2.MarkDone("not-exist-source")
	require.True(t, l2.IsResolved())

	// create the lock with 2 sources.
	l3 := NewLock(ID, task, source1, DDLs, []string{source1, source2})
	l3.ForceSynced()
	synced, remain = l3.IsSynced()
	require.True(t, synced)
	require.Equal(t, 0, remain)

	// revert the synced stage.
	l3.RevertSynced([]string{source2})
	synced, remain = l3.IsSynced()
	require.False(t, synced)
	require.Equal(t, 1, remain)
	ready := l3.Ready()
	require.True(t, ready[source1])
	require.False(t, ready[source2])
}
