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

	"github.com/stretchr/testify/require"
)

func TestLockKeeper(t *testing.T) {
	var (
		lk      = NewLockKeeper()
		schema  = "foo"
		table   = "bar"
		DDLs    = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		task1   = "task1"
		task2   = "task2"
		source1 = "mysql-replica-1"
		source2 = "mysql-replica-2"
		info11  = NewInfo(task1, source1, schema, table, DDLs)
		info12  = NewInfo(task1, source2, schema, table, DDLs)
		info21  = NewInfo(task2, source1, schema, table, DDLs)
	)

	// lock with 2 sources.
	lockID1, synced, remain, err := lk.TrySync(info11, []string{source1, source2})
	require.NoError(t, err)
	require.Equal(t, "task1-`foo`.`bar`", lockID1)
	require.False(t, synced)
	require.Equal(t, 1, remain)
	lockID1, synced, remain, err = lk.TrySync(info12, []string{source1, source2})
	require.NoError(t, err)
	require.Equal(t, "task1-`foo`.`bar`", lockID1)
	require.True(t, synced)
	require.Equal(t, 0, remain)

	// lock with only 1 source.
	lockID2, synced, remain, err := lk.TrySync(info21, []string{source1})
	require.NoError(t, err)
	require.Equal(t, "task2-`foo`.`bar`", lockID2)
	require.True(t, synced)
	require.Equal(t, 0, remain)

	// find lock.
	lock1 := lk.FindLock(lockID1)
	require.NotNil(t, lock1)
	require.Equal(t, lockID1, lock1.ID)
	lock2 := lk.FindLock(lockID2)
	require.NotNil(t, lock2)
	require.Equal(t, lockID2, lock2.ID)
	lockIDNotExists := "lock-not-exists"
	require.Nil(t, lk.FindLock(lockIDNotExists))

	// all locks.
	locks := lk.Locks()
	require.Len(t, locks, 2)
	require.Equal(t, lock1, locks[lockID1]) // compare pointer
	require.Equal(t, lock2, locks[lockID2])

	// remove lock.
	require.True(t, lk.RemoveLock(lockID1))
	require.False(t, lk.RemoveLock(lockIDNotExists))
	require.Len(t, lk.Locks(), 1)

	// clear locks.
	lk.Clear()

	// no locks exist.
	require.Len(t, lk.Locks(), 0)
}
