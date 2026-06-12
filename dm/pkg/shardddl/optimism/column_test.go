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

package optimism

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColumnETCD(t *testing.T) {
	defer clearTestInfoOperation(t)

	var (
		task       = "test"
		downSchema = "shardddl"
		downTable  = "tb"
		source1    = "mysql-replica-1"
		source2    = "mysql-replica-2"
		upSchema1  = "shardddl1"
		upTable1   = "tb1"
		upSchema2  = "shardddl2"
		upTable2   = "tb2"
		info1      = NewInfo(task, source1, upSchema1, upTable1, downSchema, downTable, nil, nil, nil)
		lockID     = genDDLLockID(info1)
	)
	rev1, putted, err := PutDroppedColumns(etcdTestCli, lockID, source1, upSchema1, upTable1, []string{"a"}, DropNotDone)
	require.NoError(t, err)
	require.True(t, putted)
	rev2, putted, err := PutDroppedColumns(etcdTestCli, lockID, source1, upSchema1, upTable1, []string{"b"}, DropNotDone)
	require.NoError(t, err)
	require.True(t, putted)
	require.Greater(t, rev2, rev1)
	rev3, putted, err := PutDroppedColumns(etcdTestCli, lockID, source1, upSchema2, upTable2, []string{"b"}, DropNotDone)
	require.NoError(t, err)
	require.True(t, putted)
	require.Greater(t, rev3, rev2)
	rev4, putted, err := PutDroppedColumns(etcdTestCli, lockID, source2, upSchema1, upTable1, []string{"b"}, DropNotDone)
	require.NoError(t, err)
	require.True(t, putted)
	require.Greater(t, rev4, rev3)
	rev5, putted, err := PutDroppedColumns(etcdTestCli, lockID, source2, upSchema1, upTable1, []string{"b", "c"}, DropNotDone)
	require.NoError(t, err)
	require.True(t, putted)
	require.Greater(t, rev5, rev4)

	expectedColm := map[string]map[string]map[string]map[string]map[string]DropColumnStage{
		lockID: {
			"a": {source1: {upSchema1: {upTable1: DropNotDone}}},
			"b": {
				source1: {
					upSchema1: {upTable1: DropNotDone},
					upSchema2: {upTable2: DropNotDone},
				},
				source2: {upSchema1: {upTable1: DropNotDone}},
			},
			"c": {
				source2: {upSchema1: {upTable1: DropNotDone}},
			},
		},
	}
	colm, rev6, err := GetAllDroppedColumns(etcdTestCli)
	require.NoError(t, err)
	require.Equal(t, expectedColm, colm)
	require.Equal(t, rev5, rev6)

	rev7, deleted, err := DeleteDroppedColumns(etcdTestCli, lockID, "b", "c")
	require.NoError(t, err)
	require.True(t, deleted)
	require.Greater(t, rev7, rev6)

	delete(expectedColm[lockID], "b")
	delete(expectedColm[lockID], "c")
	colm, rev8, err := GetAllDroppedColumns(etcdTestCli)
	require.NoError(t, err)
	require.Equal(t, expectedColm, colm)
	require.Equal(t, rev7, rev8)
}
