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

package orchestrator

import (
	"fmt"
	"testing"

	"github.com/pingcap/tiflow/pkg/orchestrator/util"
	"github.com/stretchr/testify/require"
)

func TestGetBatchChangeState(t *testing.T) {
	t.Parallel()
	patchGroupSize := 1000
	patchGroup := make([][]DataPatch, patchGroupSize)
	for i := 0; i < patchGroupSize; i++ {
		i := i
		patches := []DataPatch{&SingleDataPatch{
			Key: util.NewEtcdKey(fmt.Sprintf("/key%d", i)),
			Func: func(old []byte) (newValue []byte, changed bool, err error) {
				newValue = []byte(fmt.Sprintf("abc%d", i))
				return newValue, true, nil
			},
		}}
		patchGroup[i] = patches
	}
	rawState := make(map[util.EtcdKey][]byte)
	changedState, n, size, err := getBatchChangedState(rawState, patchGroup)
	require.Nil(t, err)
	require.LessOrEqual(t, n, len(patchGroup))
	require.LessOrEqual(t, size, etcdTxnMaxSize)
	require.LessOrEqual(t, len(changedState), etcdTxnMaxOps)
	require.Equal(t, []byte(fmt.Sprintf("abc%d", 0)), changedState[util.NewEtcdKey("/key0")])

	// test single patch exceed txn max size
	largeSizePatches := []DataPatch{&SingleDataPatch{
		Key: util.NewEtcdKey("largePatch"),
		Func: func(old []byte) (newValue []byte, changed bool, err error) {
			newValue = make([]byte, etcdTxnMaxSize)
			return newValue, true, nil
		},
	}}
	patchGroup = [][]DataPatch{largeSizePatches}
	_, _, _, err = getBatchChangedState(rawState, patchGroup)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "a single changefeed exceed etcd txn max size")

	// test single patch exceed txn max ops
	manyOpsPatches := make([]DataPatch, 0)
	for i := 0; i <= etcdTxnMaxOps*2; i++ {
		manyOpsPatches = append(manyOpsPatches, &SingleDataPatch{
			Key: util.NewEtcdKey(fmt.Sprintf("/key%d", i)),
			Func: func(old []byte) (newValue []byte, changed bool, err error) {
				newValue = []byte(fmt.Sprintf("abc%d", i))
				return newValue, true, nil
			},
		})
	}
	patchGroup = [][]DataPatch{manyOpsPatches}
	_, _, _, err = getBatchChangedState(rawState, patchGroup)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "a single changefeed exceed etcd txn max ops")
}
