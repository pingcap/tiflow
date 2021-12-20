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
	"github.com/pingcap/errors"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator/util"
)

const (
	// 1.25 MiB
	// Ref: https://etcd.io/docs/v3.3/dev-guide/limit/
	etcdTxnMaxSize = 1024 * (1024 + 256)
	// Ref: https://etcd.io/docs/v3.3/op-guide/configuration/#--max-txn-ops
	etcdTxnMaxOps = 128
)

// getBatchChangedState has 4 return values:
// 1.batchChangedSate
// 2.number of patch apply to batchChangedState
// 3.size of batchChangedState in byte
// 4.error
func getBatchChangedState(state map[util.EtcdKey][]byte, patchGroups [][]DataPatch) (map[util.EtcdKey][]byte, int, int, error) {
	num := 0
	totalSize := 0
	// store changedState of multiple changefeed
	batchChangedState := make(map[util.EtcdKey][]byte)
	for i, patches := range patchGroups {
		changedState, changedSize, err := getChangedState(state, patches)
		if err != nil {
			return nil, 0, 0, err
		}
		// if a changefeed's changedState size is larger than etcdTxnMaxSize
		// or the length of changedState is larger than etcdTxnMaxOps
		// we should return an error instantly
		if i == 0 {
			if changedSize > etcdTxnMaxSize {
				return nil, 0, 0, cerrors.ErrEtcdTxnSizeExceed.GenWithStackByArgs(changedSize, etcdTxnMaxSize)
			}
			if len(changedState) > etcdTxnMaxOps {
				return nil, 0, 0, cerrors.ErrEtcdTxnOpsExceed.GenWithStackByArgs(len(changedState), etcdTxnMaxOps)
			}
		}

		// batchChangedState size should not exceeds the etcdTxnMaxSize limit
		// and keys numbers should not exceeds the etcdTxnMaxOps limit
		if totalSize+changedSize >= etcdTxnMaxSize ||
			len(batchChangedState)+len(changedState) >= etcdTxnMaxOps {
			break
		}
		for k, v := range changedState {
			batchChangedState[k] = v
		}
		num++
		totalSize += changedSize
	}
	return batchChangedState, num, totalSize, nil
}

func getChangedState(state map[util.EtcdKey][]byte, patches []DataPatch) (map[util.EtcdKey][]byte, int, error) {
	changedSet := make(map[util.EtcdKey]struct{})
	changeState := make(map[util.EtcdKey][]byte)
	changedSize := 0
	for _, patch := range patches {
		err := patch.Patch(state, changedSet)
		if err != nil {
			if cerrors.ErrEtcdIgnore.Equal(errors.Cause(err)) {
				continue
			}
			return nil, 0, errors.Trace(err)
		}
	}
	for k := range changedSet {
		v := state[k]
		changedSize += len(k.String())*2 + len(v)
		changeState[k] = v
	}
	return changeState, changedSize, nil
}
