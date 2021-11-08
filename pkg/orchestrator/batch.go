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
	"go.etcd.io/etcd/clientv3"
)

const (
	maxBatchPatchSize    = 64
	maxBatchResponseSize = 64
)

// getBatchPatches batch patches
func getBatchPatches(patchGroups [][]DataPatch) ([]DataPatch, int) {
	batchPatches := make([]DataPatch, 0)
	patchesNum := 0
	for i, patches := range patchGroups {
		// make sure there at least one patches are put into batchPatches
		if i == 0 {
			batchPatches = append(batchPatches, patches...)
			patchesNum++
			continue
		}

		if (len(batchPatches) + len(patches)) > maxBatchPatchSize {
			break
		}
		batchPatches = append(batchPatches, patches...)
		patchesNum++
	}
	return batchPatches, patchesNum
}

// getBatchResponse try to get more WatchResponse from watchCh
func getBatchResponse(watchCh clientv3.WatchChan, revision int64) ([]clientv3.WatchResponse, int64, error) {
	responses := make([]clientv3.WatchResponse, 0)
BATCH:
	for {
		select {
		case response := <-watchCh:
			if err := response.Err(); err != nil {
				// we should always return revision
				return nil, revision, errors.Trace(err)
			}
			if revision >= response.Header.GetRevision() {
				continue
			}
			revision = response.Header.GetRevision()
			if response.IsProgressNotify() {
				continue
			}
			responses = append(responses, response)
			if len(responses) == maxBatchResponseSize {
				break BATCH
			}
		default:
			break BATCH
		}
	}
	return responses, revision, nil
}
