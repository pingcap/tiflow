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
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
)

// MergeCommutativePatches merges patches to the same key as one.
// Since the order of applications is not guaranteed, please only use with commutative patches.
func MergeCommutativePatches(patches []*DataPatch) []*DataPatch {
	patchesByKey := make(map[util.EtcdKey][]*DataPatch)
	for _, patch := range patches {
		key := patch.Key
		patchesByKey[key] = append(patchesByKey[key], patch)
	}

	mergedPatches := make([]*DataPatch, 0, len(patchesByKey))
	for key, patches := range patchesByKey {
		mergedPatch := &DataPatch{
			Key: key,
			Fun: func(old []byte) (newValue []byte, err error) {
				buf := old
				for _, patch := range patches {
					var err error
					buf, err = patch.Fun(buf)
					if err != nil {
						if cerrors.ErrEtcdIgnore.Equal(err) {
							continue
						}
						return nil, err
					}
				}
				return buf, nil
			},
		}
		mergedPatches = append(mergedPatches, mergedPatch)
	}

	return mergedPatches
}
