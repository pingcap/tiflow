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
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
)

// ReactorStateTester is a helper struct for unit-testing an implementer of ReactorState
type ReactorStateTester struct {
	state     ReactorState
	kvEntries map[string]string
}

// NewReactorStateTester creates a new ReactorStateTester
func NewReactorStateTester(state ReactorState, initKVEntries map[string]string) *ReactorStateTester {
	return &ReactorStateTester{
		state:     state,
		kvEntries: initKVEntries,
	}
}

// UpdateKeys is used to update keys in the mocked kv-store.
func (t *ReactorStateTester) UpdateKeys(updatedKeys map[string][]byte) error {
	for key, value := range updatedKeys {
		k := util.NewEtcdKey(key)
		err := t.state.Update(k, value, false)
		if err != nil {
			return errors.Trace(err)
		}

		if value != nil {
			t.kvEntries[key] = string(value)
		} else {
			delete(t.kvEntries, key)
		}
	}

	return nil
}

// ApplyPatches calls the GetPatches method on the ReactorState and apply the changes to the mocked kv-store.
func (t *ReactorStateTester) ApplyPatches() error {
	patches := t.state.GetPatches()
	mergedPatches := mergePatch(patches)

	for _, patch := range mergedPatches {
		old, ok := t.kvEntries[patch.Key.String()]
		var (
			newBytes []byte
			err      error
		)
		if ok {
			newBytes, err = patch.Fun([]byte(old))
		} else {
			newBytes, err = patch.Fun(nil)
		}
		if cerrors.ErrEtcdIgnore.Equal(errors.Cause(err)) {
			continue
		}
		if err != nil {
			return errors.Trace(err)
		}
		err = t.state.Update(patch.Key, newBytes, false)
		if err != nil {
			return errors.Trace(err)
		}
		if newBytes == nil {
			delete(t.kvEntries, patch.Key.String())
			continue
		}
		t.kvEntries[patch.Key.String()] = string(newBytes)
	}

	return nil
}

// KVEntries returns the contents of the mocked KV store.
func (t *ReactorStateTester) KVEntries() map[string]string {
	return t.kvEntries
}
