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
	"testing"

	"github.com/pingcap/errors"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator/util"
	"github.com/stretchr/testify/require"
)

// ReactorStateTester is a helper struct for unit-testing an implementer of ReactorState
type ReactorStateTester struct {
	t         *testing.T
	state     ReactorState
	kvEntries map[string]string
}

// NewReactorStateTester creates a new ReactorStateTester
func NewReactorStateTester(t *testing.T, state ReactorState, initKVEntries map[string]string) *ReactorStateTester {
	if initKVEntries == nil {
		initKVEntries = make(map[string]string)
	}
	for k, v := range initKVEntries {
		err := state.Update(util.NewEtcdKey(k), []byte(v), true)
		require.NoError(t, err)
	}
	return &ReactorStateTester{
		t:         t,
		state:     state,
		kvEntries: initKVEntries,
	}
}

// Update is used to update keys in the mocked kv-store.
func (r *ReactorStateTester) Update(key string, value []byte) error {
	k := util.NewEtcdKey(key)
	err := r.state.Update(k, value, false)
	if err != nil {
		return errors.Trace(err)
	}
	if value != nil {
		r.kvEntries[key] = string(value)
	} else {
		delete(r.kvEntries, key)
	}
	return nil
}

// ApplyPatches calls the GetPatches method on the ReactorState and apply the changes to the mocked kv-store.
func (r *ReactorStateTester) ApplyPatches() error {
	patchGroups := r.state.GetPatches()
	for _, patches := range patchGroups {
		err := r.applyPatches(patches)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ReactorStateTester) applyPatches(patches []DataPatch) error {
RetryLoop:
	for {
		tmpKVEntries := make(map[util.EtcdKey][]byte)
		for k, v := range r.kvEntries {
			tmpKVEntries[util.NewEtcdKey(k)] = []byte(v)
		}
		changedSet := make(map[util.EtcdKey]struct{})
		for _, patch := range patches {
			err := patch.Patch(tmpKVEntries, changedSet)
			if cerrors.ErrEtcdIgnore.Equal(errors.Cause(err)) {
				continue
			} else if cerrors.ErrEtcdTryAgain.Equal(errors.Cause(err)) {
				continue RetryLoop
			} else if err != nil {
				return errors.Trace(err)
			}
		}
		for k := range changedSet {
			err := r.state.Update(k, tmpKVEntries[k], false)
			if err != nil {
				return err
			}
			if value := tmpKVEntries[k]; value != nil {
				r.kvEntries[k.String()] = string(value)
			} else {
				delete(r.kvEntries, k.String())
			}
		}
		return nil
	}
}

// MustApplyPatches calls ApplyPatches and must successfully
func (r *ReactorStateTester) MustApplyPatches() {
	require.Nil(r.t, r.ApplyPatches())
}

// MustUpdate calls Update and must successfully
func (r *ReactorStateTester) MustUpdate(key string, value []byte) {
	require.Nil(r.t, r.Update(key, value))
}

// KVEntries returns the contents of the mocked KV store.
func (r *ReactorStateTester) KVEntries() map[string]string {
	return r.kvEntries
}
