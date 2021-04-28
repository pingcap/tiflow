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
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
)

// ReactorStateTester is a helper struct for unit-testing an implementer of ReactorState
type ReactorStateTester struct {
	c         *check.C
	state     ReactorState
	kvEntries map[string]string
}

// NewReactorStateTester creates a new ReactorStateTester
func NewReactorStateTester(c *check.C, state ReactorState, initKVEntries map[string]string) *ReactorStateTester {
	if initKVEntries == nil {
		initKVEntries = make(map[string]string)
	}
	for k, v := range initKVEntries {
		err := state.Update(util.NewEtcdKey(k), []byte(v), true)
		c.Assert(err, check.IsNil)
	}
	return &ReactorStateTester{
		c:         c,
		state:     state,
		kvEntries: initKVEntries,
	}
}

// Update is used to update keys in the mocked kv-store.
func (t *ReactorStateTester) Update(key string, value []byte) error {
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
	return nil
}

// ApplyPatches calls the GetPatches method on the ReactorState and apply the changes to the mocked kv-store.
func (t *ReactorStateTester) ApplyPatches() error {
	patches := t.state.GetPatches()

	tmpKVEntries := make(map[util.EtcdKey][]byte)
	for k, v := range t.kvEntries {
		tmpKVEntries[util.NewEtcdKey(k)] = []byte(v)
	}
	changedSet := make(map[util.EtcdKey]struct{})
	for _, patch := range patches {
		err := patch.Patch(tmpKVEntries, changedSet)
		if err != nil {
			return err
		}
	}
	for k := range changedSet {
		err := t.state.Update(k, tmpKVEntries[k], false)
		if err != nil {
			return err
		}
		if value := tmpKVEntries[k]; value != nil {
			t.kvEntries[k.String()] = string(value)
		} else {
			delete(t.kvEntries, k.String())
		}
	}
	return nil
}

func (t *ReactorStateTester) MustApplyPatches() {
	t.c.Assert(t.ApplyPatches(), check.IsNil)
}

func (t *ReactorStateTester) MustUpdateKey(key string, value []byte) {
	t.c.Assert(t.Update(key, value), check.IsNil)
}

// KVEntries returns the contents of the mocked KV store.
func (t *ReactorStateTester) KVEntries() map[string]string {
	return t.kvEntries
}
