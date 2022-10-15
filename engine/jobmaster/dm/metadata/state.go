// Copyright 2022 PingCAP, Inc.
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

package metadata

import (
	"bytes"
	"context"
	"reflect"
	"sync"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
)

// State represents the state which need to be stored in metadata.
type State interface{}

// Store manages a type of state.
// Store provides the factory, some utility functions and persistence of State.
type Store interface {
	CreateState() State
	Key() string
}

// TomlStore implements some default methods of Store. It uses TOML serialization.
type TomlStore struct {
	Store

	state    State
	kvClient metaModel.KVClient

	mu sync.RWMutex
}

// NewTomlStore returns a new TomlStore instance
func NewTomlStore(kvClient metaModel.KVClient) *TomlStore {
	return &TomlStore{
		kvClient: kvClient,
	}
}

func (ds *TomlStore) putOp(state State) (metaModel.Op, error) {
	var b bytes.Buffer
	err := toml.NewEncoder(&b).Encode(state)
	if err != nil {
		return metaModel.Op{}, errors.Trace(err)
	}
	return metaModel.OpPut(ds.Key(), b.String()), nil
}

func (ds *TomlStore) deleteOp() metaModel.Op {
	return metaModel.OpDelete(ds.Key())
}

// checkAllFieldsIsPublic check all fields of a state is public.
func checkAllFieldsIsPublic(state State) bool {
	v := reflect.ValueOf(state)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < v.NumField(); i++ {
		if !v.Field(i).CanSet() {
			return false
		}
	}
	return true
}

// Put updates state into metastore
func (ds *TomlStore) Put(ctx context.Context, state State) error {
	if !checkAllFieldsIsPublic(state) {
		return errors.New("fields of state should all be public")
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()

	putOp, err := ds.putOp(state)
	if err != nil {
		return errors.Trace(err)
	}

	if _, err = ds.kvClient.Txn(ctx).Do(putOp).Commit(); err != nil {
		return errors.Trace(err)
	}

	ds.state = state
	return nil
}

// Delete deletes the state from metastore
func (ds *TomlStore) Delete(ctx context.Context) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.state == nil {
		return nil
	}

	delOp := ds.deleteOp()
	if _, err := ds.kvClient.Txn(ctx).Do(delOp).Commit(); err != nil {
		return errors.Trace(err)
	}

	ds.state = nil
	return nil
}

// Get queries State from metastore, it always return clone of a state.
func (ds *TomlStore) Get(ctx context.Context) (State, error) {
	ds.mu.RLock()
	if ds.state != nil {
		clone, err := ds.cloneState()
		ds.mu.RUnlock()
		return clone, err
	}
	ds.mu.RUnlock()

	ds.mu.Lock()
	defer ds.mu.Unlock()

	// check again with write lock
	if ds.state != nil {
		return ds.cloneState()
	}

	resp, err := ds.kvClient.Get(ctx, ds.Key())
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(resp.Kvs) == 0 {
		return nil, errors.New("state not found")
	}

	ds.state = ds.CreateState()
	if _, err := toml.Decode(string(resp.Kvs[0].Value), ds.state); err != nil {
		return nil, errors.Trace(err)
	}

	return ds.cloneState()
}

func (ds *TomlStore) cloneState() (State, error) {
	if ds.state == nil {
		return nil, nil
	}

	clone := ds.CreateState()
	var b bytes.Buffer
	err := toml.NewEncoder(&b).Encode(ds.state)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if _, err = toml.Decode(b.String(), clone); err != nil {
		return nil, err
	}

	return clone, errors.Trace(err)
}
