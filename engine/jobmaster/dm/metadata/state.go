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
	"encoding/json"
	"reflect"
	"sync"

	"github.com/BurntSushi/toml"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// state represents the state which need to be stored in metadata.
type state interface{}

// stateFactory creates a type of state.
type stateFactory interface {
	createState() state
	key() string
}

// Store holds one state instance.
type Store interface {
	Put(ctx context.Context, state state) error
	Delete(ctx context.Context) error
	Get(ctx context.Context) (state, error)
}

// ErrStateNotFound is returned when the state is not found in metadata.
var ErrStateNotFound = errors.New("state not found")

// frameworkMetaStore implements Store interface. It persists a state instance in
// framework metadata with given encode/decode function, and will cache the latest
// state. It's thread-safe.
type frameworkMetaStore struct {
	stateFactory

	mu         sync.RWMutex
	stateCache state
	kvClient   metaModel.KVClient
	encodeFn   func(state) ([]byte, error)
	decodeFn   func([]byte, state) error
}

func (f *frameworkMetaStore) Put(ctx context.Context, state state) error {
	if !checkAllFieldsIsPublic(state) {
		return errors.New("fields of state should all be public")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	putOp, err := f.putOp(state)
	if err != nil {
		return errors.Trace(err)
	}

	if _, err = f.kvClient.Txn(ctx).Do(putOp).Commit(); err != nil {
		return errors.Trace(err)
	}

	f.stateCache = state
	return nil
}

func (f *frameworkMetaStore) putOp(state state) (metaModel.Op, error) {
	value, err := f.encodeFn(state)
	if err != nil {
		return metaModel.Op{}, errors.Trace(err)
	}
	return metaModel.OpPut(f.key(), string(value)), nil
}

func (f *frameworkMetaStore) Delete(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.stateCache == nil {
		return nil
	}

	delOp := f.deleteOp()
	if _, err := f.kvClient.Txn(ctx).Do(delOp).Commit(); err != nil {
		return errors.Trace(err)
	}

	f.stateCache = nil
	return nil
}

func (f *frameworkMetaStore) deleteOp() metaModel.Op {
	return metaModel.OpDelete(f.key())
}

func (f *frameworkMetaStore) Get(ctx context.Context) (state, error) {
	f.mu.RLock()
	if f.stateCache != nil {
		clone, err := f.cloneState()
		f.mu.RUnlock()
		return clone, err
	}
	f.mu.RUnlock()

	f.mu.Lock()
	defer f.mu.Unlock()

	// check again with write lock
	if f.stateCache != nil {
		return f.cloneState()
	}

	resp, err := f.kvClient.Get(ctx, f.key())
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(resp.Kvs) == 0 {
		return nil, ErrStateNotFound
	}

	f.stateCache = f.createState()

	if err2 := f.decodeFn(resp.Kvs[0].Value, f.stateCache); err2 != nil {
		return nil, errors.Trace(err2)
	}

	return f.cloneState()
}

func (f *frameworkMetaStore) cloneState() (state, error) {
	if f.stateCache == nil {
		return nil, nil
	}

	s, err := f.encodeFn(f.stateCache)
	if err != nil {
		return nil, errors.Trace(err)
	}
	clone := f.createState()
	if err2 := f.decodeFn(s, clone); err2 != nil {
		return nil, errors.Trace(err2)
	}
	return clone, nil
}

var stateTp = reflect.TypeOf((*state)(nil)).Elem()

// checkAllFieldsIsPublic check all fields of a state is public.
func checkAllFieldsIsPublic(state state) bool {
	v := reflect.ValueOf(state)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if !field.CanSet() {
			// skip report error for the embedded state field
			if field.Type() == stateTp {
				continue
			}
			return false
		}
	}
	return true
}

func tomlEncodeFn(state state) ([]byte, error) {
	var b bytes.Buffer
	err := toml.NewEncoder(&b).Encode(state)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return b.Bytes(), nil
}

func tomlDecodeFn(b []byte, state state) error {
	_, err := toml.Decode(string(b), state)
	return err
}

func newTOMLFrameworkMetaStore(kvClient metaModel.KVClient) *frameworkMetaStore {
	return &frameworkMetaStore{
		kvClient: kvClient,
		encodeFn: tomlEncodeFn,
		decodeFn: tomlDecodeFn,
	}
}

func jsonEncodeFn(state state) ([]byte, error) {
	return json.Marshal(state)
}

func jsonDecodeFn(b []byte, state state) error {
	return json.Unmarshal(b, state)
}

func newJSONFrameworkMetaStore(kvClient metaModel.KVClient) *frameworkMetaStore {
	return &frameworkMetaStore{
		kvClient: kvClient,
		encodeFn: jsonEncodeFn,
		decodeFn: jsonDecodeFn,
	}
}
