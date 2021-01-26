// Copyright 2020 PingCAP, Inc.
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

package jsonstate

import (
	"encoding/json"
	"reflect"

	"github.com/pingcap/errors"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
)

// JSONReactorState models a single key whose value is a json object.
type JSONReactorState struct {
	// jsonData stores an object serializable to a valid `value` corresponding to `key`.
	jsonData interface{}
	// modifiedJSONData is the modified snapshot of jsonData that has not been uploaded to Etcd.
	modifiedJSONData   interface{}
	key                util.EtcdKey
	isUpdatedByReactor bool
	patches            []JSONPatchFunc
}

// JSONPatchFunc is a function that updates an object that is serializable to JSON.
// It is okay to modify the input and return the input itself.
// Use ErrEtcdTryAgain and ErrEtcdIgnore to trigger Etcd transaction retries and to give up this update.
type JSONPatchFunc = func(data interface{}) (newData interface{}, err error)

// NewJSONReactorState returns a new JSONReactorState.
// `data` needs to be a pointer to an object serializable in JSON.
func NewJSONReactorState(key string, data interface{}) (*JSONReactorState, error) {
	tp := reflect.TypeOf(data)
	if tp.Kind() != reflect.Ptr {
		return nil, errors.Errorf("expected pointer type, got %T", data)
	}

	copied := reflect.New(tp.Elem()).Interface()
	deepCopy(data, copied)

	return &JSONReactorState{
		jsonData:           data,
		modifiedJSONData:   copied,
		key:                util.NewEtcdKey(key),
		isUpdatedByReactor: false,
	}, nil
}

// Update implements the ReactorState interface.
func (s *JSONReactorState) Update(key util.EtcdKey, value []byte, isInit bool) error {
	if key != s.key {
		return nil
	}

	err := json.Unmarshal(value, s.jsonData)
	if err != nil {
		return errors.Trace(err)
	}

	deepCopy(s.jsonData, s.modifiedJSONData)
	s.isUpdatedByReactor = true
	return nil
}

// GetPatches implements the ReactorState interface.[]*orchestrator.DataPatch
func (s *JSONReactorState) GetPatches() []*orchestrator.DataPatch {
	if len(s.patches) == 0 {
		return []*orchestrator.DataPatch{}
	}

	// We need to let the PatchFunc capture the array of JSONPatchFunc's,
	// and let the DataPatch be the sole object referring to those JSONPatchFunc's,
	// so that JSONReactorState does not have to worry about when to clean them up.
	subPatches := make([]JSONPatchFunc, len(s.patches))
	copy(subPatches, s.patches)
	s.patches = s.patches[:0]

	dataPatch := &orchestrator.DataPatch{
		Key: s.key,
		Fun: func(old []byte) ([]byte, error) {
			tp := reflect.TypeOf(s.jsonData)
			oldStruct := reflect.New(tp.Elem()).Interface()
			err := json.Unmarshal(old, oldStruct)
			if err != nil {
				return nil, errors.Trace(err)
			}

			for _, f := range subPatches {
				newStruct, err := f(oldStruct)
				if err != nil {
					if cerrors.ErrEtcdIgnore.Equal(errors.Cause(err)) {
						continue
					}
					return nil, errors.Trace(err)
				}
				oldStruct = newStruct
			}

			newBytes, err := json.Marshal(oldStruct)
			if err != nil {
				return nil, errors.Trace(err)
			}

			return newBytes, nil
		},
	}

	return []*orchestrator.DataPatch{dataPatch}
}

// Inner returns a copy of the snapshot of the state.
// DO NOT modify the returned object. The modified object will not be persisted.
func (s *JSONReactorState) Inner() interface{} {
	return s.modifiedJSONData
}

// AddUpdateFunc accepts a JSONPatchFunc that updates the managed JSON-serializable object.
// If multiple JSONPatchFunc's are added within a Tick, they are applied in the order in which AddUpdateFunc has been called.
func (s *JSONReactorState) AddUpdateFunc(f JSONPatchFunc) {
	s.patches = append(s.patches, f)
}

// TODO optimize for performance
func deepCopy(a, b interface{}) {
	byt, _ := json.Marshal(a)
	_ = json.Unmarshal(byt, b)
}
