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

package orchestrator

import (
	"encoding/json"
	"reflect"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// JSONReactorState models a single key whose value is a json object.
type JSONReactorState struct {
	// jsonData stores an object serializable to a valid `value` corresponding to `key`.
	jsonData interface{}
	// modifiedJSONData is the modified snapshot of jsonData that has not been uploaded to Etcd.
	modifiedJSONData   interface{}
	key                string
	isUpdatedByReactor bool
}

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
		key:                key,
		isUpdatedByReactor: false,
	}, nil
}

// Update implements the ReactorState interface.
func (s *JSONReactorState) Update(key []byte, value []byte) {
	if string(key) != s.key {
		return
	}

	err := json.Unmarshal(value, s.jsonData)
	if err != nil {
		log.Panic("Cannot parse JSON state",
			zap.ByteString("key", key),
			zap.ByteString("value", value))
	}

	log.Debug("Update", zap.ByteString("key", key), zap.ByteString("value", value))

	deepCopy(s.jsonData, s.modifiedJSONData)
	s.isUpdatedByReactor = true
}

// GetPatches implements the ReactorState interface.
// The patches are generated and applied according to the standard RFC6902 JSON patches.
func (s *JSONReactorState) GetPatches() []*DataPatch {
	oldBytes, err := json.Marshal(s.jsonData)
	if err != nil {
		log.Panic("Cannot marshal JSON state", zap.String("key", s.key), zap.Reflect("json", s.jsonData))
	}

	newBytes, err := json.Marshal(s.modifiedJSONData)
	if err != nil {
		log.Panic("Cannot marshal JSON state", zap.String("key", s.key), zap.Reflect("json", s.modifiedJSONData))
	}

	patchBytes, err := jsonpatch.CreateMergePatch(oldBytes, newBytes)
	if err != nil {
		log.Panic("Cannot generate JSON patch", zap.ByteString("old", oldBytes), zap.ByteString("new", newBytes))
	}

	dataPatch := &DataPatch{
		Key: []byte(s.key),
		Fun: func(old []byte) ([]byte, error) {
			ret, err := jsonpatch.MergePatch(old, patchBytes)
			if err != nil {
				return nil, errors.Trace(err)
			}
			log.Debug("json patch applied", zap.ByteString("patch", patchBytes), zap.ByteString("old", old), zap.ByteString("new", ret))
			return ret, nil
		},
	}

	return []*DataPatch{dataPatch}
}

// Inner returns a copy of the snapshot of the state, which can safely be updated by the reactor within a tick.
func (s *JSONReactorState) Inner() interface{} {
	return s.modifiedJSONData
}

func deepCopy(a, b interface{}) {
	byt, _ := json.Marshal(a)
	_ = json.Unmarshal(byt, b)
}
