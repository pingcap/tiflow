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
	"github.com/modern-go/reflect2"
	"regexp"
	"strings"

	"github.com/pingcap/errors"
)

type Getter = func (interface{}, []byte) ([]byte, error)
type Setter = func (interface{}, []byte) error

type DataMapper struct {
	getters map[string]Getter
	setters map[string]Setter
}

func (m *DataMapper) Get(state interface{}, key []byte) (value []byte, err error) {
	for prefix, getter := range m.getters {
		if strings.HasPrefix(string(key), prefix) {
			return getter(state, key)
		}
	}
	return nil, errors.Errorf("DataMapper: cannot get value from %v with key %s", state, key)
}

func (m *DataMapper) Set(state interface{}, key []byte, value []byte) error {
	for prefix, setter := range m.setters {
		if strings.HasPrefix(string(key), prefix) {
			return setter(state, value)
		}
	}
	return errors.Errorf("DataMapper: cannot set value to %v with key %s", state, key)
}

func (m *DataMapper) RegisterGetter(prefix string, getter Getter) {
	m.getters[prefix] = getter
}

func (m *DataMapper) RegisterMapGetter(prefix string, getter func (interface{}) ([]byte, error)) {
	getMapKeyRegex := regexp.MustCompile(regexp.QuoteMeta(prefix) + "/(.+)")
	getter1 := func (state interface{}, key []byte) ([]byte, error) {
		m, err := getter(state)
		if err != nil {
			return nil, errors.Trace(err)
		}
		t := reflect2.TypeOf(m).(reflect2.MapType)
		mKey := getMapKeyRegex.FindStringSubmatch(string(key))[1]
		v := t.GetIndex(m, mKey)
		return json.Marshal(v)
	}
	m.getters[prefix] = getter1
}
