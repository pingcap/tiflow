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

package ha

import (
	"errors"
	"sync"
)

// nolint:revive
type HAStore interface {
	// Put Key/Value
	Put(string, string) error
	Get(string) (string, error)
	Del(string) error
}

func NewMockStore() HAStore {
	return &mockStore{
		kv: make(map[string]string),
	}
}

type mockStore struct {
	sync.Mutex
	kv map[string]string
}

func (s *mockStore) Put(k, v string) error {
	s.Lock()
	defer s.Unlock()
	s.kv[k] = v
	return nil
}

func (s *mockStore) Del(k string) error {
	s.Lock()
	defer s.Unlock()
	_, ok := s.kv[k]
	if !ok {
		return errors.New("not found k")
	}
	delete(s.kv, k)
	return nil
}

func (s *mockStore) Get(k string) (string, error) {
	s.Lock()
	defer s.Unlock()

	v, ok := s.kv[k]
	if !ok {
		return "", errors.New("not found k")
	}
	return v, nil
}
