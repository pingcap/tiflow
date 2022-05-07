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
